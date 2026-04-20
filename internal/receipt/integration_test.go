package receipt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stitchedCluster simulates the Phase 16 Slice 2 resolution chain
// in-process: two HealReceipt stores representing "this node" and "a
// peer", connected by a RoutingCache and a PeerQuerier that routes
// single-peer queries directly to the peer's store. The broadcast path
// fans out the same way. No QUIC, no real gossip, but the same code
// paths as a real multi-node deployment.
//
// This bridges the unit tests (which mock PeerQuerier) and a true
// multi-node E2E (which needs serve.go wiring + process spawning —
// tracked separately in TODOS.md).
type stitchedCluster struct {
	local *Store
	peers map[string]*Store // address → peer store
	cache *RoutingCache
}

func newStitchedCluster(t *testing.T, peerNames ...string) *stitchedCluster {
	t.Helper()
	localDB := openTestDB(t)
	local, err := NewStore(localDB, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	peers := make(map[string]*Store)
	for _, name := range peerNames {
		db := openTestDB(t)
		s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		peers[name] = s
	}

	return &stitchedCluster{
		local: local,
		peers: peers,
		cache: NewRoutingCache(),
	}
}

// gossipFromPeer simulates receiving a gossip tick: the RoutingCache
// learns that the peer holds these receipt IDs.
func (c *stitchedCluster) gossipFromPeer(peer string, ids []string) {
	c.cache.Update(peer, ids)
}

// QuerySingle satisfies PeerQuerier: looks up in the named peer's store.
func (c *stitchedCluster) QuerySingle(ctx context.Context, peer, id string) ([]byte, bool, error) {
	s, ok := c.peers[peer]
	if !ok {
		return nil, false, errors.New("no such peer")
	}
	raw, found := s.LookupReceiptJSON(id)
	return raw, found, nil
}

// Query satisfies PeerQuerier: fans out to all peers, first-hit wins.
func (c *stitchedCluster) Query(ctx context.Context, id string) ([]byte, bool, error) {
	var (
		mu      sync.Mutex
		winner  []byte
		found   bool
		wg      sync.WaitGroup
	)
	for _, s := range c.peers {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			if raw, ok := s.LookupReceiptJSON(id); ok {
				mu.Lock()
				if !found {
					found = true
					winner = raw
				}
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()
	return winner, found, nil
}

// put signs and stores a receipt in the named location ("local" or a peer name).
func (c *stitchedCluster) put(t *testing.T, location string, id string, ts time.Time) {
	t.Helper()
	r := mustSignedReceipt(t, id, ts)
	var s *Store
	if location == "local" {
		s = c.local
	} else {
		var ok bool
		s, ok = c.peers[location]
		require.True(t, ok, "unknown location %q", location)
	}
	require.NoError(t, s.Put(r))
	require.NoError(t, s.Flush())
}

// get exercises the API through the full resolution chain.
func (c *stitchedCluster) get(t *testing.T, id string) *httptest.ResponseRecorder {
	t.Helper()
	api := NewAPI(c.local, c.cache, c, 0)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/"+id, nil)
	api.ServeGetReceipt(rec, req, id)
	return rec
}

func TestIntegration_LocalHit_BypassesAllRemotePaths(t *testing.T) {
	c := newStitchedCluster(t, "peer-a", "peer-b")
	c.put(t, "local", "rcpt-local", time.Now())

	rec := c.get(t, "rcpt-local")
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ := io.ReadAll(rec.Body)
	var decoded HealReceipt
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "rcpt-local", decoded.ReceiptID)
}

func TestIntegration_RoutingCacheHit_ReachesRightPeer(t *testing.T) {
	c := newStitchedCluster(t, "peer-a", "peer-b")
	c.put(t, "peer-a", "rcpt-peer-a", time.Now())
	// Gossip informs the cache where this receipt lives.
	c.gossipFromPeer("peer-a", []string{"rcpt-peer-a"})

	rec := c.get(t, "rcpt-peer-a")
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ := io.ReadAll(rec.Body)
	var decoded HealReceipt
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "rcpt-peer-a", decoded.ReceiptID)
}

func TestIntegration_CacheMiss_BroadcastFindsReceipt(t *testing.T) {
	c := newStitchedCluster(t, "peer-a", "peer-b")
	// Peer-b has the receipt, but no gossip arrived (rolling window evicted it).
	c.put(t, "peer-b", "rcpt-old", time.Now().Add(-time.Hour))

	rec := c.get(t, "rcpt-old")
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ := io.ReadAll(rec.Body)
	var decoded HealReceipt
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "rcpt-old", decoded.ReceiptID)
}

func TestIntegration_NowhereFound_Returns404(t *testing.T) {
	c := newStitchedCluster(t, "peer-a")

	rec := c.get(t, "rcpt-nobody-has")
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestIntegration_ListAcrossTimeRange_LocalOnly(t *testing.T) {
	c := newStitchedCluster(t)

	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	c.put(t, "local", "rcpt-0", base)
	c.put(t, "local", "rcpt-1", base.Add(30*time.Second))
	c.put(t, "local", "rcpt-2", base.Add(2*time.Hour))

	api := NewAPI(c.local, c.cache, c, 0)
	rec := httptest.NewRecorder()
	url := "/api/receipts?from=" + base.Format(time.RFC3339Nano) +
		"&to=" + base.Add(time.Hour).Format(time.RFC3339Nano)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	api.ServeListReceipts(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got []HealReceipt
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 2, "list is local-scope only — remote receipts excluded")
	assert.Equal(t, "rcpt-0", got[0].ReceiptID)
	assert.Equal(t, "rcpt-1", got[1].ReceiptID)
}

func TestIntegration_ResolutionOrder_LocalBeatsCacheBeatsBroadcast(t *testing.T) {
	// Pathological case where the same id exists in multiple places — the
	// local copy must win, regardless of gossip/broadcast state.
	c := newStitchedCluster(t, "peer-a", "peer-b")

	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	// Same id, different bucket in each node to tell them apart.
	localR := &HealReceipt{
		ReceiptID: "rcpt-dup",
		Timestamp: base,
		Object:    ObjectRef{Bucket: "local-bucket", Key: "k"},
	}
	ks, err := NewKeyStore(Key{ID: "psk-test", Secret: []byte("test-secret")})
	require.NoError(t, err)
	require.NoError(t, Sign(localR, ks))
	require.NoError(t, c.local.Put(localR))
	require.NoError(t, c.local.Flush())

	peerR := &HealReceipt{
		ReceiptID: "rcpt-dup",
		Timestamp: base,
		Object:    ObjectRef{Bucket: "peer-bucket", Key: "k"},
	}
	require.NoError(t, Sign(peerR, ks))
	require.NoError(t, c.peers["peer-a"].Put(peerR))
	require.NoError(t, c.peers["peer-a"].Flush())
	c.gossipFromPeer("peer-a", []string{"rcpt-dup"})

	rec := c.get(t, "rcpt-dup")
	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	var decoded HealReceipt
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "local-bucket", decoded.Object.Bucket, "local store must be tried before cache")
}
