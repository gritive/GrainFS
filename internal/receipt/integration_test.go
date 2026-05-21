package receipt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/stretchr/testify/require"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

func newStitchedCluster(t cleanupTestTB, peerNames ...string) *stitchedCluster {
	t.Helper()
	localDB := openTestDB(t)
	local, err := NewStore(localDB, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	DeferCleanup(local.Close)

	peers := make(map[string]*Store)
	for _, name := range peerNames {
		db := openTestDB(t)
		s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
		require.NoError(t, err)
		DeferCleanup(s.Close)
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
	r, found := s.LookupReceipt(id)
	if !found {
		return nil, false, nil
	}
	body, err := json.Marshal(r)
	return body, found, err
}

// Query satisfies PeerQuerier: fans out to all peers, first-hit wins.
func (c *stitchedCluster) Query(ctx context.Context, id string) ([]byte, bool, error) {
	var (
		mu     sync.Mutex
		winner []byte
		found  bool
		wg     sync.WaitGroup
	)
	for _, s := range c.peers {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			r, ok := s.LookupReceipt(id)
			if ok {
				body, err := json.Marshal(r)
				if err != nil {
					return
				}
				mu.Lock()
				if !found {
					found = true
					winner = body
				}
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()
	return winner, found, nil
}

// put signs and stores a receipt in the named location ("local" or a peer name).
func (c *stitchedCluster) put(t cleanupTestTB, location string, id string, ts time.Time) {
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
func (c *stitchedCluster) get(t cleanupTestTB, id string) *httptest.ResponseRecorder {
	t.Helper()
	api := NewAPI(c.local, c.cache, c, 0)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/receipts/"+id, nil)
	api.ServeGetReceipt(rec, req, id)
	return rec
}

var _ = Describe("Receipt integration", func() {
	var (
		t cleanupTestTB
		c *stitchedCluster
	)

	BeforeEach(func() {
		t = GinkgoT()
	})

	It("bypasses all remote paths for local hits", func() {
		c = newStitchedCluster(t, "peer-a", "peer-b")
		c.put(t, "local", "rcpt-local", time.Now())

		rec := c.get(t, "rcpt-local")
		Expect(rec.Code).To(Equal(http.StatusOK))

		body, err := io.ReadAll(rec.Body)
		Expect(err).NotTo(HaveOccurred())
		var decoded HealReceipt
		Expect(json.Unmarshal(body, &decoded)).To(Succeed())
		Expect(decoded.ReceiptID).To(Equal("rcpt-local"))
	})

	It("reaches the right peer on routing cache hits", func() {
		c = newStitchedCluster(t, "peer-a", "peer-b")
		c.put(t, "peer-a", "rcpt-peer-a", time.Now())
		c.gossipFromPeer("peer-a", []string{"rcpt-peer-a"})

		rec := c.get(t, "rcpt-peer-a")
		Expect(rec.Code).To(Equal(http.StatusOK))

		body, err := io.ReadAll(rec.Body)
		Expect(err).NotTo(HaveOccurred())
		var decoded HealReceipt
		Expect(json.Unmarshal(body, &decoded)).To(Succeed())
		Expect(decoded.ReceiptID).To(Equal("rcpt-peer-a"))
	})

	It("broadcasts on cache misses", func() {
		c = newStitchedCluster(t, "peer-a", "peer-b")
		c.put(t, "peer-b", "rcpt-old", time.Now().Add(-time.Hour))

		rec := c.get(t, "rcpt-old")
		Expect(rec.Code).To(Equal(http.StatusOK))

		body, err := io.ReadAll(rec.Body)
		Expect(err).NotTo(HaveOccurred())
		var decoded HealReceipt
		Expect(json.Unmarshal(body, &decoded)).To(Succeed())
		Expect(decoded.ReceiptID).To(Equal("rcpt-old"))
	})

	It("returns 404 when no node has the receipt", func() {
		c = newStitchedCluster(t, "peer-a")

		rec := c.get(t, "rcpt-nobody-has")
		Expect(rec.Code).To(Equal(http.StatusNotFound))
	})

	It("lists local receipts across a time range", func() {
		c = newStitchedCluster(t)

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

		Expect(rec.Code).To(Equal(http.StatusOK))
		var got []HealReceipt
		Expect(json.Unmarshal(rec.Body.Bytes(), &got)).To(Succeed())
		Expect(got).To(HaveLen(2), "list is local-scope only; remote receipts are excluded")
		Expect(got[0].ReceiptID).To(Equal("rcpt-0"))
		Expect(got[1].ReceiptID).To(Equal("rcpt-1"))
	})

	It("resolves local before cache before broadcast", func() {
		c = newStitchedCluster(t, "peer-a", "peer-b")

		base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
		localR := &HealReceipt{
			ReceiptID: "rcpt-dup",
			Timestamp: base,
			Object:    ObjectRef{Bucket: "local-bucket", Key: "k"},
		}
		ks, err := NewKeyStore(Key{ID: "psk-test", Secret: []byte("test-secret")})
		Expect(err).NotTo(HaveOccurred())
		Expect(Sign(localR, ks)).To(Succeed())
		Expect(c.local.Put(localR)).To(Succeed())
		Expect(c.local.Flush()).To(Succeed())

		peerR := &HealReceipt{
			ReceiptID: "rcpt-dup",
			Timestamp: base,
			Object:    ObjectRef{Bucket: "peer-bucket", Key: "k"},
		}
		Expect(Sign(peerR, ks)).To(Succeed())
		Expect(c.peers["peer-a"].Put(peerR)).To(Succeed())
		Expect(c.peers["peer-a"].Flush()).To(Succeed())
		c.gossipFromPeer("peer-a", []string{"rcpt-dup"})

		rec := c.get(t, "rcpt-dup")
		Expect(rec.Code).To(Equal(http.StatusOK))
		body, err := io.ReadAll(rec.Body)
		Expect(err).NotTo(HaveOccurred())
		var decoded HealReceipt
		Expect(json.Unmarshal(body, &decoded)).To(Succeed())
		Expect(decoded.Object.Bucket).To(Equal("local-bucket"), "local store must be tried before cache")
	})
})
