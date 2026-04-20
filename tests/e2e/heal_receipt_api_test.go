package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/receipt"
)

// TestE2E_HealReceiptAPI_3Node verifies the Phase 16 Slice 2 resolution
// chain end-to-end against a real 3-node cluster.
//
// Covers all four resolution paths the API exposes:
//  1. Local hit       — receipt stored on the queried node.
//  2. Routing-cache   — peer gossips its recent ids, query routed directly.
//  3. Broadcast hit   — id outside the rolling window, fan-out finds it.
//  4. Not found       — id does not exist on any node.
//
// Receipts are pre-seeded into each node's BadgerDB before the node starts
// (Slice 2 does not emit receipts from the scrubber yet; that wiring lands in
// Slice 3). The cluster runs without S3 credentials because runCluster in
// serve.go does not propagate --access-key/--secret-key today (a pre-existing
// cluster-mode gap tracked separately); receipt auth is verified at unit
// level in internal/receipt/api_test.go.
//
// The test sets --heal-receipt-window=1 so only the most recent receipt on
// node C is gossiped. The older one must resolve via broadcast fallback —
// that is the path Slice 2 exists to provide.
func TestE2E_HealReceiptAPI_3Node(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const clusterKey = "E2E-RECEIPT-CLUSTER-KSJGH45"

	// Allocate a single batch of ports up front. freePort returns a port the
	// OS just closed — reusing the name `sock` avoids two nodes racing for
	// the same port between allocation and Listen.
	httpPorts := [3]int{freePort(), freePort(), freePort()}
	raftPorts := [3]int{freePort(), freePort(), freePort()}

	raftAddr := func(i int) string {
		return fmt.Sprintf("127.0.0.1:%d", raftPorts[i])
	}
	httpURL := func(i int) string {
		return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i])
	}
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j == i {
				continue
			}
			out = append(out, raftAddr(j))
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, 3)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-receipt-e2e-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
		// Pre-create raft/ so serve.go's auto-migrate branch does not
		// double-open the meta BadgerDB on fresh cluster start. The
		// auto-migration is intended for solo→cluster upgrades and is
		// falsely triggered on empty dataDirs because serve.go MkdirAll's
		// meta/ unconditionally before the existence check.
		require.NoError(t, os.MkdirAll(filepath.Join(d, "raft"), 0o755))
	}

	// ReceiptIDs used across the test — literal ids make log output readable.
	const (
		idLocalA    = "rcpt-on-A-local"
		idCHot      = "rcpt-on-C-hot-gossiped"
		idCOld      = "rcpt-on-C-cold-outside-window"
		idMissing   = "rcpt-nowhere-found"
		bucketName  = "audit-test"
		objectKey   = "some/key"
	)

	// Pre-seed: open each node's receipt BadgerDB BEFORE the process starts,
	// write signed receipts, close. The node will inherit these on open.
	// Seeding timestamps: oldest first so the newer one wins gossip's
	// "most recent 1" window.
	baseTime := time.Now().Add(-2 * time.Hour).UTC()
	seedReceipt(t, dataDirs[0], clusterKey, idLocalA, baseTime, bucketName, objectKey)
	seedReceipt(t, dataDirs[2], clusterKey, idCOld, baseTime.Add(10*time.Minute), bucketName, objectKey)
	seedReceipt(t, dataDirs[2], clusterKey, idCHot, baseTime.Add(1*time.Hour), bucketName, objectKey)

	// Spawn 3 nodes sharing cluster-key and S3 credentials.
	procs := make([]*exec.Cmd, 3)
	for i := 0; i < 3; i++ {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("e2e-node-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--heal-receipt-window=1",
			"--heal-receipt-gossip-interval=1s",
			"--nfs-port", "0",
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
			"--ec=false",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Start(), "start node %d", i)
		procs[i] = cmd
	}
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})

	for i := range procs {
		waitForPort(httpPorts[i], 15*time.Second)
	}
	// Give the gossip loop (1s interval) a few ticks so node B learns which
	// peer holds which receipt id.
	time.Sleep(4 * time.Second)

	t.Run("LocalHit_QueryANodeForItsOwnReceipt", func(t *testing.T) {
		body, status := httpGet(t, httpURL(0)+"/api/receipts/"+idLocalA)
		require.Equal(t, http.StatusOK, status, "node A should answer locally for its own receipt; body=%s", body)
		assert.Contains(t, string(body), idLocalA)
	})

	t.Run("RoutingCacheHit_QueryBForReceiptOnCThatWasGossiped", func(t *testing.T) {
		body, status := httpGet(t, httpURL(1)+"/api/receipts/"+idCHot)
		require.Equal(t, http.StatusOK, status, "node B should route via gossip cache to C; body=%s", body)
		assert.Contains(t, string(body), idCHot)
	})

	t.Run("BroadcastFallback_QueryBForReceiptOnCOutsideGossipWindow", func(t *testing.T) {
		body, status := httpGet(t, httpURL(1)+"/api/receipts/"+idCOld)
		require.Equal(t, http.StatusOK, status, "node B should find receipt via broadcast fan-out; body=%s", body)
		assert.Contains(t, string(body), idCOld)
	})

	t.Run("NotFound_UnknownReceiptReturns404", func(t *testing.T) {
		body, status := httpGet(t, httpURL(1)+"/api/receipts/"+idMissing)
		require.Equal(t, http.StatusNotFound, status, "missing receipt must return 404, got body=%s", body)
	})
}

// seedReceipt opens the target receipts/ BadgerDB on disk, writes one
// HMAC-signed HealReceipt, and closes cleanly. Must be invoked before the
// grainfs process starts on that dataDir (BadgerDB takes an exclusive lock).
func seedReceipt(t *testing.T, dataDir, psk, id string, ts time.Time, bucket, key string) {
	t.Helper()
	dir := filepath.Join(dataDir, "receipts")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 1,
		FlushInterval:  time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	ks, err := receipt.NewKeyStore(receipt.Key{ID: "cluster", Secret: []byte(psk)})
	require.NoError(t, err)

	r := &receipt.HealReceipt{
		ReceiptID: id,
		Timestamp: ts,
		Object:    receipt.ObjectRef{Bucket: bucket, Key: key},
	}
	require.NoError(t, receipt.Sign(r, ks))
	require.NoError(t, store.Put(r))
	require.NoError(t, store.Flush())
}

// httpGet performs a plain GET (no auth) and returns (body, status). Safe
// because this E2E spins up the cluster without S3 credentials — receipt
// HMAC auth is exercised at the unit-test level in internal/receipt.
func httpGet(t *testing.T, url string) ([]byte, int) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body, resp.StatusCode
}
