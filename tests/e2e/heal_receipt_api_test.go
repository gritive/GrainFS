package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go/logging"
	"github.com/dgraph-io/badger/v4"
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
// Slice 3). HTTP requests use AWS SigV4 because /api/receipts/:id sits behind
// the same S3-HMAC auth middleware as object endpoints.
//
// The test sets --heal-receipt-window=1 so only the most recent receipt on
// node C is gossiped. The older one must resolve via broadcast fallback —
// that is the path Slice 2 exists to provide.
func TestE2E_HealReceiptAPI_3Node(t *testing.T) {
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const clusterKey = "E2E-RECEIPT-CLUSTER-KSJGH45"
	var accessKey, secretKey string

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

	dataDirs := make([]string, 3)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-receipt-e2e-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}
	encKeyFile := makeSharedEncryptionKeyFile(t)

	// ReceiptIDs used across the test — literal ids make log output readable.
	const (
		idLocalA   = "rcpt-on-A-local"
		idCHot     = "rcpt-on-C-hot-gossiped"
		idCOld     = "rcpt-on-C-cold-outside-window"
		idMissing  = "rcpt-nowhere-found"
		bucketName = "audit-test"
		objectKey  = "some/key"
	)

	// Pre-seed: open each node's receipt BadgerDB BEFORE the process starts,
	// write signed receipts, close. The node will inherit these on open.
	// Seeding timestamps: oldest first so the newer one wins gossip's
	// "most recent 1" window.
	baseTime := time.Now().Add(-2 * time.Hour).UTC()
	seedReceipt(t, dataDirs[0], clusterKey, idLocalA, baseTime, bucketName, objectKey)
	seedReceipt(t, dataDirs[2], clusterKey, idCOld, baseTime.Add(10*time.Minute), bucketName, objectKey)
	seedReceipt(t, dataDirs[2], clusterKey, idCHot, baseTime.Add(1*time.Hour), bucketName, objectKey)

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", raftAddr(i),
			"--raft-addr", raftAddr(i),
			"--cluster-key", clusterKey,
			"--encryption-key-file", encKeyFile,
			"--heal-receipt-window=1",
			"--heal-receipt-gossip-interval=1s",
			"--nfs4-port", "0",
			"--nbd-port", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Start(), "start node %d", i)
		return cmd
	}

	// Spawn 3 nodes: seed first, then followers via .join-pending.
	procs := make([]*exec.Cmd, 3)
	procs[0] = startNode(0)
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})

	waitForPort(t, httpPorts[0], 15*time.Second)
	time.Sleep(2 * time.Second)

	accessKey, secretKey = bootstrapAdminViaUDSAny(t, dataDirs[:1], 60*time.Second)

	for i := 1; i < 3; i++ {
		require.NoError(t, writeNodeJoinPending(dataDirs[i], raftAddr(0)))
		procs[i] = startNode(i)
		time.Sleep(150 * time.Millisecond)
	}
	for i := range procs {
		waitForPort(t, httpPorts[i], 15*time.Second)
	}
	// Give the gossip loop (1s interval) a few ticks so node B learns which
	// peer holds which receipt id.
	time.Sleep(4 * time.Second)

	// Wait until each node's local IAM verifier accepts the bootstrap key.
	// The propose returns once the leader applied; followers apply via raft
	// replication, which is fast but not synchronous with the response.
	for i := 0; i < 3; i++ {
		iamWaitKeyReady(t, httpURL(i), accessKey, secretKey, 15*time.Second)
	}

	// DisableURIPathEscaping matches what the S3 client does — the server's
	// verifier builds its canonical URI from r.URL.Path unchanged, so any
	// extra percent-encoding at sign time would produce a signature mismatch.
	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
		if testing.Verbose() {
			o.Logger = logging.NewStandardLogger(os.Stderr)
			o.LogSigning = true
		}
	})
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	ctx := context.Background()

	t.Run("LocalHit_QueryANodeForItsOwnReceipt", func(t *testing.T) {
		body, status := signedGet(t, ctx, signer, creds, httpURL(0)+"/api/receipts/"+idLocalA)
		require.Equal(t, http.StatusOK, status, "node A should answer locally for its own receipt; body=%s", body)
		require.Contains(t, string(body), idLocalA)
	})

	t.Run("RoutingCacheHit_QueryBForReceiptOnCThatWasGossiped", func(t *testing.T) {
		body, status := signedGet(t, ctx, signer, creds, httpURL(1)+"/api/receipts/"+idCHot)
		require.Equal(t, http.StatusOK, status, "node B should route via gossip cache to C; body=%s", body)
		require.Contains(t, string(body), idCHot)
	})

	t.Run("BroadcastFallback_QueryBForReceiptOnCOutsideGossipWindow", func(t *testing.T) {
		body, status := signedGet(t, ctx, signer, creds, httpURL(1)+"/api/receipts/"+idCOld)
		require.Equal(t, http.StatusOK, status, "node B should find receipt via broadcast fan-out; body=%s", body)
		require.Contains(t, string(body), idCOld)
	})

	t.Run("NotFound_UnknownReceiptReturns404", func(t *testing.T) {
		body, status := signedGet(t, ctx, signer, creds, httpURL(1)+"/api/receipts/"+idMissing)
		require.Equal(t, http.StatusNotFound, status, "missing receipt must return 404, got body=%s", body)
	})

	t.Run("UnauthenticatedRequestIsRejected", func(t *testing.T) {
		resp, err := http.Get(httpURL(0) + "/api/receipts/" + idLocalA)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusForbidden, resp.StatusCode, "unsigned request must be rejected")
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

// signedGet issues a SigV4-signed GET against the heal-receipt API and
// returns (body, status). The server's verifier derives the payload hash
// from the X-Amz-Content-Sha256 header (fallback "UNSIGNED-PAYLOAD"), so we
// set it explicitly before signing to match what the signer hashed into the
// canonical request. Without this the server and client compute different
// canonical requests and the signature never verifies.
func signedGet(t *testing.T, ctx context.Context, signer *v4.Signer, creds aws.Credentials, url string) ([]byte, int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	sum := sha256.Sum256(nil)
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", "us-east-1", time.Now())
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body, resp.StatusCode
}
