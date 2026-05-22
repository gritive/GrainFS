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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/receipt"
)

// Heal receipt API verifies the Phase 16 Slice 2 resolution
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
// Slice 3). Primary HTTP requests use AWS SigV4; the final case documents
// the current Phase 0 anonymous-read behavior while iam.anon-enabled remains
// true.
//
// The test sets --heal-receipt-window=1 so only the most recent receipt on
// node C is gossiped. The older one must resolve via broadcast fallback —
// that is the path Slice 2 exists to provide.
var _ = ginkgo.Describe("Heal receipt API", ginkgo.Ordered, func() {
	var (
		ctx       context.Context
		signer    *v4.Signer
		creds     aws.Credentials
		httpURL   func(int) string
		sockA     string
		idLocalA  string
		idCHot    string
		idCOld    string
		idMissing string
	)

	ginkgo.BeforeAll(func() {
		t := ginkgo.GinkgoTB()
		binary := getBinary()
		if _, err := os.Stat(binary); err != nil {
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
		httpURL = func(i int) string {
			return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i])
		}

		dataDirs := make([]string, 3)
		for i := range dataDirs {
			d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-receipt-e2e-%d-*", i))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			dataDirs[i] = d
			ginkgo.DeferCleanup(os.RemoveAll, d)
		}
		sockA = filepath.Join(dataDirs[0], "admin.sock")
		encKeyFile := makeSharedEncryptionKeyFile(t)

		// ReceiptIDs used across the test — literal ids make log output readable.
		const (
			bucketName = "audit-test"
			objectKey  = "some/key"
		)
		idLocalA = "rcpt-on-A-local"
		idCHot = "rcpt-on-C-hot-gossiped"
		idCOld = "rcpt-on-C-cold-outside-window"
		idMissing = "rcpt-nowhere-found"

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
			gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start node %d", i)
			return cmd
		}

		// Spawn 3 nodes: seed first, then followers via .join-pending.
		procs := make([]*exec.Cmd, 3)
		procs[0] = startNode(0)
		ginkgo.DeferCleanup(func() {
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
			gomega.Expect(writeNodeJoinPending(dataDirs[i], dataDirs[0], raftAddr(0))).To(gomega.Succeed())
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
		signer = v4.NewSigner(func(o *v4.SignerOptions) {
			o.DisableURIPathEscaping = true
			if testing.Verbose() {
				o.Logger = logging.NewStandardLogger(os.Stderr)
				o.LogSigning = true
			}
		})
		creds = aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
		ctx = context.Background()
	})

	ginkgo.It("answers locally for a node's own receipt", func() {
		t := ginkgo.GinkgoTB()
		body, status := signedGet(t, ctx, signer, creds, httpURL(0)+"/api/receipts/"+idLocalA)
		gomega.Expect(status).To(gomega.Equal(http.StatusOK), "node A should answer locally for its own receipt; body=%s", body)
		gomega.Expect(string(body)).To(gomega.ContainSubstring(idLocalA))
	})

	ginkgo.It("routes via gossip cache to a peer", func() {
		t := ginkgo.GinkgoTB()
		body, status := signedGet(t, ctx, signer, creds, httpURL(1)+"/api/receipts/"+idCHot)
		gomega.Expect(status).To(gomega.Equal(http.StatusOK), "node B should route via gossip cache to C; body=%s", body)
		gomega.Expect(string(body)).To(gomega.ContainSubstring(idCHot))
	})

	ginkgo.It("falls back to broadcast for receipts outside the gossip window", func() {
		t := ginkgo.GinkgoTB()
		body, status := signedGet(t, ctx, signer, creds, httpURL(1)+"/api/receipts/"+idCOld)
		gomega.Expect(status).To(gomega.Equal(http.StatusOK), "node B should find receipt via broadcast fan-out; body=%s", body)
		gomega.Expect(string(body)).To(gomega.ContainSubstring(idCOld))
	})

	ginkgo.It("returns 404 for missing receipts", func() {
		t := ginkgo.GinkgoTB()
		body, status := signedGet(t, ctx, signer, creds, httpURL(1)+"/api/receipts/"+idMissing)
		gomega.Expect(status).To(gomega.Equal(http.StatusNotFound), "missing receipt must return 404, got body=%s", body)
	})

	ginkgo.It("allows unauthenticated reads while Phase 0 anonymous mode is enabled", func() {
		// D#3 + F#16: the bootstrap SA create above auto-flipped iam.anon-enabled
		// to false. Re-enabling anon is an operator-supported path (meta_fsm.go
		// "Subsequent SA creates leave the flag untouched"), so verify the
		// heal-receipt unsigned read still works once anon is back on.
		gomega.Expect(setConfigViaUDS(sockA, "iam.anon-enabled", "true")).To(gomega.Succeed())

		resp, err := http.Get(httpURL(0) + "/api/receipts/" + idLocalA)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK), "unsigned request should follow Phase 0 anonymous mode; body=%s", body)
		gomega.Expect(string(body)).To(gomega.ContainSubstring(idLocalA))
	})
})

// seedReceipt opens the target receipts/ BadgerDB on disk, writes one
// HMAC-signed HealReceipt, and closes cleanly. Must be invoked before the
// grainfs process starts on that dataDir (BadgerDB takes an exclusive lock).
func seedReceipt(t testing.TB, dataDir, psk, id string, ts time.Time, bucket, key string) {
	t.Helper()
	dir := filepath.Join(dataDir, "receipts")
	gomega.Expect(os.MkdirAll(dir, 0o755)).To(gomega.Succeed())

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() { _ = db.Close() }()

	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 1,
		FlushInterval:  time.Second,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() { _ = store.Close() }()

	ks, err := receipt.NewKeyStore(receipt.Key{ID: "cluster", Secret: []byte(psk)})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	r := &receipt.HealReceipt{
		ReceiptID: id,
		Timestamp: ts,
		Object:    receipt.ObjectRef{Bucket: bucket, Key: key},
	}
	gomega.Expect(receipt.Sign(r, ks)).To(gomega.Succeed())
	gomega.Expect(store.Put(r)).To(gomega.Succeed())
	gomega.Expect(store.Flush()).To(gomega.Succeed())
}

// signedGet issues a SigV4-signed GET against the heal-receipt API and
// returns (body, status). The server's verifier derives the payload hash
// from the X-Amz-Content-Sha256 header (fallback "UNSIGNED-PAYLOAD"), so we
// set it explicitly before signing to match what the signer hashed into the
// canonical request. Without this the server and client compute different
// canonical requests and the signature never verifies.
func signedGet(t testing.TB, ctx context.Context, signer *v4.Signer, creds aws.Credentials, url string) ([]byte, int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sum := sha256.Sum256(nil)
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = signer.SignHTTP(ctx, creds, req, payloadHash, "s3", "us-east-1", time.Now())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return body, resp.StatusCode
}
