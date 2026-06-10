package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

var _ = Describe("Quorum meta — Phase 3 primary path", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	It("PUT writes metadata to quorum store", func() {
		payload := bytes.Repeat([]byte("x"), 1<<10)
		put, err := b.PutObject(ctx, "bucket", "obj.bin",
			bytes.NewReader(payload), "application/octet-stream")
		Expect(err).To(Succeed())
		Expect(put.ETag).NotTo(BeEmpty())

		qmetaPath := filepath.Join(b.root, "shards", quorumMetaSubDir, "bucket", "obj.bin")
		_, statErr := os.Stat(qmetaPath)
		Expect(statErr).To(Succeed(), "quorum meta file must exist: %s", qmetaPath)
	})

	It("PUT does NOT write object meta to BadgerDB (data_raft bypassed)", func() {
		payload := bytes.Repeat([]byte("y"), 512)
		_, err := b.PutObject(ctx, "bucket", "bypassed.bin",
			bytes.NewReader(payload), "application/octet-stream")
		Expect(err).To(Succeed())

		// Object meta key must NOT exist in BadgerDB.
		var found bool
		_ = b.db.View(func(txn *badger.Txn) error {
			k := b.ks().ObjectMetaKey("bucket", "bypassed.bin")
			_, dbErr := txn.Get(k)
			found = dbErr == nil
			return nil
		})
		Expect(found).To(BeFalse(), "data_raft (BadgerDB) must not receive object meta in Phase 3")
	})

	It("GET reads metadata from quorum store", func() {
		payload := bytes.Repeat([]byte("z"), 2<<10)
		put, err := b.PutObject(ctx, "bucket", "read.bin",
			bytes.NewReader(payload), "text/plain")
		Expect(err).To(Succeed())

		obj, err := b.HeadObject(ctx, "bucket", "read.bin")
		Expect(err).To(Succeed())
		Expect(obj.ETag).To(Equal(put.ETag))
		Expect(obj.Size).To(Equal(int64(len(payload))))
		Expect(obj.ContentType).To(Equal("text/plain"))
	})

	It("GET body is readable after quorum-meta PUT", func() {
		payload := bytes.Repeat([]byte("body"), 256)
		_, err := b.PutObject(ctx, "bucket", "getbody.bin",
			bytes.NewReader(payload), "application/octet-stream")
		Expect(err).To(Succeed())

		obj, err := b.HeadObject(ctx, "bucket", "getbody.bin")
		Expect(err).To(Succeed())

		buf := make([]byte, obj.Size)
		n, rerr := b.ReadAt(ctx, "bucket", "getbody.bin", 0, buf)
		Expect(rerr).To(Or(Succeed(), MatchError(io.EOF)))
		Expect(buf[:n]).To(Equal(payload))
	})
})

// TestDeleteObject_QuorumMetaTombstone proves S4-1: DELETE writes an IsDeleteMarker=true
// tombstone to quorum meta instead of removing the file.
//
// RED without the tombstone write in deleteObjectWithMarker: readQuorumMetaRawCmd
// returns storage.ErrObjectNotFound after DELETE (file was removed instead of marked).
// GREEN with the tombstone write: readQuorumMetaRawCmd returns a cmd with IsDeleteMarker=true.
//
// Neuter test: if the writeQuorumMeta call is removed from deleteObjectWithMarker
// (reverting to deleteQuorumMetaLocal), this test is RED.
func TestDeleteObject_QuorumMetaTombstone(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	// PUT — writes quorum meta locally.
	payload := bytes.Repeat([]byte("d"), 512)
	_, err := b.PutObject(ctx, "bucket", "del.bin",
		bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)

	// DELETE — must write tombstone to quorum meta.
	err = b.DeleteObject(ctx, "bucket", "del.bin")
	require.NoError(t, err)

	// The quorum meta file must still exist as a tombstone.
	cmd, err := b.shardSvc.readQuorumMetaRawCmd("bucket", "del.bin")
	require.NoError(t, err, "quorum meta tombstone must be present after DELETE")
	require.True(t, cmd.IsDeleteMarker, "quorum meta after DELETE must have IsDeleteMarker=true")
}

// TestScatterGatherList_LWWAndTombstone proves S4-3:
// scatterGatherList fans out ScanQuorumMeta RPCs to all shard group peers,
// applies per-key LWW (max ModTime wins), and filters IsDeleteMarker tombstones.
//
// RED without scatterGatherList (or ScanQuorumMeta RPC): compile error.
// GREEN: stale alpha.bin on node B loses to fresh alpha.bin on node A (LWW);
// beta.bin tombstone is excluded; gamma.bin from node B is included.
//
// Neuter test: removing LWW in scatterGatherList or removing tombstone filtering
// causes key assertions to fail.
func TestScatterGatherList_LWWAndTombstone(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trA := transport.MustNewTCPTransport("test-cluster-psk")
	trB := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, trA.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trB.Listen(ctx, "127.0.0.1:0"))
	defer trA.Close()
	defer trB.Close()
	require.NoError(t, trA.Connect(ctx, trB.LocalAddr()))
	require.NoError(t, trB.Connect(ctx, trA.LocalAddr()))

	dirA := t.TempDir()
	dirB := t.TempDir()
	svcA := NewShardService(dirA, trA, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcB := NewShardService(dirB, trB, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trA.SetStreamHandler(svcA.HandleRPC())
	trB.SetStreamHandler(svcB.HandleRPC())

	encodeBlob := func(cmd PutObjectMetaCmd) []byte {
		t.Helper()
		blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
		require.NoError(t, err)
		return blob
	}
	baseCmd := func(key string, modTime int64, isDel bool) PutObjectMetaCmd {
		return PutObjectMetaCmd{Bucket: "bkt", Key: key, ETag: "e", ECData: 1,
			NodeIDs: []string{"n1"}, ModTime: modTime, IsDeleteMarker: isDel}
	}

	// Node A: alpha.bin (ModTime=10, fresh) + beta.bin (tombstone, ModTime=5).
	require.NoError(t, svcA.writeQuorumMetaLocal("bkt", "alpha.bin", encodeBlob(baseCmd("alpha.bin", 10, false))))
	require.NoError(t, svcA.writeQuorumMetaLocal("bkt", "beta.bin", encodeBlob(baseCmd("beta.bin", 5, true))))
	// Node B: alpha.bin (ModTime=5, stale) + gamma.bin (ModTime=10, normal).
	require.NoError(t, svcB.writeQuorumMetaLocal("bkt", "alpha.bin", encodeBlob(baseCmd("alpha.bin", 5, false))))
	require.NoError(t, svcB.writeQuorumMetaLocal("bkt", "gamma.bin", encodeBlob(baseCmd("gamma.bin", 10, false))))

	backendA := &DistributedBackend{
		selfAddr: trA.LocalAddr(),
		shardSvc: svcA,
		shardGroup: &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{trA.LocalAddr(), trB.LocalAddr()}},
		}},
	}

	entries, err := backendA.scatterGatherList(ctx, "bkt", "")
	require.NoError(t, err)

	// beta.bin is a tombstone → filtered.
	// alpha.bin: node A (ModTime=10) beats node B (ModTime=5).
	// gamma.bin: only on node B.
	require.Len(t, entries, 2, "tombstone filtered; alpha.bin + gamma.bin survive")
	keySet := map[string]int64{}
	for _, e := range entries {
		keySet[e.Key] = e.ModTime
	}
	require.Contains(t, keySet, "alpha.bin")
	require.Contains(t, keySet, "gamma.bin")
	require.NotContains(t, keySet, "beta.bin", "tombstone must not appear")
	require.Equal(t, int64(10), keySet["alpha.bin"], "LWW: fresh entry (ModTime=10) must win")
}

// TestScanQuorumMetaBucket proves S4-2: ScanQuorumMetaBucket returns all entries
// (including tombstones) for a bucket, with optional prefix filtering.
//
// RED without ScanQuorumMetaBucket: compile error (function not found).
// GREEN: PUT 2 objects + DELETE 1 → scan returns all 3 entries; tombstone has IsDeleteMarker=true;
// prefix filter reduces results to the matching subset.
//
// Neuter test: if ScanQuorumMetaBucket omits tombstones, the tombstone assertion fails.
func TestScanQuorumMetaBucket(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	payload := bytes.Repeat([]byte("x"), 128)
	_, err := b.PutObject(ctx, "bkt", "keep.bin", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	_, err = b.PutObject(ctx, "bkt", "del.bin", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	require.NoError(t, b.DeleteObject(ctx, "bkt", "del.bin"))

	entries, err := b.shardSvc.ScanQuorumMetaBucket("bkt", "")
	require.NoError(t, err)
	// DELETE overwrites the existing quorum meta with a tombstone (same path).
	// So 2 PUTs → 2 files; 1 DELETE → 1 file replaced with tombstone → still 2 files.
	require.Len(t, entries, 2, "scan must return 2 entries: 1 normal PUT + 1 tombstone")

	// Tombstone must have IsDeleteMarker=true.
	var sawTombstone bool
	for _, e := range entries {
		if e.Key == "del.bin" {
			sawTombstone = e.IsDeleteMarker
		}
	}
	require.True(t, sawTombstone, "del.bin entry must be IsDeleteMarker=true")

	// Prefix filter.
	keep, err := b.shardSvc.ScanQuorumMetaBucket("bkt", "keep")
	require.NoError(t, err)
	require.Len(t, keep, 1)
	require.Equal(t, "keep.bin", keep[0].Key)
}

// TestMultipartComplete_BadgerDBFallback proves the Phase 3 raft/quorum boundary:
// multipart-completed objects are committed via data_raft (applyCompleteMultipart),
// so their quorum meta file is absent. headObjectMeta must still serve them by falling
// back to BadgerDB.
//
// RED without the BadgerDB fallback in headObjectMeta: HeadObject returns ErrObjectNotFound.
// GREEN with fallback: HeadObject returns the object committed by raft.
//
// Neuter test: if the BadgerDB fallback block is removed from headObjectMeta, this test
// is RED (storage.ErrObjectNotFound) for multipart-completed objects.
func TestMultipartComplete_BadgerDBFallback(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	// Create and complete a multipart upload.
	mpu, err := b.CreateMultipartUpload(ctx, "bucket", "multi.bin", "application/octet-stream")
	require.NoError(t, err)

	payload := bytes.Repeat([]byte("mp"), 512)
	part, err := b.UploadPart(ctx, "bucket", "multi.bin", mpu.UploadID, 1, bytes.NewReader(payload))
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload(ctx, "bucket", "multi.bin", mpu.UploadID, []storage.Part{{
		PartNumber: part.PartNumber,
		ETag:       part.ETag,
		Size:       part.Size,
	}})
	require.NoError(t, err)
	require.NotEmpty(t, obj.ETag)

	// Quorum meta must NOT exist: multipart complete is raft-only (Phase 3 boundary).
	qmetaPath := filepath.Join(b.root, "shards", quorumMetaSubDir, "bucket", "multi.bin")
	_, statErr := os.Stat(qmetaPath)
	require.True(t, os.IsNotExist(statErr), "quorum meta file must not exist for multipart-completed object: %s", qmetaPath)

	// HeadObject must succeed via BadgerDB fallback (quorum meta is absent).
	head, err := b.HeadObject(ctx, "bucket", "multi.bin")
	require.NoError(t, err, "headObjectMeta must fall back to BadgerDB for multipart-completed objects")
	require.Equal(t, obj.ETag, head.ETag)
	require.Equal(t, int64(len(payload)), head.Size)
}

// TestReadQuorumMeta_PeerFallback_ParityNodeMiss proves the N-K node hazard fix:
// when a parity node did not receive the K-of-N quorum meta write, it must
// recover the metadata by fanning out ReadQuorumMeta RPCs to other placement
// nodes.
//
// RED without fetchQuorumMetaFromPeers: readQuorumMeta returns ErrObjectNotFound.
// GREEN with fan-out: readQuorumMeta returns the correct metadata.
//
// Neuter test: if the fetchQuorumMetaFromPeers fallback is removed from
// readQuorumMeta, this test is RED (storage.ErrObjectNotFound).
func TestReadQuorumMeta_PeerFallback_ParityNodeMiss(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	// Two real TCP transport nodes: node-data (has the quorum meta file) and
	// node-parity (missed the K-of-N write — file absent locally).
	trData := transport.MustNewTCPTransport("test-cluster-psk")
	trParity := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, trData.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trParity.Listen(ctx, "127.0.0.1:0"))
	defer trData.Close()
	defer trParity.Close()
	// trParity dials trData so it can send ReadQuorumMeta RPCs.
	require.NoError(t, trParity.Connect(ctx, trData.LocalAddr()))

	dirData := t.TempDir()
	dirParity := t.TempDir()
	svcData := NewShardService(dirData, trData,
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcParity := NewShardService(dirParity, trParity,
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	// trData serves incoming shard RPCs (including ReadQuorumMeta).
	trData.SetStreamHandler(svcData.HandleRPC())

	// Write quorum meta ONLY to the data node, simulating K-of-N write where
	// the parity node was not in the write quorum.
	blob := func() []byte {
		cmd := PutObjectMetaCmd{
			Bucket:      "bkt",
			Key:         "obj",
			ECData:      4,
			ECParity:    2,
			Size:        1024,
			ContentType: "application/octet-stream",
			ETag:        "abc123",
			NodeIDs:     []string{trData.LocalAddr(), trParity.LocalAddr()},
		}
		b, err := EncodeCommand(CmdPutObjectMeta, cmd)
		require.NoError(t, err)
		return b
	}()
	require.NoError(t, svcData.writeQuorumMetaLocal("bkt", "obj", blob))

	// The parity node has no local file for this object.
	_, err := os.Stat(filepath.Join(dirParity, "shards", quorumMetaSubDir, "bkt", "obj"))
	require.True(t, os.IsNotExist(err), "parity node must not have the quorum meta file")

	// Set up a DistributedBackend representing the parity node.
	// shardGroup advertises trData as a peer so fetchQuorumMetaFromPeers dials it.
	backendParity := &DistributedBackend{
		selfAddr: trParity.LocalAddr(),
		shardSvc: svcParity,
		shardGroup: &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{trData.LocalAddr(), trParity.LocalAddr()}},
		}},
	}

	// readQuorumMeta on the parity node — local miss → peer fan-out → data node hit.
	obj, pm, err := backendParity.readQuorumMeta("bkt", "obj")
	require.NoError(t, err, "peer fan-out must return the quorum meta from the data node")
	require.NotNil(t, obj)
	require.Equal(t, "obj", obj.Key)
	require.Equal(t, int64(1024), obj.Size)
	require.Equal(t, "abc123", obj.ETag)
	require.Equal(t, 2, len(pm.NodeIDs), "placement must include both nodes")

	// Sanity: a truly absent object must still return ErrObjectNotFound.
	_, _, err = backendParity.readQuorumMeta("bkt", "nonexistent")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}
