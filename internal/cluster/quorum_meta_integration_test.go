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
