package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// readDoneMarkerForTest reads the mpudone marker for uploadID directly from the
// store. Returns nil when the marker is absent. M3 removed the production reader
// (the done-marker is no longer on the complete path); this keeps the sweep tests
// able to assert marker presence until M4 deletes the marker machinery entirely.
func readDoneMarkerForTest(t *testing.T, b *DistributedBackend, uploadID string) *multipartDone {
	t.Helper()
	var marker *multipartDone
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().MultipartDoneKey(uploadID))
		if err == ErrMetaKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		raw, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalMultipartDone(raw)
		if err != nil {
			return err
		}
		marker = &m
		return nil
	}))
	return marker
}

// seedDoneMarkerViaFSM writes a done-marker through the FSM directly. M3 dropped
// the CmdCompleteMultipart propose, so CompleteMultipartUpload no longer writes a
// done-marker; the marker/sweep machinery is exercised here by applying the command
// to the FSM directly (the marker keyspace + SweepStaleMultipartDoneMarkers survive
// until M4 deletes them). When metaBlob is non-empty the marker is blob-authoritative
// (the sweep's per-version blob-durability gate applies); otherwise it is a
// non-versioned/legacy marker swept on age alone.
func seedDoneMarkerViaFSM(t *testing.T, b *DistributedBackend, uploadID, bucket, key, versionID string, modTime int64, metaBlob []byte) {
	t.Helper()
	cmd, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket:    bucket,
		Key:       key,
		UploadID:  uploadID,
		VersionID: versionID,
		ModTime:   modTime,
		MetaBlob:  metaBlob,
	})
	require.NoError(t, err)
	require.NoError(t, b.fsm.Apply(cmd))
}

// TestSweepDoneMarkers_KeepsMarkerUntilBlobDurable proves the GC durability gate:
// a meta_blob-bearing done-marker whose per-version blob is NOT yet cluster-wide
// durable must NOT be swept — it is the only copy of the winning object metadata
// until the blob lands, so deleting it would lose the object. Once the blob is
// durable, the marker is sweep-eligible.
func TestSweepDoneMarkers_KeepsMarkerUntilBlobDurable(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// A real complete writes the per-version blob (the durability target).
	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("payload")), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	vid := obj.VersionID

	// Encode a meta_blob matching the completed per-version blob and seed a
	// blob-authoritative done-marker via the FSM (M3 complete no longer writes one).
	cmd, ok, err := b.readQuorumMetaVersion(bkt, key, vid)
	require.NoError(t, err)
	require.True(t, ok)
	metaBlob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	require.NoError(t, err)
	seedDoneMarkerViaFSM(t, b, up.UploadID, bkt, key, vid, obj.LastModified, metaBlob)

	// minAge negative so the just-written marker qualifies by age deterministically.
	const minAge = -time.Second

	// Blob missing → the marker MUST be kept (the only metadata copy left).
	require.NoError(t, b.shardSvc.deleteQuorumMetaVersionLocal(bkt, key, vid))
	n, err := b.SweepStaleMultipartDoneMarkers(ctx, 100, minAge)
	require.NoError(t, err)
	require.Equal(t, 0, n, "a meta_blob marker whose per-version blob is absent must not be swept")
	require.NotNil(t, readDoneMarkerForTest(t, b, up.UploadID), "marker must still exist")

	// Re-write the per-version blob; the marker then becomes sweep-eligible.
	require.NoError(t, b.fanOutPerVersionBlob(ctx, cmd, metaBlob))
	n, err = b.SweepStaleMultipartDoneMarkers(ctx, 100, minAge)
	require.NoError(t, err)
	require.Equal(t, 1, n, "once the per-version blob is durable the marker is sweep-eligible")
}

// forcedNonLeader wraps a RaftNode and forces IsLeader() to report false, so a
// test can exercise the follower path of a leader-gated method without standing
// up a multi-node cluster. All other RaftNode methods delegate to the embedded node.
type forcedNonLeader struct{ RaftNode }

func (forcedNonLeader) IsLeader() bool { return false }

// TestSweepDoneMarkers_LeaderGated proves the sweep is leader-only: a non-leader
// node returns early without scanning or deleting (every node holds the fully
// replicated mpudone: keyspace, so only the leader need scan — followers' work
// is pure redundancy). Single-node / leader sweeps normally.
func TestSweepDoneMarkers_LeaderGated(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "gatebkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("payload")), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	// Non-versioned/legacy marker (no meta_blob) seeded via the FSM.
	seedDoneMarkerViaFSM(t, b, up.UploadID, bkt, key, obj.VersionID, obj.LastModified, nil)

	// Non-leader: the gate returns early — no scan, no delete.
	orig := b.node
	b.node = forcedNonLeader{orig}
	n, err := b.SweepStaleMultipartDoneMarkers(ctx, 100, -time.Second)
	require.NoError(t, err)
	require.Equal(t, 0, n, "a non-leader must not sweep")
	require.NotNil(t, readDoneMarkerForTest(t, b, up.UploadID), "non-leader sweep must leave the marker intact")

	// Leader (single-node node restored): the stale marker is swept.
	b.node = orig
	n, err = b.SweepStaleMultipartDoneMarkers(ctx, 100, -time.Second)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the leader sweeps the stale marker")
}

// TestSweepDoneMarkers_NonVersionedSweptByAge proves the legacy path is unchanged:
// a non-versioned done-marker (no meta_blob) is swept purely on the age gate, with
// no per-version blob probe.
func TestSweepDoneMarkers_NonVersionedSweptByAge(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("payload")), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	seedDoneMarkerViaFSM(t, b, up.UploadID, bkt, key, obj.VersionID, obj.LastModified, nil)

	n, err := b.SweepStaleMultipartDoneMarkers(ctx, 100, -time.Second)
	require.NoError(t, err)
	require.Equal(t, 1, n, "non-versioned marker is swept by age (no blob probe)")
}
