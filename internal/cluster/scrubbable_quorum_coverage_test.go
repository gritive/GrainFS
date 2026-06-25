package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// collectScanObjectKeys drains ScanObjects into a key→count map.
func collectScanObjectKeys(t *testing.T, b *DistributedBackend, bucket string) map[string]int {
	t.Helper()
	ch, err := b.ScanObjects(bucket)
	require.NoError(t, err)
	got := map[string]int{}
	for rec := range ch {
		got[rec.Key]++
	}
	return got
}

// TestScanObjects_IncludesQuorumMetaOnlyObjects proves a regular-PUT EC object
// (which lives only in the off-raft quorum-meta blob) is yielded by ScanObjects —
// the EC scrubber's work-list source — so the scrubber can repair it.
func TestScanObjects_IncludesQuorumMetaOnlyObjects(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.True(t, b.ECActive(), "test backend must be EC-active (1+0 single-node)")
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// Regular-PUT object: quorum-meta blob (the sole object-metadata store).
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{
		Bucket: "bkt", Key: "quorum-only.bin", VersionID: "v-q",
		Size: 1, ETag: "etag-q", ModTime: 1,
		ECData: 1, ECParity: 0, NodeIDs: []string{b.selfAddr},
	}))
	// Confirm the seed is visible to the quorum-meta scan (guards the test itself).
	cmds, err := b.shardSvc.ScanQuorumMetaBucket("bkt", "")
	require.NoError(t, err)
	var sawSeed bool
	for _, c := range cmds {
		if c.Key == "quorum-only.bin" {
			sawSeed = true
		}
	}
	require.True(t, sawSeed, "writeQuorumMeta seed must be visible to ScanQuorumMetaBucket")

	got := collectScanObjectKeys(t, b, "bkt")
	require.Equal(t, 1, got["quorum-only.bin"], "quorum-meta object must appear")
}

// TestScanObjects_SkipsQuorumMetaTombstonesAndNonEC proves the quorum-meta merge
// excludes (a) delete-marker tombstones and (b) non-EC entries (ECData==0) — both
// would be invalid scrub work.
//
// Mutation: remove the IsDeleteMarker/deleteMarkerETag skip → tomb.bin appears → RED.
// Remove the ECData==0 skip → plain.bin appears → RED.
func TestScanObjects_SkipsQuorumMetaTombstonesAndNonEC(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.True(t, b.ECActive())
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{
		Bucket: "bkt", Key: "live.bin", VersionID: "v1",
		Size: 1, ETag: "etag", ModTime: 1, ECData: 1, ECParity: 0,
		NodeIDs: []string{b.selfAddr},
	}))
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{
		Bucket: "bkt", Key: "tomb.bin", VersionID: "v2",
		ETag: deleteMarkerETag, IsDeleteMarker: true, ModTime: 1,
		ECData: 1, ECParity: 0, NodeIDs: []string{b.selfAddr},
	}))
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{
		Bucket: "bkt", Key: "plain.bin", VersionID: "v3",
		Size: 1, ETag: "etag3", ModTime: 1, ECData: 0, ECParity: 0,
		NodeIDs: []string{b.selfAddr},
	}))

	got := collectScanObjectKeys(t, b, "bkt")
	require.Equal(t, 1, got["live.bin"], "live EC object must appear")
	require.Equal(t, 0, got["tomb.bin"], "tombstone must be skipped")
	require.Equal(t, 0, got["plain.bin"], "non-EC object must be skipped")
}

// TestScanObjects_FSMTombstoneWinsOverStaleQuorumMeta proves the BLOCKER fix via
// the PRODUCTION delete scenario: PUT a live object, DeleteObject it (raft commit
// writes an FSM lat: tombstone and best-effort-removes quorum-meta), then a stale
// LIVE quorum-meta file reappears for the same key (the best-effort cleanup
// "failed"). The FSM tombstone is authoritative, so the object must NOT be
// scrubbed — the `seen` set records the key even for the lat: tombstone,
// suppressing the stale-live quorum-meta entry.
//
// Mutation: move `seen[key]` to after the tombstone `continue` (i.e. only record
// non-tombstone keys) → the stale-live quorum-meta entry leaks → RED.
func TestScanObjects_FSMTombstoneWinsOverStaleQuorumMeta(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.True(t, b.ECActive())
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// Production scenario: a live object, then deleted.
	_, err := b.PutObject(ctx, "bkt", "gone.bin",
		bytes.NewReader([]byte("x")), "application/octet-stream")
	require.NoError(t, err)
	require.NoError(t, b.DeleteObject(ctx, "bkt", "gone.bin"))

	// Assert the delete took effect (FSM tombstone present, object gone): without
	// the stale quorum-meta below, the object must not be scrubbable.
	pre := collectScanObjectKeys(t, b, "bkt")
	require.Equal(t, 0, pre["gone.bin"], "deleted object must be absent from the scrub set")

	// Stale LIVE quorum-meta reappears for the SAME key (best-effort cleanup
	// failed). The FSM tombstone must still win → object stays out of the set.
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{
		Bucket: "bkt", Key: "gone.bin", VersionID: "v-old",
		Size: 1, ETag: "etag-old", ModTime: 1, ECData: 1, ECParity: 0,
		NodeIDs: []string{b.selfAddr},
	}))

	got := collectScanObjectKeys(t, b, "bkt")
	require.Equal(t, 0, got["gone.bin"], "deleted object must not be scrubbed despite stale quorum-meta")
}

// TestScanObjects_NilShardSvcSafe guards that ScanObjects does not panic and
// yields no records when the shard service is absent (no quorum-meta store).
func TestScanObjects_NilShardSvcSafe(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.True(t, b.ECActive())
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	b.shardSvc = nil // simulate missing shard service

	got := collectScanObjectKeys(t, b, "bkt")
	require.Equal(t, 0, len(got), "no shard service → no scrub records (no panic)")
}

// TestECScrubSource_SeesQuorumMetaObject proves the live scrubber source path
// yields a Block for a regular-PUT EC object so the Director-driven EC scrub
// pipeline will process it. This is GENUINE (not vacuous): the Iter
// ObjectExists + OwnedShards filters must both pass, which exercises the
// production placement-resolution path for a quorum-meta-only object —
// readPlacementMeta reads placement from quorum-meta (the Phase 3 primary source
// for non-internal buckets), so OwnedShards resolves it without any FSM meta.
//
// Identity invariant mirrored from production: the placement NodeIDs and the
// scrubber's source nodeID are the same identity (here b.selfAddr — the value
// writeQuorumMeta uses for the local write AND the value OwnedShards matches).
func TestECScrubSource_SeesQuorumMetaObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := newTestDistributedBackend(t)
	require.True(t, b.ECActive())
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// Regular-PUT object: quorum-meta entry only (NodeIDs = self → local write,
	// and the placement OwnedShards resolves from quorum-meta).
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{
		Bucket: "bkt", Key: "regular.bin", VersionID: "v1",
		Size: 1, ETag: "etag", ModTime: 1, ECData: 1, ECParity: 0,
		NodeIDs: []string{b.selfAddr},
	}))

	// Sanity: the Iter filters' preconditions hold for the quorum-only object.
	exists, err := b.ObjectExists("bkt", "regular.bin")
	require.NoError(t, err)
	require.True(t, exists, "regular-PUT object must be resolvable by ObjectExists/HeadObject")
	require.NotEmpty(t, b.OwnedShards("bkt", "regular.bin", "v1", b.selfAddr),
		"OwnedShards must resolve a quorum-meta-only object via quorum-meta placement")

	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(b), b.selfAddr)
	ch, err := src.Iter(ctx, "bkt", "")
	require.NoError(t, err)

	var keys []string
	for blk := range ch {
		keys = append(keys, blk.Key)
	}
	require.Contains(t, keys, "regular.bin", "scrub source must yield the regular-PUT object")
}
