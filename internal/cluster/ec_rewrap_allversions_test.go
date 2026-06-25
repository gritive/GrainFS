package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestECRewrapAllVersions_NonLatestLegacyAndCrossBucket proves that the EC
// rewrap lane re-encrypts shards of ALL object versions (non-latest included)
// across ALL buckets, sourced from the quorum-meta blob authority:
//   - a versioning-enabled bucket's per-version blobs (non-latest + latest),
//   - a non-versioned bucket's latest-only blobs (object-version + segment shards),
//   - cross-bucket enumeration (objects in every bucket are visited).
//
// (The former lat:-heuristic collision decoys — cross-bucket / same-bucket lat:
// pointers for a slash-containing key — were FSM-list-path mechanisms with no
// blob analogue and are dropped: CollectECRewrapTargets enumerates blobs only.)
func TestECRewrapAllVersions_NonLatestLegacyAndCrossBucket(t *testing.T) {
	backend, keeper := setupECRewrapBackend(t)
	ctx := context.Background()

	// b1 versioning-enabled (per-version blobs); b2 non-versioned (latest-only blobs).
	require.NoError(t, backend.CreateBucket(ctx, "b1"))
	require.NoError(t, backend.SetBucketVersioning("b1", "Enabled"))
	require.NoError(t, backend.CreateBucket(ctx, "b2"))

	svc := backend.shardSvc
	selfID := backend.selfAddr // "self" (set by setupECRewrapBackend)
	const (
		ecData   = uint8(1)
		ecParity = uint8(0)
	)
	selfNodes := []string{selfID}

	// ---- Seed object metadata (quorum-meta blob authority) ----

	// b1/obj1 @v0 (non-latest) and @v1 (latest) — per-version blobs.
	seedVersionBlob(t, backend, "b1", "obj1", "v0", PutObjectMetaCmd{
		ETag: "e", ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes, ModTime: 1,
	})
	seedVersionBlob(t, backend, "b1", "obj1", "v1", PutObjectMetaCmd{
		ETag: "e", ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes, ModTime: 2,
	})

	// b2/obj1 (non-versioned legacy, same key-string as b1's key) — latest-only
	// blob. Cross-bucket enumeration coverage: b2 objects are visited regardless
	// of b1's same-named key.
	seedLatestBlob(t, backend, "b2", "obj1", PutObjectMetaCmd{
		ETag: "e", ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})

	// b1/seg@vtag (versioned, segmented) — per-version blob carrying a segment
	// shard (no object-version shard: top-level ECData=0). Segment shards only
	// exist on versioned objects (the writer always mints a version), so this
	// lives in the versioning-enabled bucket.
	const (
		segKey    = "seg"
		segVID    = "vtag"
		segBlobID = "blob1"
	)
	segShardKey := segKey + "/segments/" + segBlobID
	seedVersionBlob(t, backend, "b1", segKey, segVID, PutObjectMetaCmd{
		ECData: 0, ECParity: ecParity, NodeIDs: nil, ModTime: 3,
		Segments: []SegmentMetaEntry{
			{BlobID: segBlobID, ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes, SegmentIdx: 0},
		},
	})

	// ---- Write GFSENC3 shards at gen 0 ----

	shardContent := func(tag string) []byte {
		return bytes.Repeat([]byte(tag+"-payload-"), 64)
	}

	shardKeyV0 := ecObjectShardKey("obj1", "v0")
	require.NoError(t, svc.WriteLocalShard("b1", shardKeyV0, 0, shardContent("b1-obj1-v0")))

	shardKeyV1 := ecObjectShardKey("obj1", "v1")
	require.NoError(t, svc.WriteLocalShard("b1", shardKeyV1, 0, shardContent("b1-obj1-v1")))

	shardKeyB2Plain := ecObjectShardKey("obj1", "")
	require.NoError(t, svc.WriteLocalShard("b2", shardKeyB2Plain, 0, shardContent("b2-obj1")))

	// Segment shard of the versioned segmented object.
	require.NoError(t, svc.WriteLocalShard("b1", segShardKey, 0, shardContent("b1-seg")))

	// Verify all four shards start at gen 0.
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b1", shardKeyV0, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b1", shardKeyV1, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b2", shardKeyB2Plain, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b1", segShardKey, 0))

	// ---- Rotate DEK: gen 0 → 1 ----
	require.NoError(t, keeper.Rotate())
	_, activeGen := keeper.VersionsAndActive()
	require.Equal(t, uint32(1), activeGen)

	// ---- Drive the lane ----
	lane := laneFromGroups(selfID, backend)
	require.NoError(t, lane.RewrapByGen(t.Context(), 0, activeGen))

	// ---- Assert all four shards migrated to gen 1 ----
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV0, 0),
		"b1/obj1@v0 (non-latest) must be rewrapped")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV1, 0),
		"b1/obj1@v1 (latest) must be rewrapped")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", shardKeyB2Plain, 0),
		"b2/obj1 (cross-bucket non-versioned, enumeration coverage) must be rewrapped")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", segShardKey, 0),
		"b1/seg@vtag segment shard must be rewrapped")

	// ---- Plaintext preserved ----
	for _, tc := range []struct {
		bucket, shardKey, tag string
	}{
		{"b1", shardKeyV0, "b1-obj1-v0"},
		{"b1", shardKeyV1, "b1-obj1-v1"},
		{"b2", shardKeyB2Plain, "b2-obj1"},
		{"b1", segShardKey, "b1-seg"},
	} {
		got, err := svc.ReadLocalShard(tc.bucket, tc.shardKey, 0)
		require.NoError(t, err, "read shard %s/%s", tc.bucket, tc.shardKey)
		assert.Equal(t, shardContent(tc.tag), got, "plaintext preserved for %s/%s", tc.bucket, tc.shardKey)
	}

	// ---- Idempotent: second sweep, gens stay at 1 ----
	require.NoError(t, lane.RewrapByGen(t.Context(), 0, activeGen))
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV0, 0), "idempotent: b1/v0")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV1, 0), "idempotent: b1/v1")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", shardKeyB2Plain, 0), "idempotent: b2/plain")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", segShardKey, 0), "idempotent: b1/seg")
}

// TestCollectECRewrapTargets_VersionedBlobNoFSM proves the DEK rewrap enumeration
// covers a versioning-enabled object that exists ONLY as a per-version blob (no FSM
// obj: record — the post-propose-removal steady state), and EXCLUDES a hard-delete
// tombstone. Without this, KEK rotation would silently skip every versioned object's
// shards once the propose is removed (unrotated DEK).
func TestCollectECRewrapTargets_VersionedBlobNoFSM(t *testing.T) {
	backend, _ := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "vb"))
	require.NoError(t, backend.SetBucketVersioning("vb", "Enabled"))
	self := backend.selfAddr

	// versioned object via per-version blob ONLY (no FSM record).
	seedVersionBlob(t, backend, "vb", "k", "v1", PutObjectMetaCmd{ETag: "e", ECData: 1, ECParity: 0, NodeIDs: []string{self}})
	// hard-delete tombstone → must NOT be enumerated for rewrap.
	seedVersionBlob(t, backend, "vb", "k", "v2", PutObjectMetaCmd{ETag: "e", ECData: 1, ECParity: 0, NodeIDs: []string{self}, IsHardDeleted: true})

	targets, err := backend.CollectECRewrapTargets()
	require.NoError(t, err)
	var vbKeys []string
	for _, tgt := range targets {
		if tgt.Bucket == "vb" {
			vbKeys = append(vbKeys, tgt.ShardKey)
		}
	}
	require.Contains(t, vbKeys, ecObjectShardKey("k", "v1"), "blob-only versioned object must be enumerated for rewrap")
	require.NotContains(t, vbKeys, ecObjectShardKey("k", "v2"), "hard-delete tombstone must not be enumerated")
}
