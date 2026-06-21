package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestECRewrapAllVersions_NonLatestLegacyAndCrossBucket proves that the EC
// rewrap lane re-encrypts shards of ALL object versions (non-latest, legacy
// unversioned) across ALL buckets.
//
// The authoritative-m.Key derivation closes two collision classes:
//
//   - Cross-bucket: a lat: pointer in b1 for key "seg/vtag" would, under the old
//     lat:-heuristic, cause the b2 versioned entry obj:b2/seg/vtag to be
//     misclassified as legacy (VersionID=""). The m.Key field ("seg") gives the
//     exact key, leaving "vtag" as the version ID.
//
//   - Same-bucket: a lat: pointer in b2 for key "seg/vtag" triggers the same
//     misclassification even with the bucket-scoped heuristic — the fix replaces
//     the lat:-heuristic entirely with the m.Key derive.
func TestECRewrapAllVersions_NonLatestLegacyAndCrossBucket(t *testing.T) {
	backend, keeper := setupECRewrapBackend(t)
	ctx := context.Background()

	// Create both buckets so shard directory creation works.
	require.NoError(t, backend.CreateBucket(ctx, "b1"))
	require.NoError(t, backend.CreateBucket(ctx, "b2"))

	svc := backend.shardSvc
	selfID := backend.selfAddr // "self" (set by setupECRewrapBackend)
	const (
		ecData   = uint8(1)
		ecParity = uint8(0)
	)
	selfNodes := []string{selfID}

	f := backend.FSMRef()

	seedMeta := func(t *testing.T, metaKey []byte, key string, m objectMeta) {
		t.Helper()
		m.Key = key
		val, err := marshalObjectMeta(m)
		require.NoError(t, err)
		require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
			return f.setValue(txn, metaKey, val)
		}))
	}

	// ---- Seed object metadata ----

	// 1. b1/obj1 v0 (non-latest, no lat:)
	seedMeta(t, f.keys.ObjectMetaKeyV("b1", "obj1", "v0"), "obj1", objectMeta{
		ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})

	// 1. b1/obj1 v1 (latest, with lat:)
	seedMeta(t, f.keys.ObjectMetaKeyV("b1", "obj1", "v1"), "obj1", objectMeta{
		ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})
	require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
		return txn.Set(f.keys.LatestKey("b1", "obj1"), []byte("v1"))
	}))

	// 2. b1/legacyA (legacy unversioned, no lat:)
	seedMeta(t, f.keys.ObjectMetaKey("b1", "legacyA"), "legacyA", objectMeta{
		ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})

	// 3. b2/obj1 (legacy unversioned in b2, same key-string as b1's versioned
	// key). Cross-bucket enumeration coverage: proves b2 objects are visited
	// regardless of b1's lat: for the same key name.
	seedMeta(t, f.keys.ObjectMetaKey("b2", "obj1"), "obj1", objectMeta{
		ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})

	// ---- Cross-bucket collision case ----
	//
	// A lat: pointer in b1 for key "seg/vtag" (a slash-containing key) would,
	// under the old lat:-heuristic, cause obj:b2/seg/vtag to be misclassified
	// as legacy (key="seg/vtag", versionID="") when looking up the bucket-scoped
	// map for b2. With m.Key derivation, m.Key="seg" gives the exact key and
	// "vtag" becomes the version ID — correctly emitting the segment target.
	const decoyKey = "seg/vtag" // slash-containing key — collides with b2's tail
	require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
		return txn.Set(f.keys.LatestKey("b1", decoyKey), []byte("somevid"))
	}))
	// No b1 obj: entry for decoyKey needed; the lat: alone populated latestMap.

	const (
		segKey    = "seg"
		segVID    = "vtag"
		segBlobID = "blob1"
	)
	segShardKey := segKey + "/segments/" + segBlobID
	seedMeta(t, f.keys.ObjectMetaKeyV("b2", segKey, segVID), segKey, objectMeta{
		// Top-level ECData=0: no object-version shard, only segment shard.
		ECData: 0, ECParity: ecParity, NodeIDs: nil,
		Segments: []storage.SegmentRef{
			{BlobID: segBlobID, ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes},
		},
	})

	// ---- Same-bucket collision case ----
	//
	// A lat: pointer in b2 itself for key "seg/vtag" fires the same-bucket
	// variant: latestMap["b2\x00seg/vtag"] is set, so obj:b2/seg/vtag would be
	// misclassified as legacy even with the bucket-scoped heuristic. The m.Key
	// derivation closes this class: m.Key="seg" is authoritative regardless of
	// what any lat: entry says.
	//
	// We reuse the same versioned segment object already seeded above and add
	// a same-bucket decoy lat: pointer. No second obj: record needed (two metas
	// for obj:b2/seg/vtag would overwrite each other — they share the same key).
	require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
		return txn.Set(f.keys.LatestKey("b2", decoyKey), []byte("somevid2"))
	}))
	// segShardKey is already seeded above; the same-bucket decoy lat: just adds
	// a second misclassification trigger for the same b2/seg/vtag entry.

	// ---- Write GFSENC3 shards at gen 0 ----

	shardContent := func(tag string) []byte {
		return bytes.Repeat([]byte(tag+"-payload-"), 64)
	}

	shardKeyV0 := ecObjectShardKey("obj1", "v0")
	require.NoError(t, svc.WriteLocalShard("b1", shardKeyV0, 0, shardContent("b1-obj1-v0")))

	shardKeyV1 := ecObjectShardKey("obj1", "v1")
	require.NoError(t, svc.WriteLocalShard("b1", shardKeyV1, 0, shardContent("b1-obj1-v1")))

	shardKeyLegacyA := ecObjectShardKey("legacyA", "")
	require.NoError(t, svc.WriteLocalShard("b1", shardKeyLegacyA, 0, shardContent("b1-legacyA")))

	shardKeyB2Plain := ecObjectShardKey("obj1", "")
	require.NoError(t, svc.WriteLocalShard("b2", shardKeyB2Plain, 0, shardContent("b2-obj1")))

	// Segment shard for both cross-bucket and same-bucket collision cases.
	require.NoError(t, svc.WriteLocalShard("b2", segShardKey, 0, shardContent("b2-seg")))

	// Verify all five shards start at gen 0.
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b1", shardKeyV0, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b1", shardKeyV1, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b1", shardKeyLegacyA, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b2", shardKeyB2Plain, 0))
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b2", segShardKey, 0))

	// ---- Rotate DEK: gen 0 → 1 ----
	require.NoError(t, keeper.Rotate())
	_, activeGen := keeper.VersionsAndActive()
	require.Equal(t, uint32(1), activeGen)

	// ---- Drive the lane ----
	lane := laneFromGroups(selfID, backend)
	require.NoError(t, lane.RewrapByGen(t.Context(), 0, activeGen))

	// ---- Assert all five shards migrated to gen 1 ----
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV0, 0),
		"b1/obj1@v0 (non-latest) must be rewrapped")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV1, 0),
		"b1/obj1@v1 (latest) must be rewrapped")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyLegacyA, 0),
		"b1/legacyA (legacy unversioned) must be rewrapped")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", shardKeyB2Plain, 0),
		"b2/obj1 (cross-bucket legacy, enumeration coverage) must be rewrapped")
	// Cross-bucket and same-bucket collision: both decoy lat: entries (b1 and b2
	// for "seg/vtag") must not prevent the b2/seg@vtag segment shard from being
	// re-encrypted.
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", segShardKey, 0),
		"b2/seg@vtag segment shard (cross+same-bucket lat: collision) must be rewrapped")

	// ---- Plaintext preserved ----
	for _, tc := range []struct {
		bucket, shardKey, tag string
	}{
		{"b1", shardKeyV0, "b1-obj1-v0"},
		{"b1", shardKeyV1, "b1-obj1-v1"},
		{"b1", shardKeyLegacyA, "b1-legacyA"},
		{"b2", shardKeyB2Plain, "b2-obj1"},
		{"b2", segShardKey, "b2-seg"},
	} {
		got, err := svc.ReadLocalShard(tc.bucket, tc.shardKey, 0)
		require.NoError(t, err, "read shard %s/%s", tc.bucket, tc.shardKey)
		assert.Equal(t, shardContent(tc.tag), got, "plaintext preserved for %s/%s", tc.bucket, tc.shardKey)
	}

	// ---- Idempotent: second sweep, gens stay at 1 ----
	require.NoError(t, lane.RewrapByGen(t.Context(), 0, activeGen))
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV0, 0), "idempotent: b1/v0")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyV1, 0), "idempotent: b1/v1")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b1", shardKeyLegacyA, 0), "idempotent: b1/legacy")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", shardKeyB2Plain, 0), "idempotent: b2/plain")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", segShardKey, 0), "idempotent: b2/seg")
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
