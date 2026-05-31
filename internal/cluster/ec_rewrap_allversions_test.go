package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestECRewrapAllVersions_NonLatestLegacyAndCrossBucket proves that the EC
// rewrap lane re-encrypts shards of ALL object versions (non-latest, legacy
// unversioned) across ALL buckets.
//
// Cross-bucket RED-first (MAJOR-1) proof:
// A bucket-aware (bucket+"\x00"+key) latestMap correctly rewraps a b2 versioned
// SEGMENTED object whose tail (key+"/"+versionID) collides with a b1 lat: key.
// A bare-key latestMap mis-classifies that entry as legacy (VersionID=""),
// which triggers the VersionID=="" guard in buildECShardTargets, silently
// dropping the segment target and leaving the segment shard un-rewrapped.
//
// The plain b2/obj1 case (legacy, tail has no slash) is included for
// cross-bucket enumeration coverage but is NOT the teeth: it never consults
// latestMap (lslash < 0 unconditional legacy branch). See comment below.
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
		require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
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
	require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(f.keys.LatestKey("b1", "obj1"), []byte("v1"))
	}))

	// 2. b1/legacyA (legacy unversioned, no lat:)
	seedMeta(t, f.keys.ObjectMetaKey("b1", "legacyA"), "legacyA", objectMeta{
		ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})

	// 3. b2/obj1 (legacy unversioned in b2, same key-string as b1's versioned
	// key). Cross-bucket ENUMERATION coverage: proves b2 objects are visited
	// regardless of b1's lat: for the same key name. Note: this entry's tail
	// "obj1" has no slash → always hits the lslash<0 branch → latestMap is
	// NOT consulted. This is not the latestMap collision case (see below).
	seedMeta(t, f.keys.ObjectMetaKey("b2", "obj1"), "obj1", objectMeta{
		ECData: ecData, ECParity: ecParity, NodeIDs: selfNodes,
	})

	// ---- MAJOR-1 teeth: cross-bucket latestMap collision via segmented object ----
	//
	// The bare-key latestMap collision fires only when tail (rest after bucket
	// slash in an obj: entry) HAS a slash AND that tail string equals a key
	// stored in latestMap.
	//
	// Decoy: plant a lat: pointer in b1 for key "seg/vtag" (a key that contains
	// a slash). This populates:
	//   bucket-scoped: latestMap["b1\x00seg/vtag"] = "somevid"
	//   bare-key bug:  latestMap["seg/vtag"]       = "somevid"
	const decoyKey = "seg/vtag" // slash-containing key — matches b2's tail below
	require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(f.keys.LatestKey("b1", decoyKey), []byte("somevid"))
	}))
	// No b1 obj: entry for decoyKey needed; the lat: alone populates latestMap.

	// Target: b2 versioned SEGMENTED object with key="seg", versionID="vtag".
	// Its obj: entry is obj:b2/seg/vtag, so tail = "seg/vtag".
	//   • Bucket-scoped check: latestMap["b2\x00seg/vtag"] → absent →
	//     CORRECTLY classified as versioned (key="seg", vid="vtag") →
	//     VersionID non-empty → segment target emitted → shard rewrapped. ✓
	//   • Bare-key bug:        latestMap["seg/vtag"]       → FOUND (b1 decoy) →
	//     MIS-classified as legacy (key="seg/vtag", vid="") →
	//     VersionID=="" guard in buildECShardTargets drops segment target →
	//     segment shard NOT rewrapped. ✗ (RED assertion)
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
	// No lat: in b2 for "seg" (entry is non-latest to keep setup minimal).

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

	// Segment shard for the MAJOR-1 case.
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
	// MAJOR-1 assertion: the cross-bucket segment shard must be rewrapped.
	// With bare-key latestMap the segment target is dropped → gen stays 0.
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b2", segShardKey, 0),
		"b2/seg@vtag segment shard (MAJOR-1 cross-bucket latestMap collision) must be rewrapped")

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
