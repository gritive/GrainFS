package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestClusterCoordinator_ListObjectVersions_FansOutAcrossGroups proves the
// coordinator-level fix: objects are key-hash placed across shard groups, so a
// versioned LIST must fan out across ALL groups and union. RED before the fix:
// ListObjectVersions routed via RouteBucket to a single group and returned only
// the versions of keys that happened to live in the bucket's assigned group.
func TestClusterCoordinator_ListObjectVersions_FansOutAcrossGroups(t *testing.T) {
	gbA, _ := newTestFollowerGroupBackend(t, "ga", "self")
	gbB, _ := newTestFollowerGroupBackend(t, "gb", "self")
	for _, gb := range []*GroupBackend{gbA, gbB} {
		gb.Node().Start()
		stop := make(chan struct{})
		go gb.RunApplyLoop(stop)
		t.Cleanup(func() { close(stop) })
	}

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("ga", []string{"self"}, gbA))
	mgr.Add(NewDataGroupWithBackend("gb", []string{"self"}, gbB))
	router := NewRouter(mgr)
	router.AssignBucket("vbkt", "ga")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"ga": {ID: "ga", PeerIDs: []string{"self"}},
		"gb": {ID: "gb", PeerIDs: []string{"self"}},
	}}
	ec := ECConfig{DataShards: 1, ParityShards: 0}
	c := NewClusterCoordinator(&fakeBackend{}, mgr, router, meta, "self").WithECConfig(ec)

	// Blob-primary: the leaf backends must know vbkt is versioning-enabled so their
	// ListObjectVersions takes the per-version blob path (not the now-removed FSM
	// obj: scan). Persist the bucket + versioning state on both groups.
	for _, gb := range []*GroupBackend{gbA, gbB} {
		require.NoError(t, gb.CreateBucket(context.Background(), "vbkt"))
		require.NoError(t, gb.SetBucketVersioning("vbkt", "Enabled"))
	}

	// Pick one key that hashes to each group, using the exact candidate set
	// RouteObjectWrite uses (placement assertion below catches any mismatch).
	cand, err := candidateGroupsFor(meta.ShardGroups(), ec)
	require.NoError(t, err)
	ids := make([]string, len(cand))
	for i, g := range cand {
		ids[i] = g.ID
	}
	keyFor := func(want string) string {
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("obj-%d", i)
			if groupIDForObject("vbkt", k, ids) == want {
				return k
			}
		}
		t.Fatalf("no key hashes to group %s", want)
		return ""
	}
	keyA, keyB := keyFor("ga"), keyFor("gb")
	require.NotEqual(t, keyA, keyB)

	ctx := ContextWithBucketVersioning(context.Background(), true)
	put := func(key, body string) string {
		sz := int64(len(body))
		obj, err := c.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket: "vbkt", Key: key, Body: bytes.NewReader([]byte(body)),
			ContentType: "text/plain", SizeHint: &sz,
		})
		require.NoError(t, err)
		require.NotEmpty(t, obj.VersionID)
		return obj.VersionID
	}
	a1, a2 := put(keyA, "a1"), put(keyA, "a2")
	b1, b2 := put(keyB, "b1"), put(keyB, "b2")

	// Placement assertion: the two keys really landed in DIFFERENT groups, so
	// the fan-out is genuinely exercised (otherwise the test is vacuous).
	aLocal, err := gbA.ListObjectVersions(context.Background(), "vbkt", "", 0)
	require.NoError(t, err)
	bLocal, err := gbB.ListObjectVersions(context.Background(), "vbkt", "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, aLocal, "keyA must live in group ga")
	require.NotEmpty(t, bLocal, "keyB must live in group gb")

	versions, err := c.ListObjectVersions(ctx, "vbkt", "", 0)
	require.NoError(t, err)
	got := map[string][]string{}
	latest := map[string]string{}
	for _, v := range versions {
		got[v.Key] = append(got[v.Key], v.VersionID)
		if v.IsLatest {
			require.Empty(t, latest[v.Key], "key %s has >1 IsLatest", v.Key)
			latest[v.Key] = v.VersionID
		}
	}
	require.ElementsMatch(t, []string{a1, a2}, got[keyA], "all versions of keyA (in group ga)")
	require.ElementsMatch(t, []string{b1, b2}, got[keyB], "all versions of keyB (in group gb) — RED: missing under single-group routing")
	require.Equal(t, a2, latest[keyA], "keyA latest is its 2nd PUT")
	require.Equal(t, b2, latest[keyB], "keyB latest is its 2nd PUT")
	require.Len(t, versions, 4)
}

// TestReconcileVersionIsLatest covers the cross-group IsLatest collapse:
//   - a key flagged IsLatest in TWO groups (divergent lat:) collapses to exactly
//     one IsLatest — the max-VersionID (newest UUIDv7) wins;
//   - a non-flagged newer version (e.g. a PreserveLatest write) is NEVER promoted.
func TestReconcileVersionIsLatest(t *testing.T) {
	versions := []*storage.ObjectVersion{
		// key "a": two groups each flagged their own lat: latest.
		{Key: "a", VersionID: "v3", IsLatest: true}, // newer — should stay latest
		{Key: "a", VersionID: "v1", IsLatest: true}, // older flagged — demote
		{Key: "a", VersionID: "v2", IsLatest: false},
		// key "b": a non-flagged version with a HIGHER VersionID than the flagged
		// one (PreserveLatest-style) must not become latest.
		{Key: "b", VersionID: "v9", IsLatest: false}, // newest but not flagged
		{Key: "b", VersionID: "v5", IsLatest: true},  // the real latest
	}
	reconcileVersionIsLatest(versions)

	latest := map[string]string{}
	for _, v := range versions {
		if v.IsLatest {
			require.Empty(t, latest[v.Key], "key %s has >1 IsLatest", v.Key)
			latest[v.Key] = v.VersionID
		}
	}
	require.Equal(t, "v3", latest["a"], "newest flagged version wins the cross-group tie")
	require.Equal(t, "v5", latest["b"], "non-flagged newer version must not be promoted")
}

// TestDedupVersionsKeepFirst covers the coordinator's soleauth=on dedup: every
// leaf returns the identical cluster-wide blob view (→ N× duplicates), which the
// coordinator collapses by (Key,VersionID) keeping the first; carve-out keys
// owned by one group never duplicate and survive intact.
func TestDedupVersionsKeepFirst(t *testing.T) {
	// Two leaves each returned the same two blob versions of key "k" (duplicated),
	// plus group-A's local carve-out "ck" and group-B's local carve-out "bare".
	versions := []*storage.ObjectVersion{
		{Key: "k", VersionID: "v2", IsLatest: true, ETag: "blob-v2"},  // leaf A
		{Key: "k", VersionID: "v1", IsLatest: false, ETag: "blob-v1"}, // leaf A
		{Key: "ck", VersionID: "cv", IsLatest: true, ETag: "carve-A"}, // leaf A carve-out
		{Key: "k", VersionID: "v2", IsLatest: true, ETag: "blob-v2"},  // leaf B dup
		{Key: "k", VersionID: "v1", IsLatest: false, ETag: "blob-v1"}, // leaf B dup
		{Key: "bare", VersionID: "", IsLatest: true, ETag: "carve-B"}, // leaf B carve-out
	}
	out := dedupVersionsKeepFirst(versions)

	idx := versionByKeyVID(out)
	require.Len(t, out, 4, "the two duplicated blob versions collapse; carve-outs survive")
	require.Equal(t, "blob-v2", idx[[2]string{"k", "v2"}].ETag)
	require.Equal(t, "blob-v1", idx[[2]string{"k", "v1"}].ETag)
	require.Equal(t, "carve-A", idx[[2]string{"ck", "cv"}].ETag, "group-A carve-out kept")
	require.Equal(t, "carve-B", idx[[2]string{"bare", ""}].ETag, "group-B carve-out kept")
}

// TestListObjectVersions_SplitKeyNoLocalLatest proves the leaf no longer emits a
// corrupt legacy row for a versioned obj:{bucket}/{key}/{vid} record whose lat:
// pointer is absent locally (cross-group key split, or a PreserveLatest write).
// PreserveLatest writes the per-version key WITHOUT moving lat:, so on a fresh
// key it reproduces exactly the "obj: without lat:" condition a remote-lat: group
// would see. RED before the leaf fix: the row is treated as legacy and emitted
// with Key="{key}/{vid}", VersionID="", IsLatest=true.
func TestListObjectVersions_SplitKeyNoLocalLatest(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key, vid = "vbkt", "obj", "00000000-0000-7000-8000-000000000abc"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	// PreserveLatest: writes obj:{bkt}/{key}/{vid} but NOT lat:{bkt}/{key}.
	require.NoError(t, b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:         bkt,
		Key:            key,
		VersionID:      vid,
		Size:           3,
		ETag:           "etag",
		ModTime:        1,
		PreserveLatest: true,
	}))

	versions, err := b.ListObjectVersions(ctx, bkt, "", 0)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	got := versions[0]
	require.Equal(t, key, got.Key, "key must not glue in the versionID (RED: Key=obj/<vid>)")
	require.Equal(t, vid, got.VersionID, "must surface the real versionID (RED: empty)")
	require.False(t, got.IsLatest, "a no-local-lat: version must not be flagged latest (RED: true)")
}
