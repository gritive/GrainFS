package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Per-site unit coverage for the ModTime-primary latest-version rule: a version
// with a LOWER VersionID but a HIGHER ModTime (the last-completed write, e.g. a
// multipart minted-before-but-completed-after a PUT) must be selected as latest at
// every latest-pick site. ModTimes are explicit so the flip is deterministic — no
// sleep — and proves the comparator no longer reduces to max-VID.
//
// Convention: "vlow" has the smaller VersionID but the larger ModTime (the true
// latest); "vhigh" has the larger VersionID but the smaller ModTime (the stale one).
const (
	vidLowLateMod = "00000000-0000-7000-8000-000000000001" // smaller VID, completed later
	vidHighEarlyM = "00000000-0000-7000-8000-000000000002" // larger VID, completed earlier
	modEarly      = int64(1000)
	modLate       = int64(2000)
)

func TestDeriveLatestVersion_ModTimePrimary(t *testing.T) {
	// vlow completed later (higher ModTime) despite the smaller VID → latest.
	cmds := []PutObjectMetaCmd{
		{VersionID: vidHighEarlyM, ETag: "early-put", ModTime: modEarly},
		{VersionID: vidLowLateMod, ETag: "late-mpu", ModTime: modLate},
	}
	latest, live := deriveLatestVersion(cmds)
	require.True(t, live)
	require.Equal(t, vidLowLateMod, latest.VersionID,
		"the higher-ModTime version wins even though its VID is smaller")
	require.Equal(t, "late-mpu", latest.ETag)
}

func TestDeriveLatestVersion_SameModTimeFallsBackToVID(t *testing.T) {
	// Equal ModTime → deterministic VID tiebreak (no regression for same-second writes).
	cmds := []PutObjectMetaCmd{
		{VersionID: vidLowLateMod, ETag: "lo", ModTime: modEarly},
		{VersionID: vidHighEarlyM, ETag: "hi", ModTime: modEarly},
	}
	latest, live := deriveLatestVersion(cmds)
	require.True(t, live)
	require.Equal(t, vidHighEarlyM, latest.VersionID, "equal ModTime → larger VID wins (tiebreak)")
}

func TestListObjectVersionsBlobAuth_ModTimePrimaryIsLatest(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", vidHighEarlyM, PutObjectMetaCmd{ETag: "early-put", ModTime: modEarly})
	seedVersionBlob(t, b, "b", "k", vidLowLateMod, PutObjectMetaCmd{ETag: "late-mpu", ModTime: modLate})

	vers, err := b.listObjectVersionsBlobAuth("b", "", 0)
	require.NoError(t, err)
	latestByVID := map[string]bool{}
	for _, v := range vers {
		latestByVID[v.VersionID] = v.IsLatest
	}
	require.True(t, latestByVID[vidLowLateMod], "higher-ModTime version is IsLatest")
	require.False(t, latestByVID[vidHighEarlyM], "lower-ModTime (larger VID) version is not latest")
}

func TestListObjectsPerVersion_ModTimePrimaryLatest(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", vidHighEarlyM, PutObjectMetaCmd{ETag: "early-put", ModTime: modEarly})
	seedVersionBlob(t, b, "b", "k", vidLowLateMod, PutObjectMetaCmd{ETag: "late-mpu", ModTime: modLate})

	out, err := b.listObjectsPerVersion(ctx, "b", "")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, vidLowLateMod, out[0].VersionID, "LIST-latest collapses to the higher-ModTime version")
	require.Equal(t, "late-mpu", out[0].ETag)
}

func TestListBlobAuthBucketObjectsForGC_ModTimePrimaryIsLatest(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", vidHighEarlyM, PutObjectMetaCmd{ETag: "early-put", Size: 10, ModTime: modEarly})
	seedVersionBlob(t, b, "b", "k", vidLowLateMod, PutObjectMetaCmd{ETag: "late-mpu", Size: 20, ModTime: modLate})

	objs, err := b.listBlobAuthBucketObjectsForGC("b")
	require.NoError(t, err)
	latestByVID := map[string]bool{}
	for _, o := range objs {
		latestByVID[o.VersionID] = o.IsLatest
	}
	require.True(t, latestByVID[vidLowLateMod], "GC known-set flags the higher-ModTime version latest")
	require.False(t, latestByVID[vidHighEarlyM])
}
