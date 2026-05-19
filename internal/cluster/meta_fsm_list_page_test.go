package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestObjectIndexLatestEntriesPage_Marker exercises the pagination contract
// the S3 ListObjects handler depends on: results are sorted by key, entries
// with key > marker are returned, the page is capped at maxKeys, and the
// truncated flag reports whether more entries match beyond the slice.
func TestObjectIndexLatestEntriesPage_Marker(t *testing.T) {
	f := NewMetaFSM()
	bucket := "b1"

	// Seed 5 objects keyed a..e on a single bucket.
	for _, key := range []string{"e", "a", "c", "b", "d"} {
		vid := "v-" + key
		f.objectLatest[objectIndexLatestKey(bucket, key)] = vid
		f.objectIndex[objectIndexVersionKey(bucket, key, vid)] = ObjectIndexEntry{
			Bucket:           bucket,
			Key:              key,
			VersionID:        vid,
			Size:             1,
			ETag:             "etag-" + key,
			PlacementGroupID: "group-0",
		}
	}

	cases := []struct {
		name      string
		prefix    string
		marker    string
		maxKeys   int
		wantKeys  []string
		wantTrunc bool
	}{
		{name: "first page", marker: "", maxKeys: 2, wantKeys: []string{"a", "b"}, wantTrunc: true},
		{name: "second page", marker: "b", maxKeys: 2, wantKeys: []string{"c", "d"}, wantTrunc: true},
		{name: "tail page exact", marker: "c", maxKeys: 2, wantKeys: []string{"d", "e"}, wantTrunc: false},
		{name: "past end", marker: "z", maxKeys: 2, wantKeys: []string{}, wantTrunc: false},
		{name: "no cap", marker: "", maxKeys: 10, wantKeys: []string{"a", "b", "c", "d", "e"}, wantTrunc: false},
		{name: "max 0 returns all", marker: "", maxKeys: 0, wantKeys: []string{"a", "b", "c", "d", "e"}, wantTrunc: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entries, truncated := f.ObjectIndexLatestEntriesPage(bucket, tc.prefix, tc.marker, tc.maxKeys)
			gotKeys := make([]string, 0, len(entries))
			for _, e := range entries {
				gotKeys = append(gotKeys, e.Key)
			}
			require.Equal(t, tc.wantKeys, gotKeys)
			require.Equal(t, tc.wantTrunc, truncated)
		})
	}
}

// TestObjectIndexLatestEntriesPage_PrefixAndMarker verifies prefix filtering
// composes with marker pagination: only keys matching the prefix and strictly
// greater than marker are returned.
func TestObjectIndexLatestEntriesPage_PrefixAndMarker(t *testing.T) {
	f := NewMetaFSM()
	bucket := "b1"

	for _, key := range []string{"a/1", "a/2", "a/3", "b/1", "b/2"} {
		vid := "v-" + key
		f.objectLatest[objectIndexLatestKey(bucket, key)] = vid
		f.objectIndex[objectIndexVersionKey(bucket, key, vid)] = ObjectIndexEntry{
			Bucket:           bucket,
			Key:              key,
			VersionID:        vid,
			Size:             1,
			ETag:             "etag",
			PlacementGroupID: "group-0",
		}
	}

	entries, truncated := f.ObjectIndexLatestEntriesPage(bucket, "a/", "a/1", 10)
	require.False(t, truncated)
	require.Equal(t, 2, len(entries))
	require.Equal(t, "a/2", entries[0].Key)
	require.Equal(t, "a/3", entries[1].Key)
}

// TestObjectIndexLatestEntriesPage_SkipsDeleteMarkers makes sure deletion
// tombstones are excluded from listing results — they were never visible in
// the pre-pagination call and must stay invisible after.
func TestObjectIndexLatestEntriesPage_SkipsDeleteMarkers(t *testing.T) {
	f := NewMetaFSM()
	bucket := "b1"

	live := ObjectIndexEntry{
		Bucket: bucket, Key: "alive", VersionID: "v1", Size: 1, ETag: "e",
		PlacementGroupID: "group-0",
	}
	tomb := ObjectIndexEntry{
		Bucket: bucket, Key: "gone", VersionID: "v1", IsDeleteMarker: true,
		PlacementGroupID: "group-0",
	}
	f.objectLatest[objectIndexLatestKey(bucket, "alive")] = "v1"
	f.objectIndex[objectIndexVersionKey(bucket, "alive", "v1")] = live
	f.objectLatest[objectIndexLatestKey(bucket, "gone")] = "v1"
	f.objectIndex[objectIndexVersionKey(bucket, "gone", "v1")] = tomb

	entries, truncated := f.ObjectIndexLatestEntriesPage(bucket, "", "", 10)
	require.False(t, truncated)
	require.Equal(t, 1, len(entries))
	require.Equal(t, "alive", entries[0].Key)
}
