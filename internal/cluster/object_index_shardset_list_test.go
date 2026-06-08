package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLister serves a fixed, key-sorted entry set and mimics the MetaFSM LIST
// contract (marker-exclusive, prefix, maxKeys cap → truncated).
type fakeLister struct {
	latest   []ObjectIndexEntry // pre-sorted by Key asc; one per key (latest)
	versions []ObjectIndexEntry // pre-sorted by (Key asc, VersionID desc)
}

func (f *fakeLister) ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) ([]ObjectIndexEntry, bool) {
	out := make([]ObjectIndexEntry, 0)
	for _, e := range f.latest {
		if e.Bucket != bucket || (prefix != "" && !hasPrefix(e.Key, prefix)) {
			continue
		}
		if marker != "" && e.Key <= marker {
			continue
		}
		out = append(out, e)
	}
	truncated := false
	if maxKeys > 0 && len(out) > maxKeys {
		out = out[:maxKeys]
		truncated = true
	}
	return out, truncated
}
func (f *fakeLister) ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	e, _ := f.ObjectIndexLatestEntriesPage(bucket, prefix, "", maxKeys)
	return e
}
func (f *fakeLister) ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	out := make([]ObjectIndexEntry, 0)
	for _, e := range f.versions {
		if e.Bucket != bucket || (prefix != "" && !hasPrefix(e.Key, prefix)) {
			continue
		}
		out = append(out, e)
	}
	if maxKeys > 0 && len(out) > maxKeys {
		out = out[:maxKeys]
	}
	return out
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func ent(key, ver string) ObjectIndexEntry {
	return ObjectIndexEntry{Bucket: "b", Key: key, VersionID: ver, PlacementGroupID: "g"}
}

func TestShardSet_ListPage_N1_Identity(t *testing.T) {
	l := &fakeLister{latest: []ObjectIndexEntry{ent("a", "1"), ent("c", "1"), ent("e", "1")}}
	set, err := NewObjectIndexShardSet([]ObjectIndexShard{{Lister: l}})
	require.NoError(t, err)
	got, trunc := set.ObjectIndexLatestEntriesPage("b", "", "", 0)
	assert.False(t, trunc)
	assert.Equal(t, []string{"a", "c", "e"}, keysOf(got))
}

func TestShardSet_ListPage_N2_MergeOrdered(t *testing.T) {
	// shard 0 holds a,c,e ; shard 1 holds b,d,f — merged must be globally sorted.
	s0 := &fakeLister{latest: []ObjectIndexEntry{ent("a", "1"), ent("c", "1"), ent("e", "1")}}
	s1 := &fakeLister{latest: []ObjectIndexEntry{ent("b", "1"), ent("d", "1"), ent("f", "1")}}
	set, err := NewObjectIndexShardSet([]ObjectIndexShard{{Lister: s0}, {Lister: s1}})
	require.NoError(t, err)

	all, trunc := set.ObjectIndexLatestEntriesPage("b", "", "", 0)
	assert.False(t, trunc)
	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, keysOf(all))

	// maxKeys page across shards → first 3 globally, truncated.
	page, trunc := set.ObjectIndexLatestEntriesPage("b", "", "", 3)
	assert.True(t, trunc)
	assert.Equal(t, []string{"a", "b", "c"}, keysOf(page))

	// marker continuation across the shard boundary.
	page2, _ := set.ObjectIndexLatestEntriesPage("b", "", "c", 3)
	assert.Equal(t, []string{"d", "e", "f"}, keysOf(page2))
}

func TestShardSet_VersionEntries_N2_MergeKeyAscVersionDesc(t *testing.T) {
	s0 := &fakeLister{versions: []ObjectIndexEntry{ent("a", "3"), ent("a", "1")}}
	s1 := &fakeLister{versions: []ObjectIndexEntry{ent("a", "2"), ent("b", "1")}}
	set, err := NewObjectIndexShardSet([]ObjectIndexShard{{Lister: s0}, {Lister: s1}})
	require.NoError(t, err)
	got := set.ObjectIndexVersionEntries("b", "", 0)
	// key asc, then versionID desc within a key: a/3, a/2, a/1, b/1
	assert.Equal(t, []string{"a", "a", "a", "b"}, keysOf(got))
	assert.Equal(t, []string{"3", "2", "1", "1"}, versOf(got))
}

func keysOf(es []ObjectIndexEntry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Key
	}
	return out
}
func versOf(es []ObjectIndexEntry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.VersionID
	}
	return out
}
