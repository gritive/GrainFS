package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeIndexShard records calls and is both a reader and a writer.
type fakeIndexShard struct {
	latestCalls  [][2]string
	versionCalls [][3]string
	proposed     []ObjectIndexEntry
	deleted      [][3]string
	latestEntry  ObjectIndexEntry
	latestOK     bool
}

func (f *fakeIndexShard) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	f.latestCalls = append(f.latestCalls, [2]string{bucket, key})
	return f.latestEntry, f.latestOK
}

func (f *fakeIndexShard) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	f.versionCalls = append(f.versionCalls, [3]string{bucket, key, versionID})
	return f.latestEntry, f.latestOK
}

func (f *fakeIndexShard) ProposeObjectIndex(_ context.Context, e ObjectIndexEntry, _ bool) error {
	f.proposed = append(f.proposed, e)
	return nil
}

func (f *fakeIndexShard) ProposeDeleteObjectIndex(_ context.Context, bucket, key, versionID string) error {
	f.deleted = append(f.deleted, [3]string{bucket, key, versionID})
	return nil
}

func TestObjectIndexShardSet_N1_AlwaysSelectsShardZero(t *testing.T) {
	s := &fakeIndexShard{latestEntry: ObjectIndexEntry{Bucket: "b", Key: "k"}, latestOK: true}
	set, err := NewObjectIndexShardSet([]ObjectIndexShard{{Reader: s, Writer: s}})
	require.NoError(t, err)
	for _, key := range []string{"a", "b", "zzz", "obj/with/slashes", ""} {
		assert.Equal(t, 0, set.shardFor("bucket", key), "key=%q", key)
	}
}

func TestObjectIndexShardSet_N1_DelegatesReadAndWrite(t *testing.T) {
	s := &fakeIndexShard{latestEntry: ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1"}, latestOK: true}
	set, err := NewObjectIndexShardSet([]ObjectIndexShard{{Reader: s, Writer: s}})
	require.NoError(t, err)

	got, ok := set.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	assert.Equal(t, "v1", got.VersionID)
	assert.Equal(t, [][2]string{{"b", "k"}}, s.latestCalls)

	_, _ = set.ObjectIndexVersion("b", "k", "v1")
	assert.Equal(t, [][3]string{{"b", "k", "v1"}}, s.versionCalls)

	require.NoError(t, set.ProposeObjectIndex(context.Background(), ObjectIndexEntry{Bucket: "b", Key: "k"}, false))
	assert.Len(t, s.proposed, 1)

	require.NoError(t, set.ProposeDeleteObjectIndex(context.Background(), "b", "k", "v1"))
	assert.Equal(t, [][3]string{{"b", "k", "v1"}}, s.deleted)
}

func TestNewObjectIndexShardSet_RejectsEmpty(t *testing.T) {
	_, err := NewObjectIndexShardSet(nil)
	require.Error(t, err)
}
