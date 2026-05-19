package lifecycle_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/storage"
)

func ver(size int64, tags ...storage.Tag) storage.ObjectVersionRecord {
	return storage.ObjectVersionRecord{
		IsLatest: true, Size: size, Tags: tags,
	}
}

func TestMatchFilter_NilMatchesAll(t *testing.T) {
	v := ver(0)
	require.True(t, lifecycle.MatchFilter(&v, "anything", nil))
}

func TestMatchFilter_PrefixOnly(t *testing.T) {
	v := ver(0)
	f := &lifecycle.Filter{Prefix: "logs/"}
	require.True(t, lifecycle.MatchFilter(&v, "logs/x", f))
	require.False(t, lifecycle.MatchFilter(&v, "other/x", f))
}

func TestMatchFilter_Size(t *testing.T) {
	v := ver(1000)
	gt := int64(500)
	lt := int64(2000)
	require.True(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{ObjectSizeGreaterThan: &gt}))
	require.True(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{ObjectSizeLessThan: &lt}))
	tooHigh := int64(2000)
	require.False(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{ObjectSizeGreaterThan: &tooHigh}))
	eq := int64(1000) // exclusive: size > N means strictly greater
	require.False(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{ObjectSizeGreaterThan: &eq}))
}

func TestMatchFilter_Tag(t *testing.T) {
	v := ver(0, storage.Tag{Key: "env", Value: "prod"})
	require.True(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{Tag: &lifecycle.Tag{Key: "env", Value: "prod"}}))
	require.False(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{Tag: &lifecycle.Tag{Key: "env", Value: "dev"}}))
	require.False(t, lifecycle.MatchFilter(&v, "k", &lifecycle.Filter{Tag: &lifecycle.Tag{Key: "missing", Value: "x"}}))
}

func TestMatchFilter_And(t *testing.T) {
	v := ver(1000, storage.Tag{Key: "env", Value: "prod"})
	gt := int64(500)
	f := &lifecycle.Filter{And: &lifecycle.AndFilter{
		Prefix:                "logs/",
		Tags:                  []lifecycle.Tag{{Key: "env", Value: "prod"}},
		ObjectSizeGreaterThan: &gt,
	}}
	require.True(t, lifecycle.MatchFilter(&v, "logs/x", f))

	// Same filter but missing tag fails AND
	v2 := ver(1000)
	require.False(t, lifecycle.MatchFilter(&v2, "logs/x", f))

	// Same filter but wrong prefix fails AND
	require.False(t, lifecycle.MatchFilter(&v, "other/x", f))

	// Same filter but size below threshold fails AND
	v3 := ver(100, storage.Tag{Key: "env", Value: "prod"})
	require.False(t, lifecycle.MatchFilter(&v3, "logs/x", f))
}
