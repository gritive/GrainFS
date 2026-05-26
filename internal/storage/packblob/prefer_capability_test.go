package packblob

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// prefRecordingInner is a minimal storage.Backend stub that also implements
// the optional PreferReadAt/PreferWriteAt capability probes. It is used only
// to exercise PackedBackend's pass-through of those probes; all real Backend
// methods panic because the test never calls them.
type prefRecordingInner struct {
	storage.Backend
	readAt  map[string]bool
	writeAt map[string]bool
}

func (p *prefRecordingInner) PreferReadAt(bucket string) bool {
	return p.readAt[bucket]
}

func (p *prefRecordingInner) PreferWriteAt(bucket string) bool {
	return p.writeAt[bucket]
}

// silentInner is an inner that does NOT implement the capability probes.
type silentInner struct {
	storage.Backend
}

func TestPackedBackend_PreferReadAtForwardsToInner(t *testing.T) {
	inner := &prefRecordingInner{
		readAt: map[string]bool{"good": true, "bad": false},
	}
	dir := t.TempDir()
	pb, err := NewPackedBackendWithOptions(inner, dir, 1024, PackedBackendOptions{})
	require.NoError(t, err)
	defer pb.Close()

	cases := []struct {
		bucket string
		want   bool
	}{
		{"good", true},
		{"bad", false},
		{"unknown", false}, // zero value from inner map
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, pb.PreferReadAt(tc.bucket), "bucket=%q", tc.bucket)
	}
}

func TestPackedBackend_PreferWriteAtForwardsToInner(t *testing.T) {
	inner := &prefRecordingInner{
		writeAt: map[string]bool{"good": true, "bad": false},
	}
	dir := t.TempDir()
	pb, err := NewPackedBackendWithOptions(inner, dir, 1024, PackedBackendOptions{})
	require.NoError(t, err)
	defer pb.Close()

	cases := []struct {
		bucket string
		want   bool
	}{
		{"good", true},
		{"bad", false},
		{"unknown", false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, pb.PreferWriteAt(tc.bucket), "bucket=%q", tc.bucket)
	}
}

func TestPackedBackend_PreferReturnsFalseWhenInnerHasNoCapability(t *testing.T) {
	inner := &silentInner{}
	dir := t.TempDir()
	pb, err := NewPackedBackendWithOptions(inner, dir, 1024, PackedBackendOptions{})
	require.NoError(t, err)
	defer pb.Close()

	assert.False(t, pb.PreferReadAt("any"), "inner without PreferReadAt should yield false")
	assert.False(t, pb.PreferWriteAt("any"), "inner without PreferWriteAt should yield false")
}
