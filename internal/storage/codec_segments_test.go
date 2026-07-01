package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectCodecRoundTripWithSegments(t *testing.T) {
	orig := &Object{
		Key: "k", Size: 30 * 1024 * 1024, ETag: "cafef00d-3",
		Segments: []SegmentRef{
			{BlobID: "b1", Size: 10 << 20, Checksum: bytes.Repeat([]byte{0xaa}, ChecksumLen), PlacementGroupID: "pg1", ShardSize: 1 << 20},
			{BlobID: "b2", Size: 10 << 20, Checksum: bytes.Repeat([]byte{0xbb}, ChecksumLen), PlacementGroupID: "pg1", ShardSize: 1 << 20},
			{BlobID: "b3", Size: 10 << 20, Checksum: bytes.Repeat([]byte{0xcc}, ChecksumLen), PlacementGroupID: "pg2", ShardSize: 1 << 20},
		},
		IsAppendable: true,
	}
	data, err := marshalObject(orig)
	require.NoError(t, err, "marshal")
	got, err := unmarshalObject(data)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, orig.Segments, got.Segments)
	require.Equal(t, orig.IsAppendable, got.IsAppendable)
}

func TestObjectCodecRoundTripWithCoalesced(t *testing.T) {
	orig := &Object{
		Key: "k", Size: 30 * 1024 * 1024, ETag: "cafef00d-3",
		Coalesced: []CoalescedRef{
			{
				CoalescedID: "c1",
				Size:        20 << 20,
				ETag:        "etag-c1",
				ShardKey:    "k/coalesced/c1",
				ECData:      2,
				ECParity:    1,
				StripeBytes: 1 << 20,
				NodeIDs:     []string{"n1", "n2", "n3"},
			},
			{CoalescedID: "c2", Size: 10 << 20, ETag: "etag-c2"},
		},
		IsAppendable: true,
	}
	data, err := marshalObject(orig)
	require.NoError(t, err, "marshal")
	got, err := unmarshalObject(data)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, orig.Coalesced, got.Coalesced)
	require.Equal(t, orig.IsAppendable, got.IsAppendable)
}

func TestObjectCodecLegacyHasNoSegments(t *testing.T) {
	orig := &Object{Key: "legacy", Size: 100, ETag: "x"}
	data, err := marshalObject(orig)
	require.NoError(t, err, "marshal")
	got, err := unmarshalObject(data)
	require.NoError(t, err, "unmarshal")
	require.Nil(t, got.Segments)
	require.False(t, got.IsAppendable, "expected IsAppendable false for legacy")
}
