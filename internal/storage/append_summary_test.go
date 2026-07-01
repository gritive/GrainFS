package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// The append-summary + append-segment wire codecs are LIVE production
// serialization: cluster's append side-record path (internal/cluster/
// append_side_record.go) encodes/decodes with these. Pin the round-trip so the
// contract cannot silently drift. (These tests replace the coverage that lived
// in the deleted LocalBackend append_side_record_test.go.)

func TestAppendSummary_EncodeDecodeRoundTrip(t *testing.T) {
	cases := map[string]AppendSummary{
		// 16-byte form: no compacted prefix / etag state.
		"minimal": {Size: 1 << 30, SegmentCount: 7},
		// 24-byte form: compacted prefix, no etag state.
		"compacted": {Size: 42, SegmentCount: 3, CompactedPrefixCount: 2},
		// 28-byte form: etag state, no compacted prefix.
		"etag_state": {Size: 99, SegmentCount: 5, ETagPartCount: 4, ETagDigestState: bytes.Repeat([]byte{0xAB}, 16)},
		// 36-byte form: both.
		"full": {Size: 1 << 40, SegmentCount: 11, CompactedPrefixCount: 6, ETagPartCount: 10, ETagDigestState: bytes.Repeat([]byte{0xCD}, 24)},
	}
	for name, orig := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := DecodeAppendSummary(EncodeAppendSummary(orig))
			require.NoError(t, err)
			require.Equal(t, orig, got)
		})
	}
}

func TestAppendSegment_EncodeDecodeRoundTrip(t *testing.T) {
	// StoredSize is intentionally 0: the append-segment wire codec does not
	// carry it (pre-existing; tracked as a LocalBackend-removal follow-up in
	// TODOS, same family as the SegmentRef stored_size codec parity fix). All
	// other fields must round-trip exactly.
	orig := SegmentRef{
		BlobID:           "blob-01",
		Size:             16 << 20,
		Checksum:         bytes.Repeat([]byte{0x0C}, 16),
		PlacementGroupID: "pg-a",
		ShardSize:        1 << 20,
		ECData:           4,
		ECParity:         2,
		StripeBytes:      64 << 10,
		NodeIDs:          []string{"n1", "n2", "n3"},
	}
	got, err := DecodeAppendSegment(EncodeAppendSegment(orig))
	require.NoError(t, err)
	require.Equal(t, orig, got)
}
