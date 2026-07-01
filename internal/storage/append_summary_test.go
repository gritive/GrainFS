package storage

import (
	"bytes"
	"encoding/binary"
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

// AppendSummaryLogicalAppendCount is live cluster code (the append segment-count
// cap): logical count = max(CompactedPrefixCount+SegmentCount, ETagPartCount).
func TestAppendSummaryLogicalAppendCount(t *testing.T) {
	cases := []struct {
		name string
		s    AppendSummary
		want int
	}{
		{"zero", AppendSummary{}, 0},
		{"segments_only", AppendSummary{SegmentCount: 5}, 5},
		{"compacted_plus_segments", AppendSummary{CompactedPrefixCount: 3, SegmentCount: 4}, 7},
		{"etag_count_dominates", AppendSummary{CompactedPrefixCount: 1, SegmentCount: 1, ETagPartCount: 9}, 9},
		{"etag_count_not_dominant", AppendSummary{CompactedPrefixCount: 4, SegmentCount: 4, ETagPartCount: 3}, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, AppendSummaryLogicalAppendCount(tc.s))
		})
	}
}

func sampleAppendSegment() SegmentRef {
	return SegmentRef{
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
}

// encodeAppendSegmentPreStoredSize replicates the exact pre-StoredSize wire
// layout (no trailing StoredSize field). Used as an independent golden so the
// backward-compat tests exercise real old-format bytes, not new code decoding
// its own output.
func encodeAppendSegmentPreStoredSize(seg SegmentRef) []byte {
	var buf bytes.Buffer
	writeString := func(s string) {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(s)))
		buf.WriteString(s)
	}
	writeBytes := func(b []byte) {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(b)))
		buf.Write(b)
	}
	writeString(seg.BlobID)
	_ = binary.Write(&buf, binary.BigEndian, seg.Size)
	writeBytes(seg.Checksum)
	writeString(seg.PlacementGroupID)
	_ = binary.Write(&buf, binary.BigEndian, seg.ShardSize)
	_ = binary.Write(&buf, binary.BigEndian, seg.ECData)
	_ = binary.Write(&buf, binary.BigEndian, seg.ECParity)
	_ = binary.Write(&buf, binary.BigEndian, seg.StripeBytes)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(seg.NodeIDs)))
	for _, nodeID := range seg.NodeIDs {
		writeString(nodeID)
	}
	return buf.Bytes()
}

func TestAppendSegment_EncodeDecodeRoundTrip(t *testing.T) {
	// StoredSize 0 (uncompressed) round-trips, as do all other fields.
	got, err := DecodeAppendSegment(EncodeAppendSegment(sampleAppendSegment()))
	require.NoError(t, err)
	require.Equal(t, sampleAppendSegment(), got)
}

// TestAppendSegment_StoredSizeRoundTrip pins the parity fix: a compressed
// segment's StoredSize survives the wire codec (it was silently dropped before).
func TestAppendSegment_StoredSizeRoundTrip(t *testing.T) {
	orig := sampleAppendSegment()
	orig.StoredSize = 956
	got, err := DecodeAppendSegment(EncodeAppendSegment(orig))
	require.NoError(t, err)
	require.Equal(t, orig, got)
	require.Equal(t, int64(956), got.StoredSize)
}

// TestAppendSegment_StoredSizeZero_ByteIdenticalToPreChange proves the encoder
// writes no trailing field while StoredSize==0, so the wire format is unchanged
// for today's (never-compressed) append-side segments — no rolling-upgrade window.
func TestAppendSegment_StoredSizeZero_ByteIdenticalToPreChange(t *testing.T) {
	seg := sampleAppendSegment() // StoredSize == 0
	require.Equal(t, encodeAppendSegmentPreStoredSize(seg), EncodeAppendSegment(seg))
}

// TestAppendSegment_DecodeOldFormat_NoStoredSize is the critical backward-compat
// guard: a record written by the pre-StoredSize encoder (no trailing bytes) must
// decode to StoredSize==0 without error, or every pre-upgrade append-side record
// becomes unreadable (a data-availability regression).
func TestAppendSegment_DecodeOldFormat_NoStoredSize(t *testing.T) {
	seg := sampleAppendSegment()
	got, err := DecodeAppendSegment(encodeAppendSegmentPreStoredSize(seg))
	require.NoError(t, err)
	require.Equal(t, int64(0), got.StoredSize)
	require.Equal(t, seg, got)
}

// TestAppendSegment_DecodeNegativeStoredSize_Errors keeps corruption detection:
// StoredSize is a compressed-byte length, so a negative trailing value is
// unambiguous corruption and must not silently hydrate into a SegmentRef.
func TestAppendSegment_DecodeNegativeStoredSize_Errors(t *testing.T) {
	seg := sampleAppendSegment()
	buf := append([]byte(nil), encodeAppendSegmentPreStoredSize(seg)...)
	var neg int64 = -1
	var tail [8]byte
	binary.BigEndian.PutUint64(tail[:], uint64(neg))
	buf = append(buf, tail[:]...)
	_, err := DecodeAppendSegment(buf)
	require.Error(t, err)
}
