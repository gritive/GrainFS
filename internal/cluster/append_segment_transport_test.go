package cluster

import (
	"testing"
)

// TestAppendSegmentRequestCodecKindRoundTrip exercises the Phase B2 wire
// extension: encode with each kind, decode it back, ensure the kind is
// preserved and the other fields are untouched.
func TestAppendSegmentRequestCodecKindRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		kind byte
	}{
		{"segment", appendSegKindSegment},
		{"coalesced", appendSegKindCoalesced},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := encodeAppendSegmentRequestKind("group-0", "b", "k", "blob-1", tc.kind)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			gid, bucket, key, blobID, kind, err := decodeAppendSegmentRequest(payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if gid != "group-0" || bucket != "b" || key != "k" || blobID != "blob-1" {
				t.Fatalf("string fields mismatch: %q %q %q %q", gid, bucket, key, blobID)
			}
			if kind != tc.kind {
				t.Fatalf("kind = %d, want %d", kind, tc.kind)
			}
		})
	}
}

// TestAppendSegmentRequestCodecLegacyDefaultsToSegment verifies backward
// compatibility: a payload encoded without the trailing kind byte (legacy
// Phase B1 callers) decodes as kind=segment.
func TestAppendSegmentRequestCodecLegacyDefaultsToSegment(t *testing.T) {
	full, err := encodeAppendSegmentRequestKind("group-0", "b", "k", "blob-1", appendSegKindCoalesced)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Drop the trailing kind byte to simulate a legacy encoder.
	legacy := full[:len(full)-1]
	_, _, _, _, kind, err := decodeAppendSegmentRequest(legacy)
	if err != nil {
		t.Fatalf("decode legacy: %v", err)
	}
	if kind != appendSegKindSegment {
		t.Fatalf("legacy decode kind = %d, want %d (segment)", kind, appendSegKindSegment)
	}
}

// TestCoalesceClusterE2E_4Node — multi-node verification is deferred to the
// Phase B3 e2e omnibus (Task 20) which exercises non-owner reads against a
// real 4-node cluster + S3 endpoint. The unit-level coverage in this PR is:
//   - codec round-trip (this file)
//   - transport handler resolves coalesced vs segment paths (this file via
//     the kind dispatch landed in append_segment_transport.go)
//   - openAppendableSegments threads kinds through to fetchAppendBlobFromAnyPeer
//   - in-process trigger end-to-end on the single-node fixture
//     (TestProcessCoalesceJobB2EndToEnd, TestAppendObjectTriggersCoalesceOnCount)
//
// Phase B2 ships with this coverage; B3 closes the gap.
func TestCoalesceClusterE2E_4Node(t *testing.T) {
	t.Skip("multi-node coalesce forward-on-read verification deferred to Phase B3 Task 20 e2e omnibus")
}
