package cluster

import (
	"testing"
)

func TestSegmentMetaEntry_StoredSizeRoundTrip(t *testing.T) {
	in := []SegmentMetaEntry{{
		BlobID:           "blob-1",
		Size:             16 << 20, // plaintext
		StoredSize:       1 << 20,  // compressed
		Checksum:         []byte("0123456789abcdef"),
		PlacementGroupID: "pg-0",
		ShardSize:        262144,
		SegmentIdx:       0,
		ECData:           4,
		ECParity:         2,
	}}
	// refs round-trip
	refs := segmentMetaEntriesToRefs(in)
	if refs[0].StoredSize != 1<<20 || refs[0].Size != 16<<20 {
		t.Fatalf("ref StoredSize/Size = %d/%d", refs[0].StoredSize, refs[0].Size)
	}
	back := segmentRefsToMetaEntries(refs)
	if back[0].StoredSize != 1<<20 {
		t.Fatalf("entry StoredSize = %d", back[0].StoredSize)
	}
}
