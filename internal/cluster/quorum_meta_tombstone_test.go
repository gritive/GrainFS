package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPutObjectMetaCmd_IsHardDeleted_RoundTrip proves the per-version blob codec
// carries the hard-delete tombstone flag (the C2 schema field). A tombstone is a
// per-version blob with IsHardDeleted=true that replaces the data blob it deletes.
func TestPutObjectMetaCmd_IsHardDeleted_RoundTrip(t *testing.T) {
	in := PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v1", IsHardDeleted: true}
	blob, err := encodePutObjectMetaCmd(in)
	require.NoError(t, err)
	got, err := decodePutObjectMetaCmd(blob)
	require.NoError(t, err)
	require.True(t, got.IsHardDeleted, "IsHardDeleted must round-trip true")

	// Default (old blob / data blob) decodes false.
	dataBlob, err := encodePutObjectMetaCmd(PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v1"})
	require.NoError(t, err)
	gotData, err := decodePutObjectMetaCmd(dataBlob)
	require.NoError(t, err)
	require.False(t, gotData.IsHardDeleted, "absent flag decodes to false (backward-compatible)")
}

// TestQuorumMetaCmdWins_TombstoneBeatsDataOnTie proves the LWW comparator gives a
// hard-delete tombstone top priority on an otherwise-equal (ModTime,VID,MetaSeq)
// tie, so a hard-deleted version can never lose to the stale data blob it replaced
// (closes the relocation-re-write equal-MetaSeq window). Normal LWW order is
// unchanged for non-tombstone blobs.
func TestQuorumMetaCmdWins_TombstoneBeatsDataOnTie(t *testing.T) {
	data := PutObjectMetaCmd{ModTime: 100, VersionID: "v1", MetaSeq: 5}
	tomb := PutObjectMetaCmd{ModTime: 100, VersionID: "v1", MetaSeq: 5, IsHardDeleted: true}

	require.True(t, quorumMetaCmdWins(tomb, data), "tombstone wins the full tie")
	require.False(t, quorumMetaCmdWins(data, tomb), "data must not beat the tombstone on a tie")
	require.False(t, quorumMetaCmdWins(tomb, tomb), "identical tombstones: not strictly greater")

	// Normal LWW precedence unaffected by the tombstone tiebreak.
	require.True(t, quorumMetaCmdWins(
		PutObjectMetaCmd{ModTime: 200, VersionID: "v1"},
		PutObjectMetaCmd{ModTime: 100, VersionID: "v9", IsHardDeleted: true}),
		"higher ModTime wins even over a tombstone")
	require.True(t, quorumMetaCmdWins(
		PutObjectMetaCmd{ModTime: 100, VersionID: "v9", MetaSeq: 7},
		PutObjectMetaCmd{ModTime: 100, VersionID: "v1", MetaSeq: 1}),
		"higher VersionID wins on equal ModTime")
}
