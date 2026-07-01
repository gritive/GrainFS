package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQuorumMetaBlobWins_MetaSeqTiebreak verifies the LWW priority order:
// ModTime > VersionID > MetaSeq. MetaSeq is the lowest-priority tiebreak and is
// only consulted when both ModTime AND VersionID are equal.
func TestQuorumMetaBlobWins_MetaSeqTiebreak(t *testing.T) {
	tests := []struct {
		name string
		modA int64
		verA string
		seqA uint64
		modB int64
		verB string
		seqB uint64
		want bool
	}{
		{
			name: "differing ModTime decides; MetaSeq ignored even when lower-ModTime side has higher MetaSeq",
			modA: 100, verA: "v1", seqA: 0,
			modB: 200, verB: "v1", seqB: 99,
			want: false, // A's lower ModTime loses despite seqA being irrelevant
		},
		{
			name: "higher ModTime wins; MetaSeq ignored",
			modA: 200, verA: "v1", seqA: 0,
			modB: 100, verB: "v9", seqB: 99,
			want: true,
		},
		{
			name: "equal ModTime, differing VersionID decides; MetaSeq ignored",
			modA: 100, verA: "v9", seqA: 0,
			modB: 100, verB: "v1", seqB: 99,
			want: true,
		},
		{
			name: "equal ModTime, lower VersionID loses; MetaSeq ignored",
			modA: 100, verA: "v1", seqA: 99,
			modB: 100, verB: "v9", seqB: 0,
			want: false,
		},
		{
			name: "equal ModTime+VersionID, higher MetaSeq wins",
			modA: 100, verA: "v1", seqA: 2,
			modB: 100, verB: "v1", seqB: 1,
			want: true,
		},
		{
			name: "equal ModTime+VersionID, lower MetaSeq loses",
			modA: 100, verA: "v1", seqA: 1,
			modB: 100, verB: "v1", seqB: 2,
			want: false,
		},
		{
			name: "equal all three: not strictly greater",
			modA: 100, verA: "v1", seqA: 5,
			modB: 100, verB: "v1", seqB: 5,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := quorumMetaBlobWins(tc.modA, tc.verA, tc.seqA, tc.modB, tc.verB, tc.seqB)
			require.Equal(t, tc.want, got)
		})
	}
}
