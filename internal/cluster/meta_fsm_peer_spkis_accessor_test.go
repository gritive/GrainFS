package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaFSM_PeerSPKIs_SnapshotsCurrentRegistry(t *testing.T) {
	f := NewMetaFSM()
	wireSnapshotKEK(t, f)

	a := [32]byte{0xAA}
	b := [32]byte{0xBB}

	dataA, err := encodeRegisterMemberCmd("node-A", a, "10.0.0.1:9000", false)
	require.NoError(t, err)
	require.NoError(t, f.applyRegisterMember(dataA))

	dataB, err := encodeRegisterMemberCmd("node-B", b, "10.0.0.2:9000", false)
	require.NoError(t, err)
	require.NoError(t, f.applyRegisterMember(dataB))

	got := f.PeerSPKIs()
	require.ElementsMatch(t, [][32]byte{a, b}, got)
}
