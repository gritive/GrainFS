package serveruntime

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvitePendingSentinel_PeerSPKIs_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := &inviteJoinState{
		inviteID:          "inv-1",
		leaderID:          "node-leader",
		seedAddr:          "10.0.0.1:9000",
		seedSPKI:          [32]byte{0x01},
		nodeID:            "node-joiner",
		raftAddr:          "10.0.0.2:9000",
		nodeSPKI:          [32]byte{0x02},
		nodeKeyKEKGen:     7,
		peerSPKIs:         [][32]byte{{0x55}, {0x66}},
		clusterKeyDropped: false,
	}
	path := filepath.Join(dir, invitePendingFile)
	require.NoError(t, writeInvitePendingSentinel(dir, path, st))

	rec, ok := readInvitePendingSentinel(dir)
	require.True(t, ok)
	require.Equal(t, st.inviteID, rec.inviteID)
	require.ElementsMatch(t, st.peerSPKIs, rec.peerSPKIs)
	require.False(t, rec.clusterKeyDropped)
}
