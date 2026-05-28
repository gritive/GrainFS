package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

func TestRestore_DenylistSurvivesSnapshot(t *testing.T) {
	src := NewMetaFSM()
	wireSnapshotKEK(t, src)
	s := spki(44)
	require.NoError(t, src.Peers().registerMember("node-revoked", s, "127.0.0.1:7044", true, 0))
	payload, err := encodeRevokePeerCmd("node-revoked")
	require.NoError(t, err)
	require.NoError(t, src.applyRevokePeer(payload))

	blob, err := src.Snapshot()
	require.NoError(t, err)
	dst := NewMetaFSM()
	wireSnapshotKEK(t, dst)
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, blob))

	require.True(t, dst.Peers().isDenylisted(s))
	require.ErrorIs(t, dst.Peers().registerMember("node-revoked", s, "127.0.0.1:7044", true, 0), errSPKIDenylisted)
}
