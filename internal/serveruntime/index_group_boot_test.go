package serveruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestIndexGroupCount_DefaultAndNormalize(t *testing.T) {
	require.Equal(t, 1, normalizeIndexGroupCount(0))
	require.Equal(t, 1, normalizeIndexGroupCount(-3))
	require.Equal(t, 16, normalizeIndexGroupCount(16))
}

// TestBuildObjectIndexShards_N1MetaFSM verifies the byte-identical N=1 default:
// with IndexGroupCount=1, buildObjectIndexShards returns exactly one shard whose
// Reader/Lister are the cluster's only meta-FSM object index (*cluster.MetaFSM)
// and whose Writer is the supplied ForwardingObjectIndexProposer — the same
// shard the two pre-Slice-4b façade sites built inline.
func TestBuildObjectIndexShards_N1MetaFSM(t *testing.T) {
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:  "node-a",
		RaftID:  "node-a",
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metaRaft.Close()) })

	state := &bootState{
		cfg:      Config{IndexGroupCount: 1},
		metaRaft: metaRaft,
	}

	// The Writer face for the N=1 path: a forwarding object-index proposer over
	// the meta-raft (the production N=1 Writer). The forward func is never invoked
	// in this construction-only assertion.
	indexProposer := cluster.NewForwardingObjectIndexProposer(metaRaft,
		func(ctx context.Context, command []byte) error { return nil })

	shards, err := buildObjectIndexShards(state, indexProposer)
	require.NoError(t, err)
	require.Len(t, shards, 1, "N=1 must produce exactly one (meta-FSM) shard")

	// Reader and Lister are the cluster's meta-FSM (byte-identical to the inline sites).
	require.Same(t, metaRaft.FSM(), shards[0].Reader, "N=1 Reader must be the meta-FSM")
	require.Same(t, metaRaft.FSM(), shards[0].Lister, "N=1 Lister must be the meta-FSM")
	require.Same(t, indexProposer, shards[0].Writer, "N=1 Writer must be the forwarding proposer")
}
