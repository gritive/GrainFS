package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// v2GroupMuxCluster groups the raft nodes under test so verifyClusterReplicates
// can drive election + replication transport-agnostically (over HTTP Call now
// that the raft mux subsystem is removed). Shared by the group-raft-over-HTTP
// integration test.
type v2GroupMuxCluster struct {
	nodes []RaftNode
}

func (c *v2GroupMuxCluster) waitForLeader(timeout time.Duration) RaftNode {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return nil
		default:
			for _, n := range c.nodes {
				if n.IsLeader() {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func verifyClusterReplicates(t *testing.T, cluster *v2GroupMuxCluster, payload string) {
	t.Helper()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "leader required for replication test")

	type applied struct {
		idx uint64
		cmd []byte
	}
	applyCh := make([]chan applied, len(cluster.nodes))
	for i, n := range cluster.nodes {
		ch := make(chan applied, 16)
		applyCh[i] = ch
		go func(src <-chan raft.LogEntry) {
			for e := range src {
				ch <- applied{idx: e.Index, cmd: e.Command}
			}
		}(n.ApplyCh())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := leader.ProposeWait(ctx, []byte(payload))
	require.NoError(t, err)
	require.Greater(t, idx, uint64(0))

	deadline := time.After(5 * time.Second)
	for i, ch := range applyCh {
		var seen bool
		for !seen {
			select {
			case a := <-ch:
				if a.idx == idx && string(a.cmd) == payload {
					seen = true
				}
			case <-deadline:
				t.Fatalf("node[%d] did not apply index %d before deadline", i, idx)
			}
		}
	}
}
