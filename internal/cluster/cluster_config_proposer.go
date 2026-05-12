package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// ClusterConfigProposeFunc is the cluster-side propose function signature:
// takes a MetaCmd type and an already-encoded typed (inner) payload, returns
// nil on successful raft commit + local apply. Mirrors iam.ProposeFunc.
type ClusterConfigProposeFunc func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error

// ClusterConfigProposer is the production raft-side write path for
// cluster-config PATCH. It encodes the inner FlatBuffers payload and dispatches
// it through Propose, which transparently forwards to the leader when this
// node is a follower (see MetaRaft.proposeOrForward).
//
// Tests substitute a fake that calls fsm.ApplyClusterConfigPatchForTest
// directly, bypassing raft.
type ClusterConfigProposer struct {
	Propose ClusterConfigProposeFunc
}

// ProposeClusterConfigPatch encodes p and submits it via the configured
// Propose closure. Errors propagate raw — adminapi maps them to HTTP codes.
func (m *ClusterConfigProposer) ProposeClusterConfigPatch(p ClusterConfigPatch) error {
	payload, err := EncodeClusterConfigPatchInner(p)
	if err != nil {
		return err
	}
	return m.Propose(context.Background(), clusterpb.MetaCmdTypeClusterConfigPatch, payload)
}
