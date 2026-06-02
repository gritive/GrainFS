package cluster

import (
	"github.com/gritive/GrainFS/internal/raft"
)

// MetaTransportMux is the cluster-transport-backed MetaTransport. As of M6.2 step 3 it
// dispatches to the raft v2 bridge (RaftV2MetaTransport, raftv2_meta.go).
// PR 30b drops the type alias and renames the v2 file.
type MetaTransportMux = RaftV2MetaTransport

// NewMetaTransport constructs the v2 meta-Raft transport. The node
// parameter is the cluster.RaftNode interface; v1's *raft.Node satisfies it
// via the compile-time check at raftnode.go:148, so existing callers that
// pass metaRaft.Node() (still typed *raft.Node until M6.3 flips meta_raft.go)
// continue to compile unchanged.
func NewMetaTransport(tr clusterRPCTransport, node RaftNode) *MetaTransportMux {
	return NewRaftV2MetaTransport(tr, node)
}

// NewMetaTransportMux keeps the mux-aware constructor signature for boot
// wiring compat. Meta-Raft does not participate in the per-group mux
// (StreamMetaRaft != StreamControl), so groupMux is unused on the v2 path.
// Kept here so internal/serveruntime/boot_phases_raft.go's three-arg call
// continues to compile; PR 30b can clean up the signature once boot is
// audited.
func NewMetaTransportMux(tr clusterRPCTransport, node RaftNode, groupMux *raft.GroupRaftMux) *MetaTransportMux {
	_ = groupMux
	return NewRaftV2MetaTransport(tr, node)
}
