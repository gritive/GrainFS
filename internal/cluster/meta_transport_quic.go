package cluster

import (
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// MetaTransportQUIC is the QUIC-backed MetaTransport. As of M6.2 step 3 it
// dispatches to the raft v2 bridge (RaftV2MetaQUICTransport, raftv2_meta_quic.go).
// PR 30b drops the type alias and renames the v2 file.
type MetaTransportQUIC = RaftV2MetaQUICTransport

// NewMetaTransportQUIC constructs the v2 meta-Raft QUIC transport. The node
// parameter is the cluster.RaftNode interface; v1's *raft.Node satisfies it
// via the compile-time check at raftnode.go:148, so existing callers that
// pass metaRaft.Node() (still typed *raft.Node until M6.3 flips meta_raft.go)
// continue to compile unchanged.
func NewMetaTransportQUIC(tr *transport.QUICTransport, node RaftNode) *MetaTransportQUIC {
	return NewRaftV2MetaQUICTransport(tr, node)
}

// NewMetaTransportQUICMux keeps the mux-aware constructor signature for boot
// wiring compat. Meta-Raft does not participate in the per-group mux
// (StreamMetaRaft != StreamControl), so groupMux is unused on the v2 path.
// Kept here so internal/serveruntime/boot_phases_raft.go's three-arg call
// continues to compile; PR 30b can clean up the signature once boot is
// audited.
func NewMetaTransportQUICMux(tr *transport.QUICTransport, node RaftNode, groupMux *raft.GroupRaftQUICMux) *MetaTransportQUIC {
	_ = groupMux
	return NewRaftV2MetaQUICTransport(tr, node)
}
