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

// NewMetaTransportMux wires meta-Raft to ride the shared persistent mux carrier
// (the same one group-raft uses) instead of connection-per-RPC transport.Call.
// Call did a fresh TLS handshake on every AppendEntries/heartbeat — the cluster
// PUT throughput bottleneck. When groupMux is non-nil it is stored as the mux
// fast path AND the meta node is registered on it so inbound meta RPCs over the
// "__meta__" group route correctly; the legacy Call handler stays registered as
// the fallback. groupMux==nil keeps the Call-only behavior (tests).
func NewMetaTransportMux(tr clusterRPCTransport, node RaftNode, groupMux *raft.GroupRaftMux) *MetaTransportMux {
	mt := NewRaftV2MetaTransport(tr, node)
	if groupMux != nil {
		mt.mux = groupMux
		// Receiver side: route inbound "__meta__" mux RPCs to this node. RaftNode
		// satisfies raft.RaftV2Handler (HandleRequestVote/HandleAppendEntries), so
		// the cluster-layer node registers directly — no concrete *raft.Node needed.
		groupMux.RegisterMetaNode(node)
	}
	return mt
}
