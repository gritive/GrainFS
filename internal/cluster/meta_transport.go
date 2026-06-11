package cluster

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
