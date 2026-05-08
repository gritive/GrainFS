package cluster

// ClusterStatus is a single-shot snapshot of cluster topology and liveness used
// by the dashboard / status endpoints. All fields are optional; nil/empty
// signals the underlying source did not surface that capability for this
// snapshot. Producers SHOULD return slices and maps owned by the snapshot
// (callers may keep them across mutations of the source).
//
// Returned by ClusterInfo.Snapshot(); replaces the seven type-asserted
// mini-interfaces (clusterPeerSnapshot / clusterPeerAddrs / clusterPeerStates
// / clusterBucketAssignments / clusterShardGroups) that previously lived in
// internal/server. Per-bucket and per-key reports (ObjectIndexSummary,
// PlacementReport) take parameters and remain dedicated methods on
// ClusterInfo, not snapshot fields.
type ClusterStatus struct {
	PeerSnapshot      []PeerLivenessRow
	PeerAddrs         map[string]string
	PeerStates        map[string]string
	BucketAssignments map[string]string
	ShardGroups       []ShardGroupEntry
}
