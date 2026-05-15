package server

import (
	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

func buildClusterStatus(info ClusterInfo, degraded bool, bucket string) adminapi.Status {
	status := adminapi.Status{
		Mode:      "local",
		Degraded:  degraded,
		DownNodes: []string{},
	}
	if info == nil {
		return status
	}

	status.Mode = "cluster"
	status.NodeID = info.NodeID()
	status.State = info.State()
	status.Term = info.Term()
	status.LeaderID = info.LeaderID()

	snap := info.Snapshot()
	if snap.PeerSnapshot != nil {
		status.PeerSnapshot = cluster.PeerLivenessRowsToWire(snap.PeerSnapshot)
		status.Peers = legacyPeersFromSnapshot(snap.PeerSnapshot)
		status.PeerAddrs = legacyPeerAddrsFromSnapshot(snap.PeerSnapshot)
		status.PeerStates = legacyPeerStatesFromSnapshot(snap.PeerSnapshot)
		status.DownNodes = legacyDownNodesFromSnapshot(snap.PeerSnapshot)
	} else {
		status.Peers = info.Peers()
		if snap.PeerAddrs != nil {
			status.PeerAddrs = snap.PeerAddrs
		}
		if snap.PeerStates != nil {
			status.PeerStates = snap.PeerStates
		}
		status.DownNodes = legacyDownNodesFromPeerLists(info.Peers(), info.LivePeers())
	}
	if snap.BucketAssignments != nil {
		status.BucketAssignments = snap.BucketAssignments
	}
	if snap.ShardGroups != nil {
		status.ShardGroups = clusterStatusShardGroups(snap.ShardGroups)
	}
	summary := info.ObjectIndexSummary(bucket)
	status.ObjectIndexSummary = &adminapi.ObjectIndexSummary{
		Bucket:               summary.Bucket,
		PlacementGroupCounts: summary.PlacementGroupCounts,
	}
	return status
}

func legacyDownNodesFromPeerLists(peers, livePeers []string) []string {
	liveSet := make(map[string]struct{}, len(livePeers))
	for _, p := range livePeers {
		liveSet[p] = struct{}{}
	}
	downNodes := []string{}
	for _, p := range peers {
		if _, ok := liveSet[p]; !ok {
			downNodes = append(downNodes, p)
		}
	}
	return downNodes
}
