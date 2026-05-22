package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

func seedGroupCountForClusterSize(clusterSize int) int {
	if clusterSize < 1 {
		clusterSize = 1
	}
	seedGroups := clusterSize * 4
	if seedGroups < 8 {
		seedGroups = 8
	}
	return seedGroups
}

// SeedInitialShardGroups proposes group-0..N-1 ShardGroup entries via meta-raft.
// Solo (peers=0) and bootstrap-leader paths use this to populate the FSM with
// initial shard placement so per-group raft instantiation can begin.
//
// Group-0 always uses the full peer set; groups 1..N-1 are PickVoters-derived
// triplets so replication factor stays at 3 even on larger clusters.
func SeedInitialShardGroups(
	ctx context.Context,
	metaRaft *cluster.MetaRaft,
	selfNodeID string,
	selfAddr string,
	peers []string,
	seedGroups int,
	normalGroupVoters int,
) error {
	bootstrapCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	if err := WaitForMetaRaftLeader(bootstrapCtx, metaRaft, 15*time.Second); err != nil {
		return err
	}

	for i := 0; i < seedGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		voterCount := normalGroupVoters
		if voterCount < 1 {
			voterCount = 1
		}
		voters := SeedShardGroupVoters(selfNodeID, selfAddr, peers, metaRaft.FSM().Nodes(), groupID, voterCount)

		sgCtx, sgCancel := context.WithTimeout(bootstrapCtx, 5*time.Second)
		err := metaRaft.ProposeShardGroup(sgCtx, cluster.ShardGroupEntry{
			ID:      groupID,
			PeerIDs: voters,
		})
		sgCancel()
		if i == 0 && err != nil && metaRaft.IsLeader() {
			return fmt.Errorf("seed group-0: %w", err)
		}
		if err != nil {
			log.Debug().Str("group", groupID).Err(err).Msg("seed shard group propose failed (non-fatal)")
		}
		if err := bootstrapCtx.Err(); err != nil {
			return err
		}
	}
	return nil
}

// SeedShardGroupVoters returns the voter / placement-slot list for a
// freshly-seeded shard group. group-0 receives the full cluster peer set so
// the legacy single-backend path keeps working; groups 1..N-1 are sampled via
// PickVoters.
//
// Single-node multi-slot: when only one cluster peer exists (single-node
// deployment) and replicationFactor > 1 (multi-drive EC width), the single
// peer is replicated to fill the slot list. The EC pipeline then produces
// replicationFactor shards and ShardService routes shardIdx across the local
// drives, while instantiateLocalGroup filters duplicated self entries so the
// per-group raft still starts as a single-voter quorum.
func SeedShardGroupVoters(
	selfNodeID string,
	selfAddr string,
	peers []string,
	nodes []cluster.MetaNodeEntry,
	groupID string,
	replicationFactor int,
) []string {
	clusterPeers := seedShardGroupPeerIDs(selfNodeID, selfAddr, peers, nodes)
	if len(clusterPeers) == 1 && replicationFactor > 1 {
		out := make([]string, replicationFactor)
		for i := range out {
			out[i] = clusterPeers[0]
		}
		return out
	}
	if groupID == "group-0" {
		return clusterPeers
	}
	return cluster.PickVoters(groupID, clusterPeers, replicationFactor)
}

func MissingSeedShardGroups(
	selfNodeID string,
	selfAddr string,
	nodes []cluster.MetaNodeEntry,
	existing []cluster.ShardGroupEntry,
	replicationFactor int,
) []cluster.ShardGroupEntry {
	clusterSize := len(nodes)
	if clusterSize < 1 {
		clusterSize = 1
	}
	want := seedGroupCountForClusterSize(clusterSize)
	seen := make(map[string]bool, len(existing))
	for _, group := range existing {
		seen[group.ID] = true
	}
	peers := seedShardGroupPeerAddrsFromNodes(selfNodeID, selfAddr, nodes)
	out := make([]cluster.ShardGroupEntry, 0)
	for i := 0; i < want; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		if seen[groupID] {
			continue
		}
		voters := SeedShardGroupVoters(selfNodeID, selfAddr, peers, nodes, groupID, replicationFactor)
		out = append(out, cluster.ShardGroupEntry{ID: groupID, PeerIDs: voters})
	}
	return out
}

func seedShardGroupPeerAddrsFromNodes(selfNodeID string, selfAddr string, nodes []cluster.MetaNodeEntry) []string {
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == "" && node.Address == "" {
			continue
		}
		if node.ID == selfNodeID || node.Address == selfAddr {
			continue
		}
		if node.Address != "" {
			out = append(out, node.Address)
			continue
		}
		out = append(out, node.ID)
	}
	return out
}

// seedShardGroupPeerIDs converts a list of dialable peer addresses into
// stable node IDs by consulting the meta-FSM's address book. Self is
// always first. Static --peers clusters that bootstrap before any meta
// catalog entries exist may yield raw addresses as legacy aliases — the
// address-book seam fixes those once the remote nodes register.
func seedShardGroupPeerIDs(selfNodeID string, selfAddr string, peers []string, nodes []cluster.MetaNodeEntry) []string {
	byAddress := make(map[string]string, len(nodes)+1)
	if selfAddr != "" && selfNodeID != "" {
		byAddress[selfAddr] = selfNodeID
	}
	for _, node := range nodes {
		if node.Address != "" && node.ID != "" {
			byAddress[node.Address] = node.ID
		}
	}

	out := make([]string, 0, 1+len(peers))
	if selfNodeID != "" {
		out = append(out, selfNodeID)
	} else {
		out = append(out, selfAddr)
	}
	for _, peer := range peers {
		if id, ok := byAddress[peer]; ok {
			out = append(out, id)
			continue
		}
		out = append(out, peer)
	}
	return out
}
