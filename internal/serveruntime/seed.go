package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

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
) error {
	bootstrapCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	if err := WaitForMetaRaftLeader(bootstrapCtx, metaRaft, 15*time.Second); err != nil {
		return err
	}

	const replicationFactor = 3
	for i := 0; i < seedGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		voters := SeedShardGroupVoters(selfNodeID, selfAddr, peers, metaRaft.FSM().Nodes(), groupID, replicationFactor)

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

// SeedShardGroupVoters returns the voter list for a freshly-seeded shard
// group. group-0 receives the full cluster peer set so the legacy single-
// backend path keeps working; groups 1..N-1 are sampled via PickVoters so
// replication factor stays at 3 regardless of cluster size.
func SeedShardGroupVoters(
	selfNodeID string,
	selfAddr string,
	peers []string,
	nodes []cluster.MetaNodeEntry,
	groupID string,
	replicationFactor int,
) []string {
	clusterPeers := seedShardGroupPeerIDs(selfNodeID, selfAddr, peers, nodes)
	if groupID == "group-0" {
		return clusterPeers
	}
	return cluster.PickVoters(groupID, clusterPeers, replicationFactor)
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
