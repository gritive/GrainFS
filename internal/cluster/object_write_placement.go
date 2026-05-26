package cluster

import (
	"context"
	"fmt"
)

type ObjectWritePlacementInput struct {
	Operation           string
	PlacementGroupID    string
	PlacementGroup      *ShardGroupEntry
	LiveNodes           []string
	CurrentECConfig     ECConfig
	BypassBucketCheck   bool
	ShardKey            string
	NodeStates          []ObjectWritePlacementNodeState
	WeightedHRWEnabled  bool
	BoundedLoadsEnabled bool
	PeerHealth          []PeerHealthEntry
	HasPeerHealth       bool
	SelfID              string
}

type ObjectWritePlacementNodeState struct {
	NodeID         string
	DiskAvailBytes uint64
	Hot            bool
}

type ObjectWritePlacementPlan struct {
	PlacementGroupID string
	Config           ECConfig
	NodeIDs          []string
	TopologyWrite    bool
	TopologyGroup    ShardGroupEntry
}

func PlanObjectWritePlacement(in ObjectWritePlacementInput) (ObjectWritePlacementPlan, error) {
	operation := in.Operation
	if operation == "" {
		operation = "put_object"
	}
	in.Operation = operation
	placementGroupID := in.PlacementGroupID
	if placementGroupID == "" {
		if in.BypassBucketCheck {
			return ObjectWritePlacementPlan{}, fmt.Errorf("putObjectEC: missing placement_group_id")
		}
		placementGroupID = "group-0"
	}

	liveNodes := cloneStringSlice(in.LiveNodes)
	effectiveCfg := EffectiveConfig(len(liveNodes), in.CurrentECConfig)
	if effectiveCfg.NumShards() == 0 && !in.BypassBucketCheck {
		effectiveCfg = AutoECConfigForClusterSize(len(liveNodes))
	}
	if effectiveCfg.NumShards() == 0 {
		return ObjectWritePlacementPlan{}, fmt.Errorf("putObjectEC: EC profile cannot place on %d nodes", len(liveNodes))
	}

	placement := selectECPlacementFromNodeStates(
		effectiveCfg,
		liveNodes,
		in.ShardKey,
		in.NodeStates,
		in.WeightedHRWEnabled,
		in.BoundedLoadsEnabled,
	)
	plan := ObjectWritePlacementPlan{
		PlacementGroupID: placementGroupID,
		Config:           effectiveCfg,
		NodeIDs:          placement,
	}

	if in.PlacementGroup != nil {
		group, cfg, err := objectWritePlacementTargetsForGroup(operation, *in.PlacementGroup, placementGroupID)
		if err != nil {
			return ObjectWritePlacementPlan{}, err
		}
		plan.TopologyWrite = true
		plan.TopologyGroup = group
		plan.PlacementGroupID = group.ID
		plan.Config = cfg
		plan.NodeIDs = cloneStringSlice(group.PeerIDs[:cfg.NumShards()])
	}

	if len(plan.NodeIDs) != plan.Config.NumShards() {
		return ObjectWritePlacementPlan{}, fmt.Errorf("putObjectEC: placement has %d nodes, need %d (k=%d m=%d)",
			len(plan.NodeIDs), plan.Config.NumShards(), plan.Config.DataShards, plan.Config.ParityShards)
	}
	if plan.TopologyWrite {
		if err := checkObjectWritePlacementHealth(in, plan.TopologyGroup, plan.Config, plan.NodeIDs); err != nil {
			return ObjectWritePlacementPlan{}, err
		}
	}
	return plan, nil
}

func objectWritePlacementTargetsForGroup(operation string, group ShardGroupEntry, groupID string) (ShardGroupEntry, ECConfig, error) {
	if group.ID == "" {
		group.ID = groupID
	}
	cfg := DesiredECConfigForGroup(group)
	if cfg.NumShards() == 0 {
		return ShardGroupEntry{}, ECConfig{}, &ErrInsufficientPlacementTargets{
			Operation:     operation,
			GroupID:       group.ID,
			Configured:    cloneStringSlice(group.PeerIDs),
			FailureReason: "placement group has no zero-config EC profile",
		}
	}
	if len(group.PeerIDs) < cfg.NumShards() {
		return ShardGroupEntry{}, ECConfig{}, &ErrInsufficientPlacementTargets{
			Operation:     operation,
			GroupID:       group.ID,
			Desired:       cfg,
			Configured:    cloneStringSlice(group.PeerIDs),
			Unavailable:   cloneStringSlice(group.PeerIDs),
			FailureReason: "configured placement group is narrower than desired profile",
		}
	}
	group.PeerIDs = cloneStringSlice(group.PeerIDs)
	return group, cfg, nil
}

func checkObjectWritePlacementHealth(in ObjectWritePlacementInput, group ShardGroupEntry, cfg ECConfig, placement []string) error {
	if !in.HasPeerHealth {
		return nil
	}
	health := make(map[string]bool, len(in.PeerHealth))
	for _, entry := range in.PeerHealth {
		health[entry.ID] = entry.Healthy
	}
	for _, node := range placement {
		if node == in.SelfID {
			continue
		}
		if healthy, ok := health[node]; ok && !healthy {
			return &ErrInsufficientPlacementTargets{
				Operation:     in.Operation,
				GroupID:       group.ID,
				Desired:       cfg,
				Configured:    cloneStringSlice(group.PeerIDs),
				Unavailable:   []string{node},
				FailureReason: "known unhealthy placement target",
			}
		}
	}
	return nil
}

func (b *DistributedBackend) planObjectWritePlacement(ctx context.Context, in ObjectWritePlacementInput) (ObjectWritePlacementPlan, error) {
	if in.Operation == "" {
		in.Operation = "put_object"
	}
	if in.PlacementGroupID == "" && ctx != nil {
		in.PlacementGroupID, _ = PlacementGroupFromContext(ctx)
	}
	if in.PlacementGroup == nil && ctx != nil {
		if group, ok := PlacementGroupEntryFromContext(ctx); ok {
			in.PlacementGroup = &group
		}
	}
	if in.LiveNodes == nil {
		in.LiveNodes = b.effectivePlacementNodes()
	}
	in.CurrentECConfig = b.currentECConfig()
	in.BypassBucketCheck = b.bypassBucketCheck
	if ph := b.currentPeerHealth(); ph != nil {
		in.PeerHealth = ph.Snapshot()
		in.HasPeerHealth = true
	}
	in.SelfID = b.currentSelfAddr()
	in.NodeStates = objectWritePlacementNodeStatesFromRuntime(in.LiveNodes, b.nodeStatsStore, b.bl)
	in.WeightedHRWEnabled = b.clusterCfg.WeightedHRWEnabled()
	in.BoundedLoadsEnabled = b.clusterCfg.BoundedLoadsEnabled()
	return PlanObjectWritePlacement(in)
}
