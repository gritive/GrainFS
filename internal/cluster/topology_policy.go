package cluster

import (
	"errors"
	"fmt"
	"strings"
)

type ProfileComparison int

const (
	ProfileUnknown ProfileComparison = iota
	ProfileLower
	ProfileEqual
	ProfileHigher
)

type LayoutState string

const (
	LayoutCurrent          LayoutState = "current"
	LayoutPendingUpgrade   LayoutState = "pending-upgrade"
	LayoutDowngradeSkipped LayoutState = "downgrade-skipped"
	LayoutRepairNeeded     LayoutState = "repair-needed"
	LayoutUnknown          LayoutState = "unknown"
)

type RepairSignals struct {
	KnownMissingNodes map[string]bool
}

var ErrPlacementTargetsUnavailable = errors.New("insufficient placement targets")

type ErrInsufficientPlacementTargets struct {
	Operation     string
	GroupID       string
	Desired       ECConfig
	Configured    []string
	Unavailable   []string
	FailureReason string
}

func (e *ErrInsufficientPlacementTargets) Error() string {
	return fmt.Sprintf("%s: group %s requires EC %d+%d across [%s], unavailable [%s]: %s",
		e.Operation,
		e.GroupID,
		e.Desired.DataShards,
		e.Desired.ParityShards,
		strings.Join(e.Configured, ","),
		strings.Join(e.Unavailable, ","),
		e.FailureReason,
	)
}

func (e *ErrInsufficientPlacementTargets) Unwrap() error {
	return ErrPlacementTargetsUnavailable
}

var zeroConfigProfiles = []ECConfig{
	{DataShards: 1, ParityShards: 0},
	{DataShards: 1, ParityShards: 1},
	{DataShards: 2, ParityShards: 1},
	{DataShards: 2, ParityShards: 2},
	{DataShards: 3, ParityShards: 2},
	{DataShards: 4, ParityShards: 2},
	{DataShards: 5, ParityShards: 2},
	{DataShards: 6, ParityShards: 2},
}

func DesiredECConfigForGroup(group ShardGroupEntry) ECConfig {
	return AutoECConfigForClusterSize(len(group.PeerIDs))
}

func CompareECProfile(actual, desired ECConfig) ProfileComparison {
	actualIdx, actualOK := zeroConfigProfileIndex(actual)
	desiredIdx, desiredOK := zeroConfigProfileIndex(desired)
	if !actualOK || !desiredOK {
		return ProfileUnknown
	}
	switch {
	case actualIdx < desiredIdx:
		return ProfileLower
	case actualIdx > desiredIdx:
		return ProfileHigher
	default:
		return ProfileEqual
	}
}

func ClassifyObjectLayout(entry ObjectIndexEntry, group ShardGroupEntry, signals RepairSignals) LayoutState {
	if group.ID == "" || entry.PlacementGroupID == "" || entry.PlacementGroupID != group.ID {
		return LayoutUnknown
	}
	if len(entry.NodeIDs) == 0 {
		return LayoutRepairNeeded
	}
	actual := ECConfig{DataShards: int(entry.ECData), ParityShards: int(entry.ECParity)}
	desired := DesiredECConfigForGroup(group)
	comparison := CompareECProfile(actual, desired)
	if comparison == ProfileHigher {
		return LayoutDowngradeSkipped
	}
	groupNodes := make(map[string]bool, len(group.PeerIDs))
	for _, id := range group.PeerIDs {
		groupNodes[id] = true
	}
	for _, id := range entry.NodeIDs {
		if signals.KnownMissingNodes[id] || !groupNodes[id] {
			return LayoutRepairNeeded
		}
	}
	switch comparison {
	case ProfileLower:
		return LayoutPendingUpgrade
	case ProfileEqual:
		return LayoutCurrent
	default:
		return LayoutUnknown
	}
}

func zeroConfigProfileIndex(cfg ECConfig) (int, bool) {
	for i, candidate := range zeroConfigProfiles {
		if candidate.DataShards == cfg.DataShards && candidate.ParityShards == cfg.ParityShards {
			return i, true
		}
	}
	return 0, false
}
