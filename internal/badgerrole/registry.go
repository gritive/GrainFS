package badgerrole

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

type Registry struct {
	specs map[Role]RoleSpec
}

func DefaultRegistry() Registry {
	return Registry{specs: map[Role]RoleSpec{
		RoleMeta: {
			Role: RoleMeta, DisplayName: "metadata", RelativePath: "meta",
			OptionsKind: OptionsSmall, Criticality: CriticalityRequired,
			SourceContract: SourceContractBadgerSelf, ReadOnlyEligible: false,
			QuarantineEligible: false, MaxProbeConcurrency: 1,
		},
		RoleMetaRaftLog: {
			Role: RoleMetaRaftLog, DisplayName: "meta raft log", RelativePath: "raft",
			OptionsKind: OptionsRaftLog, Criticality: CriticalityRequired,
			SourceContract: SourceContractRaftLog, ReadOnlyEligible: false,
			QuarantineEligible: false, MaxProbeConcurrency: 1,
		},
		RoleSharedRaftLog: {
			Role: RoleSharedRaftLog, DisplayName: "shared raft log", RelativePath: "shared-raft-log",
			OptionsKind: OptionsRaftLog, Criticality: CriticalityRequired,
			SourceContract: SourceContractRaftLog, ReadOnlyEligible: false,
			QuarantineEligible: false, MaxProbeConcurrency: 1,
		},
		RoleSharedFSM: {
			Role: RoleSharedFSM, DisplayName: "shared FSM state", RelativePath: "shared-fsm",
			OptionsKind: OptionsSmall, Criticality: CriticalityRequired,
			SourceContract: SourceContractBadgerSelf, ReadOnlyEligible: false,
			QuarantineEligible: false, MaxProbeConcurrency: 1,
		},
		RoleGroupState: {
			Role: RoleGroupState, DisplayName: "data group state", RelativePath: "groups/{group}/badger",
			OptionsKind: OptionsSmall, Criticality: CriticalityReadOnly,
			SourceContract: SourceContractBadgerSelf, ReadOnlyEligible: true,
			QuarantineEligible: false, MaxProbeConcurrency: 2,
		},
		RoleGroupRaftLog: {
			Role: RoleGroupRaftLog, DisplayName: "data group raft log", RelativePath: "groups/{group}/raft",
			OptionsKind: OptionsRaftLog, Criticality: CriticalityReadOnly,
			SourceContract: SourceContractRaftLog, ReadOnlyEligible: true,
			QuarantineEligible: false, MaxProbeConcurrency: 2,
		},
		RoleReceipts: {
			Role: RoleReceipts, DisplayName: "heal receipts", RelativePath: "receipts",
			OptionsKind: OptionsSmall, Criticality: CriticalityOptional,
			SourceContract: SourceContractNone, ReadOnlyEligible: false,
			QuarantineEligible: false, FeatureFlag: "heal-receipt", MaxProbeConcurrency: 1,
		},
		RoleDedup: {
			Role: RoleDedup, DisplayName: "dedup index", RelativePath: "dedup",
			OptionsKind: OptionsSmall, Criticality: CriticalityOptional,
			SourceContract: SourceContractNone, ReadOnlyEligible: false,
			QuarantineEligible: false, FeatureFlag: "dedup", MaxProbeConcurrency: 1,
		},
		RoleVolumeCatalog: {
			Role: RoleVolumeCatalog, DisplayName: "volume catalog", RelativePath: "volume-catalog",
			OptionsKind: OptionsSmall, Criticality: CriticalityReadOnly,
			SourceContract: SourceContractOperatorMarker, ReadOnlyEligible: true,
			QuarantineEligible: false, MaxProbeConcurrency: 1,
		},
		RoleIncidentState: {
			Role: RoleIncidentState, DisplayName: "incident state", RelativePath: "incident-state",
			OptionsKind: OptionsSmall, Criticality: CriticalityOptional,
			SourceContract: SourceContractNone, ReadOnlyEligible: false,
			QuarantineEligible: false, FeatureFlag: "incident-api", MaxProbeConcurrency: 1,
		},
	}}
}

func (r Registry) All() []RoleSpec {
	out := make([]RoleSpec, 0, len(r.specs))
	for _, spec := range r.specs {
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Role < out[j].Role })
	return out
}

func (r Registry) Get(role Role) (RoleSpec, bool) {
	spec, ok := r.specs[role]
	return spec, ok
}

func (r Registry) ResolvePath(role Role, ctx PathContext) (string, error) {
	spec, ok := r.Get(role)
	if !ok {
		return "", fmt.Errorf("badger role %q: unknown role", role)
	}
	if ctx.DataDir == "" {
		return "", fmt.Errorf("badger role %s: data dir required", role)
	}
	rel := spec.RelativePath
	if strings.Contains(rel, "{group}") {
		if ctx.GroupID == "" {
			return "", fmt.Errorf("badger role %s: group id required", role)
		}
		rel = strings.ReplaceAll(rel, "{group}", ctx.GroupID)
	}
	return filepath.Join(ctx.DataDir, rel), nil
}
