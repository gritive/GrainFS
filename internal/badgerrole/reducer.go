package badgerrole

import (
	"fmt"
	"sort"
)

func ReduceStartupDecisions(reg Registry, decisions []Decision) StartupResult {
	ordered := append([]Decision(nil), decisions...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Role == ordered[j].Role {
			return ordered[i].GroupID < ordered[j].GroupID
		}
		return ordered[i].Role < ordered[j].Role
	})

	result := StartupResult{
		Mode:      StartupModeWritable,
		Decisions: ordered,
	}

	for _, decision := range ordered {
		if decision.Status == DecisionOK {
			continue
		}

		spec, ok := reg.Get(decision.Role)
		if !ok || decision.Status == DecisionUnknownRole {
			result.Mode = StartupModeBlocked
			result.BlockedReasons = append(result.BlockedReasons, reasonFor(decision))
			continue
		}

		if decision.Status == DecisionQuarantineRequired || decision.Action == RecoveryActionNeedsQuarantine {
			result.Mode = StartupModeBlocked
			result.BlockedReasons = append(result.BlockedReasons, reasonFor(decision))
			continue
		}

		switch spec.Criticality {
		case CriticalityRequired:
			result.Mode = StartupModeBlocked
			result.BlockedReasons = append(result.BlockedReasons, reasonFor(decision))
		case CriticalityReadOnly:
			if result.Mode != StartupModeBlocked {
				result.Mode = StartupModeReadOnly
			}
			result.ReadOnlyReasons = append(result.ReadOnlyReasons, reasonFor(decision))
		case CriticalityOptional:
			if spec.FeatureFlag != "" {
				result.DisabledFeatures = append(result.DisabledFeatures, spec.FeatureFlag)
			}
		default:
			result.Mode = StartupModeBlocked
			result.BlockedReasons = append(result.BlockedReasons, fmt.Sprintf("%s: unsupported criticality %q", decision.Role, spec.Criticality))
		}
	}

	sort.Strings(result.BlockedReasons)
	sort.Strings(result.ReadOnlyReasons)
	sort.Strings(result.DisabledFeatures)
	return result
}

func reasonFor(decision Decision) string {
	if decision.GroupID != "" {
		if decision.Reason != "" {
			return fmt.Sprintf("%s[%s]: %s", decision.Role, decision.GroupID, decision.Reason)
		}
		return fmt.Sprintf("%s[%s]: %s", decision.Role, decision.GroupID, decision.Status)
	}
	if decision.Reason != "" {
		return fmt.Sprintf("%s: %s", decision.Role, decision.Reason)
	}
	return fmt.Sprintf("%s: %s", decision.Role, decision.Status)
}
