package serveruntime

import (
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
)

// OptionalRoleDisabled inspects a single Badger role open decision and
// reports whether the optional feature it gates can be skipped (writable
// startup mode + the role is on the disabled-features list). Returns
// (feature, true) when the caller should degrade gracefully, ("", false)
// when the failure is unrecoverable.
func OptionalRoleDisabled(reg badgerrole.Registry, decision badgerrole.Decision) (string, bool) {
	result := badgerrole.ReduceStartupDecisions(reg, []badgerrole.Decision{decision})
	if result.Mode != badgerrole.StartupModeWritable || len(result.DisabledFeatures) == 0 {
		return "", false
	}
	return result.DisabledFeatures[0], true
}

// LogOptionalRoleDisabled emits the standard warn log when an optional role
// has been gracefully disabled. Centralised so log shape stays uniform
// across all optional roles (incident, dedup, receipt).
func LogOptionalRoleDisabled(role badgerrole.Role, feature string, err error) {
	log.Warn().
		Str("role", string(role)).
		Str("feature", feature).
		Err(err).
		Msg("optional badger role disabled after open failure")
}
