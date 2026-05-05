package main

import (
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
)

func optionalRoleDisabled(reg badgerrole.Registry, decision badgerrole.Decision) (string, bool) {
	result := badgerrole.ReduceStartupDecisions(reg, []badgerrole.Decision{decision})
	if result.Mode != badgerrole.StartupModeWritable || len(result.DisabledFeatures) == 0 {
		return "", false
	}
	return result.DisabledFeatures[0], true
}

func logOptionalRoleDisabled(role badgerrole.Role, feature string, err error) {
	log.Warn().
		Str("role", string(role)).
		Str("feature", feature).
		Err(err).
		Msg("optional badger role disabled after open failure")
}
