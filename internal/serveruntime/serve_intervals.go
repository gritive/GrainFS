package serveruntime

import (
	"time"

	"github.com/rs/zerolog/log"
)

type intervalCheck struct {
	name     string
	val      *time.Duration
	defValue time.Duration
}

// ValidateRequiredIntervals ensures scrub and EC reshard intervals are never
// zero. These two are always-on: disabling them risks data integrity
// (scrub: silent corruption, EC reshard: N× → EC backlog).
// If any interval is zero, it is reset to its default and a warning is logged.
func ValidateRequiredIntervals(cfg *Config) {
	checks := []intervalCheck{
		{"--scrub-interval", &cfg.ScrubInterval, 24 * time.Hour},
		{"--reshard-interval", &cfg.ReshardInterval, 24 * time.Hour},
	}
	for _, c := range checks {
		if *c.val == 0 {
			log.Warn().Str("flag", c.name).Dur("default", c.defValue).
				Msg("interval cannot be zero; resetting to default — these services are always-on")
			*c.val = c.defValue
		}
	}
}
