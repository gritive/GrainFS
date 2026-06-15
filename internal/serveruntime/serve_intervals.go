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

// ValidateRequiredIntervals ensures the scrub interval is never zero. Scrub is
// always-on: disabling it risks silent corruption going undetected. If the
// interval is zero, it is reset to its default and a warning is logged.
func ValidateRequiredIntervals(cfg *Config) {
	checks := []intervalCheck{
		{"--scrub-interval", &cfg.ScrubInterval, 24 * time.Hour},
	}
	for _, c := range checks {
		if *c.val == 0 {
			log.Warn().Str("flag", c.name).Dur("default", c.defValue).
				Msg("interval cannot be zero; resetting to default — these services are always-on")
			*c.val = c.defValue
		}
	}
}
