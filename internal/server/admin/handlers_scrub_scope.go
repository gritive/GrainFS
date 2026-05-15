package admin

import (
	"strings"

	"github.com/gritive/GrainFS/internal/scrubber"
)

func parseScrubScope(raw string) (scrubber.ScrubScope, error) {
	switch strings.ToLower(raw) {
	case "", "full":
		return scrubber.ScopeFull, nil
	case "live":
		return scrubber.ScopeLive, nil
	default:
		return scrubber.ScopeFull, NewInvalid("scope must be 'full' or 'live'")
	}
}
