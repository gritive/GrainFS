package startuprecovery

import (
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// emitStartup builds a HealEvent for a single startup-recovery action.
// Path is stashed in Key so the dashboard can render where the cleanup
// happened without needing a richer schema.
func emitStartup(emit scrubber.Emitter, path, errCode string, outcome scrubber.HealOutcome) {
	ev := scrubber.NewEvent(scrubber.PhaseStartup, outcome)
	ev.Key = path
	ev.ErrCode = errCode
	emit.Emit(ev)
	metrics.HealEventsTotal.WithLabelValues(string(scrubber.PhaseStartup), string(outcome)).Inc()
}
