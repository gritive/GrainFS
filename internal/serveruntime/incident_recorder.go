package serveruntime

import (
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// IncidentRecorderInterfaces narrows a concrete *incident.Recorder into the
// two slim consumer interfaces used by the cluster and scrubber subsystems.
// nil input → both return values are nil so callers can pass them
// unconditionally and let downstream code treat nil as "incident recording
// disabled".
func IncidentRecorderInterfaces(rec *incident.Recorder) (cluster.IncidentRecorder, scrubber.IncidentRecorder) {
	if rec == nil {
		return nil, nil
	}
	return rec, rec
}
