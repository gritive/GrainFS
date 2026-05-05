package main

import (
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/scrubber"
)

func incidentRecorderInterfaces(rec *incident.Recorder) (cluster.IncidentRecorder, scrubber.IncidentRecorder) {
	if rec == nil {
		return nil, nil
	}
	return rec, rec
}
