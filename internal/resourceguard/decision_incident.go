package resourceguard

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/resourcewatch"
)

// decisionIncidentSpec captures the per-resource variation in how a
// resourcewatch.Decision is turned into incident facts. The control flow — an
// Observed fact plus a Level→fact-type switch (Resolved / Diagnosed /
// Diagnosed+ActionFailed) — is identical across the FD, goroutine, and vlog
// monitors; only the fields below differ.
type decisionIncidentSpec struct {
	// correlationID is the incident correlation ID stamped on every fact
	// (e.g. "fd-<node>"); identical for all facts in one decision.
	correlationID string
	// cause is stamped on the Observed fact for every resource.
	cause incident.Cause
	// criticalCode is the ErrorCode on the Critical FactActionFailed fact.
	criticalCode string
	// diagMessage, when non-nil, overrides decision.Message for the Warn and
	// Critical (Diagnosed / ActionFailed) facts. The Observed and Resolved
	// facts always use decision.Message. vlog uses this to append the per-DB
	// breakdown; FD/goroutine leave it nil.
	diagMessage func(*resourcewatch.Decision) string
	// causeOnDerived stamps cause onto the derived (Resolved / Diagnosed /
	// ActionFailed) facts as well, not just the Observed fact. vlog does this;
	// FD/goroutine leave the derived facts' Cause at its zero value.
	causeOnDerived bool
}

// recordResourceDecision converts a resourcewatch.Decision into incident facts
// per spec and records them. It concentrates the Load-once / Level-switch /
// fact-assembly protocol that recordFDDecision, recordGoroutineDecision, and
// recordVlogDecision previously hand-copied.
func recordResourceDecision(ctx context.Context, recorder IncidentRecorder, nodeID string, decision *resourcewatch.Decision, spec decisionIncidentSpec) error {
	if recorder == nil || decision == nil {
		return nil
	}
	at := decision.Snapshot.CollectedAt
	if at.IsZero() {
		at = time.Now()
	}
	derivedCause := incident.Cause("")
	if spec.causeOnDerived {
		derivedCause = spec.cause
	}
	diagMsg := decision.Message
	if spec.diagMessage != nil {
		diagMsg = spec.diagMessage(decision)
	}
	facts := []incident.Fact{{
		CorrelationID: spec.correlationID,
		Type:          incident.FactObserved,
		Cause:         spec.cause,
		Scope:         incident.Scope{Kind: incident.ScopeNode, NodeID: nodeID},
		Message:       decision.Message,
		At:            at,
	}}
	switch decision.Level {
	case resourcewatch.LevelOK:
		facts = append(facts, incident.Fact{
			CorrelationID: spec.correlationID,
			Type:          incident.FactResolved,
			Cause:         derivedCause,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.LevelWarn:
		facts = append(facts, incident.Fact{
			CorrelationID: spec.correlationID,
			Type:          incident.FactDiagnosed,
			Cause:         derivedCause,
			Action:        incident.ActionResourceWarning,
			Message:       diagMsg,
			At:            at,
		})
	case resourcewatch.LevelCritical:
		facts = append(facts, incident.Fact{
			CorrelationID: spec.correlationID,
			Type:          incident.FactDiagnosed,
			Cause:         derivedCause,
			Action:        incident.ActionResourceWarning,
			Message:       diagMsg,
			At:            at,
		}, incident.Fact{
			CorrelationID: spec.correlationID,
			Type:          incident.FactActionFailed,
			Cause:         derivedCause,
			Action:        incident.ActionResourceWarning,
			ErrorCode:     spec.criticalCode,
			Message:       diagMsg,
			At:            at,
		})
	}
	return recorder.Record(ctx, facts)
}
