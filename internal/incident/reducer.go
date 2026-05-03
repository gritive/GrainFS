package incident

import (
	"errors"
	"sort"
)

var ErrNoFacts = errors.New("incident: no facts")

type Reducer struct{}

func NewReducer() Reducer { return Reducer{} }

func (Reducer) Reduce(facts []Fact) (IncidentState, error) {
	if len(facts) == 0 {
		return IncidentState{}, ErrNoFacts
	}
	sort.SliceStable(facts, func(i, j int) bool { return facts[i].At.Before(facts[j].At) })

	first := facts[0]
	state := defaultStateFor(first)

	var sawVerified bool
	var sawReceipt bool
	for _, fact := range facts {
		if fact.At.After(state.UpdatedAt) {
			state.UpdatedAt = fact.At
		}
		if state.Cause == "" && fact.Cause != "" {
			state.Cause = fact.Cause
		}
		if state.Scope.Kind == "" && fact.Scope.Kind != "" {
			state.Scope = fact.Scope
		}
		switch fact.Type {
		case FactObserved:
			state.State = StateObserved
			state.Decision = fact.Message
			if fact.Action != "" {
				state.Action = fact.Action
			}
		case FactDiagnosed:
			state.State = StateDiagnosed
			state.Decision = fact.Message
			if state.Cause == CauseFDExhaustionRisk {
				state.Action = ActionResourceWarning
				state.NextAction = "Inspect connection growth and NFS sessions on this node; raise LimitNOFILE if expected."
			}
		case FactActionStarted:
			state.State = StateActing
			state.Action = fact.Action
		case FactActionFailed:
			state.State = StateBlocked
			state.Action = fact.Action
			state.Severity = SeverityCritical
			state.Proof = Proof{Status: ProofNotRequired}
			state.NextAction = nextActionForFailure(state.Cause, fact.ErrorCode)
			state.CompletedAt = fact.At
		case FactVerified:
			sawVerified = true
			state.State = StateVerifying
			state.NextAction = "Waiting for signed repair receipt."
		case FactReceiptSigned:
			sawReceipt = true
			state.State = StateFixed
			state.Severity = SeverityInfo
			state.Proof = Proof{Status: ProofSigned, ReceiptID: fact.ReceiptID}
			state.NextAction = "No action needed."
			state.CompletedAt = fact.At
		case FactIsolated:
			state.State = StateIsolated
			state.Action = fact.Action
			state.Severity = SeverityDegraded
			state.Proof = Proof{Status: ProofNotRequired}
			state.NextAction = "Review the object, restore from a clean copy, or delete the quarantined version."
			state.CompletedAt = fact.At
		case FactResolved:
			state.State = StateFixed
			if fact.Action != "" {
				state.Action = fact.Action
			}
			if state.Action == "" && state.Cause == CauseFDExhaustionRisk {
				state.Action = ActionResourceWarning
			}
			state.Severity = SeverityInfo
			state.Proof = Proof{Status: ProofNotRequired}
			state.Decision = fact.Message
			state.NextAction = nextActionForResolved(state.Cause)
			state.CompletedAt = fact.At
		}
	}
	if sawVerified && !sawReceipt && state.State != StateBlocked && proofRequiredForVerified(state.Cause) {
		state.State = StateProofUnavailable
		state.Severity = SeverityWarning
		state.Proof = Proof{Status: ProofMissing, Reason: "repair verified but no signed receipt was found"}
		state.NextAction = "Check heal-receipt signing and receipt store health."
	}
	return state, nil
}

func defaultStateFor(first Fact) IncidentState {
	state := IncidentState{
		ID:         first.CorrelationID,
		State:      StateObserved,
		Severity:   SeverityWarning,
		Cause:      first.Cause,
		Scope:      first.Scope,
		Proof:      Proof{Status: ProofNotRequired},
		NextAction: defaultNextAction(first.Cause),
		ObservedAt: first.At,
		UpdatedAt:  first.At,
	}
	if first.Cause == CauseFDExhaustionRisk {
		state.Action = ActionResourceWarning
	}
	return state
}

func defaultNextAction(cause Cause) string {
	switch cause {
	case CauseFDExhaustionRisk:
		return "Inspect connection growth and open file usage on this node."
	default:
		return "Watch for automatic repair."
	}
}

func proofRequiredForVerified(cause Cause) bool {
	return cause == CauseMissingShard
}

func nextActionForResolved(cause Cause) string {
	switch cause {
	case CauseFDExhaustionRisk:
		return "No action needed."
	default:
		return "No action needed."
	}
}

func nextActionForFailure(cause Cause, code string) string {
	if cause == CauseFDExhaustionRisk {
		switch code {
		case "fd_critical":
			return "Raise LimitNOFILE or reduce connection/file pressure on this node before continuing normal operation."
		default:
			return "Inspect open file usage on this node and reduce the source of FD growth."
		}
	}
	switch code {
	case "insufficient_survivors":
		return "Restore a peer or recover from backup before retrying repair."
	case "context_canceled":
		return "Repair was canceled; retry after the node is stable."
	default:
		return "Inspect repair logs and retry after the cause is fixed."
	}
}
