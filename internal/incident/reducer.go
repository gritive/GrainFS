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
	state := IncidentState{
		ID:         first.CorrelationID,
		State:      StateObserved,
		Severity:   SeverityWarning,
		Cause:      first.Cause,
		Scope:      first.Scope,
		Proof:      Proof{Status: ProofNotRequired},
		NextAction: "Watch for automatic repair.",
		ObservedAt: first.At,
		UpdatedAt:  first.At,
	}

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
		case FactDiagnosed:
			state.State = StateDiagnosed
			state.Decision = fact.Message
		case FactActionStarted:
			state.State = StateActing
			state.Action = fact.Action
		case FactActionFailed:
			state.State = StateBlocked
			state.Action = fact.Action
			state.Severity = SeverityCritical
			state.Proof = Proof{Status: ProofNotRequired}
			state.NextAction = nextActionForFailure(fact.ErrorCode)
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
		}
	}
	if sawVerified && !sawReceipt && state.State != StateBlocked {
		state.State = StateProofUnavailable
		state.Severity = SeverityWarning
		state.Proof = Proof{Status: ProofMissing, Reason: "repair verified but no signed receipt was found"}
		state.NextAction = "Check heal-receipt signing and receipt store health."
	}
	return state, nil
}

func nextActionForFailure(code string) string {
	switch code {
	case "insufficient_survivors":
		return "Restore a peer or recover from backup before retrying repair."
	case "context_canceled":
		return "Repair was canceled; retry after the node is stable."
	default:
		return "Inspect repair logs and retry after the cause is fixed."
	}
}
