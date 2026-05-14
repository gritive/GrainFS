package serveruntime

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/incident"
)

func recordBadgerStartupDecision(state *bootState, decision badgerrole.Decision) {
	if decision.Path == "" {
		if path, err := state.roleRegistry.ResolvePath(decision.Role, badgerrole.PathContext{DataDir: state.cfg.DataDir, GroupID: decision.GroupID}); err == nil {
			decision.Path = path
		}
	}
	state.startupDecisions = append(state.startupDecisions, decision)
	if decision.Status == badgerrole.DecisionOK || state.recoveryJournal == nil {
		return
	}
	startupMode := badgerrole.ReduceStartupDecisions(state.roleRegistry, state.startupDecisions).Mode
	if _, err := state.recoveryJournal.Record(decision, startupMode); err != nil {
		log.Warn().
			Err(err).
			Str("role", string(decision.Role)).
			Str("status", string(decision.Status)).
			Msg("badger recovery journal write failed")
	}
}

func importBadgerRecoveryJournal(ctx context.Context, store incident.StateStore, dataDir string) (int, error) {
	entries, err := badgerrole.PendingJournalEntries(dataDir)
	if err != nil {
		return 0, err
	}
	imported := 0
	for _, entry := range entries {
		if entry.IncidentID == "" {
			return imported, fmt.Errorf("badger recovery journal: entry %s missing incident id", entry.ID)
		}
		_, ok, err := store.Get(ctx, entry.IncidentID)
		if err != nil {
			return imported, err
		}
		if ok {
			if err := badgerrole.MarkJournalEntryImported(dataDir, entry.ID); err != nil {
				return imported, err
			}
			continue
		}
		recorder := incident.NewRecorder(store, incident.NewReducer())
		if err := recorder.Record(ctx, []incident.Fact{journalEntryFact(entry)}); err != nil {
			return imported, err
		}
		if err := badgerrole.MarkJournalEntryImported(dataDir, entry.ID); err != nil {
			return imported, err
		}
		imported++
	}
	return imported, nil
}

func journalEntryFact(entry badgerrole.JournalEntry) incident.Fact {
	decision := entry.Decision
	return incident.Fact{
		CorrelationID: entry.IncidentID,
		Type:          incident.FactObserved,
		Cause:         journalCause(decision.Status),
		Action:        journalAction(decision.Action),
		Scope: incident.Scope{
			Kind:       incident.ScopeBadgerRole,
			NodeID:     entry.NodeID,
			BadgerRole: string(decision.Role),
			Path:       decision.Path,
		},
		ErrorCode: string(decision.Status),
		Message:   journalMessage(entry),
		At:        entry.ObservedAt,
	}
}

func journalCause(status badgerrole.DecisionStatus) incident.Cause {
	switch status {
	case badgerrole.DecisionWritableProbeFailed, badgerrole.DecisionReadOnlyProbeFailed:
		return incident.CauseBadgerPreflightFailed
	default:
		return incident.CauseBadgerOpenFailed
	}
}

func journalAction(action badgerrole.RecoveryAction) incident.Action {
	switch action {
	case badgerrole.RecoveryActionStartReadOnly:
		return incident.ActionStartReadOnly
	case badgerrole.RecoveryActionDisableFeature:
		return incident.ActionDisableFeature
	default:
		return incident.ActionBlockStartup
	}
}

func journalMessage(entry badgerrole.JournalEntry) string {
	decision := entry.Decision
	if decision.Reason != "" {
		return fmt.Sprintf("%s %s: %s", decision.Role, decision.Status, decision.Reason)
	}
	return fmt.Sprintf("%s %s", decision.Role, decision.Status)
}
