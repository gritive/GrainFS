package server

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// PreflightBadger runs a minimal write→read→delete cycle against db to
// confirm the database is healthy enough for grainfs to take traffic.
//
// Returned errors are wrapped with an operator-friendly recovery guide so a
// fail-fast at boot points the human at the right next step instead of
// drowning them in BadgerDB internals.
//
// emit may be nil; when present, a HealEvent{Phase: startup} records the
// success/failure so the dashboard's Restart Recovery line shows the check
// ran. We do NOT count this in OrphanTmp/OrphanMultipart counters because
// it is a verification, not a cleanup.
func PreflightBadger(db *badger.DB, dbDir string, emit scrubber.Emitter) error {
	if db == nil {
		return errors.New("preflight: nil badger DB")
	}
	if emit == nil {
		emit = scrubber.NoopEmitter{}
	}

	decision := badgerrole.ProbeWritable(db, badgerrole.RoleMeta, "", dbDir)
	if decision.Status != badgerrole.DecisionOK {
		emitPreflight(emit, "write_failed", scrubber.OutcomeFailed)
		return wrapPreflight(decision.Err, dbDir, "write")
	}

	emitPreflight(emit, "ok", scrubber.OutcomeSuccess)
	return nil
}

func emitPreflight(emit scrubber.Emitter, errCode string, outcome scrubber.HealOutcome) {
	ev := scrubber.NewEvent(scrubber.PhaseStartup, outcome)
	ev.ErrCode = "preflight_" + errCode
	emit.Emit(ev)
}

// wrapPreflight returns an error whose message tells the operator both what
// failed and what to try first. The plan calls for a "복구 가이드 로그" —
// this is that guide, surfaced via the returned error.
func wrapPreflight(err error, dbDir, op string) error {
	return fmt.Errorf(`badger preflight %s failed at %s: %w

Recovery guide:
  1. Check disk space: 'df -h %s' (badger needs at least 1GB headroom).
  2. Check permissions: the user running grainfs must own %s and have rwx.
  3. If a previous grainfs is still running, stop it before retrying.
  4. If corruption is suspected, restore from the latest snapshot under
     <data>/snapshots/ rather than re-launching against this dir`, op, dbDir, err, dbDir, dbDir)
}
