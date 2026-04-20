package server

import (
	"errors"
	"fmt"
	"log/slog"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// preflightSentinel is the key written and read back during the Badger
// preflight to confirm the DB is operational. Stored under a reserved
// "_sys:preflight" prefix that no production code path uses.
var preflightSentinel = []byte("_sys:preflight")

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

	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(preflightSentinel, []byte("ok"))
	}); err != nil {
		emitPreflight(emit, "write_failed", scrubber.OutcomeFailed)
		return wrapPreflight(err, dbDir, "write")
	}

	if err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(preflightSentinel)
		return err
	}); err != nil {
		emitPreflight(emit, "read_failed", scrubber.OutcomeFailed)
		return wrapPreflight(err, dbDir, "read")
	}

	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Delete(preflightSentinel)
	}); err != nil {
		// Cleanup failure is non-fatal — the sentinel will live on as a
		// few bytes that operators can ignore. Log it but don't refuse boot.
		slog.Warn("preflight cleanup failed (non-fatal)", "err", err, "dir", dbDir)
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
