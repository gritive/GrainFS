// Phase 16 Week 3 — Self-healing storage at startup.
//
// On every boot grainfs sweeps a small set of well-defined orphan artifacts
// that crashes can leave behind:
//
//   - *.tmp files: leftovers from interrupted atomic writes (write-tmp,
//     fsync, rename pattern). These are safe to delete after a guard window
//     because no live writer would still own them.
//   - parts/<uploadID>/ directories: multipart uploads abandoned by S3
//     clients that never called CompleteMultipartUpload or AbortMultipart.
//
// What we deliberately do NOT clean up:
//
//   - blob.lock and similar flock-style lock files: the kernel releases
//     flocks on process exit, so a leftover file is harmless and the next
//     start re-acquires it cleanly.
//   - In-memory caches (CachedBackend): no on-disk cache exists today; once
//     a disk-backed pull-through cache lands, this is where its quarantine
//     logic will hook in.
//   - BadgerDB internals: badger.Open already does WAL replay and basic
//     recovery. A separate atomic recovery layer is Phase 17.
//
// Each cleanup action emits a HealEvent{Phase: startup} so the dashboard's
// "Restart Recovery" line shows the operator exactly what happened. A clean
// boot emits no per-action events to avoid dashboard noise.
package server

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// tmpInflightGuard is the minimum age before a *.tmp file is considered
// orphan. Live atomic writes complete in milliseconds; anything older than
// this is from a previous process generation.
const tmpInflightGuard = 5 * time.Minute

// multipartOrphanAge is how long an uncompleted multipart upload directory
// must sit untouched before startup recovery sweeps it. S3 clients that
// upload across hours of WAN flakiness still get a full day to finish.
const multipartOrphanAge = 24 * time.Hour

// StartupRecoveryResult is a summary returned to the caller and surfaced
// via the existing dashboard health endpoint.
type StartupRecoveryResult struct {
	OrphanTmpRemoved       int
	OrphanMultipartRemoved int
	Errors                 []string // non-fatal scan/delete failures (logged + counted)
}

// RunStartupRecovery scans dataRoot for orphan artifacts left behind by a
// previous crashed process and removes them. Each removal emits a HealEvent
// via emit (a NoopEmitter is acceptable; nil is also accepted for tests).
//
// The scan is bounded by ctx so a slow shutdown does not block the next
// boot. Returns context.Canceled if the caller cancelled mid-scan.
func RunStartupRecovery(ctx context.Context, dataRoot string, emit scrubber.Emitter) (StartupRecoveryResult, error) {
	var res StartupRecoveryResult
	if emit == nil {
		emit = scrubber.NoopEmitter{}
	}

	if _, err := os.Stat(dataRoot); errors.Is(err, fs.ErrNotExist) {
		// First boot — nothing to clean.
		return res, nil
	}

	if err := sweepOrphanTmp(ctx, dataRoot, emit, &res); err != nil {
		return res, err
	}
	if err := sweepOrphanMultiparts(ctx, dataRoot, emit, &res); err != nil {
		return res, err
	}

	if res.OrphanTmpRemoved > 0 || res.OrphanMultipartRemoved > 0 {
		log.Info().Int("orphan_tmp", res.OrphanTmpRemoved).Int("orphan_multipart", res.OrphanMultipartRemoved).Int("errors", len(res.Errors)).Msg("startup recovery completed")
	}
	return res, nil
}

// sweepOrphanTmp walks dataRoot for *.tmp files older than tmpInflightGuard.
//
// The WalkDir walkFn absorbs every per-entry error (missing root, permission
// denied, symlink loop, transient stat failures) into res.Errors and returns
// nil, so WalkDir itself only returns non-nil when the caller cancelled the
// context. The tail-end error handling below reflects that contract: we
// propagate context.Canceled, and we defensively record + log any other
// non-context error so a future change to walkFn cannot silently drop signal.
func sweepOrphanTmp(ctx context.Context, root string, emit scrubber.Emitter, res *StartupRecoveryResult) error {
	cutoff := time.Now().Add(-tmpInflightGuard)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			// Permission or transient stat errors should not abort the boot;
			// we record them and keep walking so one bad subtree doesn't
			// hide all the others.
			res.Errors = append(res.Errors, walkErr.Error())
			return nil
		}
		if d.IsDir() || filepath.Ext(d.Name()) != ".tmp" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
			return nil
		}
		if info.ModTime().After(cutoff) {
			return nil // still in the in-flight protection window
		}
		emitStartup(emit, path, "orphan_tmp", scrubber.OutcomeSuccess)
		if err := os.Remove(path); err != nil {
			res.Errors = append(res.Errors, err.Error())
			emitStartup(emit, path, "orphan_tmp", scrubber.OutcomeFailed)
			return nil
		}
		res.OrphanTmpRemoved++
		return nil
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	// Not reachable under the current walkFn (it never returns a non-context
	// error), but preserve the signal if a future change introduces one.
	// Record the error so it surfaces on the "Restart Recovery" dashboard
	// line and emit a structured log so operators can triage.
	res.Errors = append(res.Errors, "walkdir root="+root+": "+err.Error())
	log.Warn().Str("root", root).Str("operation", "orphan_tmp").Err(err).Msg("startup recovery walkdir non-context error")
	return nil
}

// sweepOrphanMultiparts removes parts/<uploadID> directories untouched
// for more than multipartOrphanAge.
func sweepOrphanMultiparts(ctx context.Context, root string, emit scrubber.Emitter, res *StartupRecoveryResult) error {
	partsRoot := filepath.Join(root, "parts")
	entries, err := os.ReadDir(partsRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		res.Errors = append(res.Errors, err.Error())
		return nil
	}
	cutoff := time.Now().Add(-multipartOrphanAge)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			continue
		}
		full := filepath.Join(partsRoot, entry.Name())
		info, err := entry.Info()
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		emitStartup(emit, full, "orphan_multipart", scrubber.OutcomeSuccess)
		if err := os.RemoveAll(full); err != nil {
			res.Errors = append(res.Errors, err.Error())
			emitStartup(emit, full, "orphan_multipart", scrubber.OutcomeFailed)
			continue
		}
		res.OrphanMultipartRemoved++
	}
	return nil
}

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
