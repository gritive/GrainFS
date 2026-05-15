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
package startuprecovery

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// tmpInflightGuard is the minimum age before a *.tmp file is considered
// orphan. Live atomic writes complete in milliseconds; anything older than
// this is from a previous process generation.
const tmpInflightGuard = 5 * time.Minute

// multipartOrphanAge is how long an uncompleted multipart upload directory
// must sit untouched before startup recovery sweeps it. S3 clients that
// upload across hours of WAN flakiness still get a full day to finish.
const multipartOrphanAge = 24 * time.Hour

// Result is a summary returned to the caller and surfaced
// via the existing dashboard health endpoint.
type Result struct {
	OrphanTmpRemoved       int
	OrphanMultipartRemoved int
	Errors                 []string // non-fatal scan/delete failures (logged + counted)
}

// Run scans dataRoot for orphan artifacts left behind by a
// previous crashed process and removes them. Each removal emits a HealEvent
// via emit (a NoopEmitter is acceptable; nil is also accepted for tests).
//
// ops carries the storage operations facade so the multipart-orphan sweep
// first flows through the storage capability plan (ADR 0001). A direct
// dataRoot/parts fallback then catches runtime backend chains whose local
// sweeper is hidden behind non-unwrappable routing wrappers. nil ops disables
// the multipart sweep — useful for tests that only exercise tmp cleanup.
//
// The scan is bounded by ctx so a slow shutdown does not block the next
// boot. Returns context.Canceled if the caller cancelled mid-scan.
func Run(ctx context.Context, dataRoot string, ops *storage.Operations, emit scrubber.Emitter) (Result, error) {
	var res Result
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
	if err := sweepOrphanMultiparts(ctx, dataRoot, ops, emit, &res); err != nil {
		return res, err
	}

	if res.OrphanTmpRemoved > 0 || res.OrphanMultipartRemoved > 0 {
		log.Info().Int("orphan_tmp", res.OrphanTmpRemoved).Int("orphan_multipart", res.OrphanMultipartRemoved).Int("errors", len(res.Errors)).Msg("startup recovery completed")
	}
	return res, nil
}
