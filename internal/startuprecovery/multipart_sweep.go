package startuprecovery

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// sweepOrphanMultiparts delegates the parts/<uploadID> sweep to the storage
// operations facade, then scans dataRoot/parts directly for any leftovers. The
// direct pass is intentionally a fallback instead of the primary path: normal
// storage backends should own their staging layout, but startup recovery must
// still clean the configured data directory when runtime routing wrappers hide
// the LocalBackend capability from Operations.
func sweepOrphanMultiparts(ctx context.Context, dataRoot string, ops *storage.Operations, emit scrubber.Emitter, res *Result) error {
	if ops == nil {
		return nil
	}
	cutoff := time.Now().Add(-multipartOrphanAge)
	sweep, err := ops.SweepOrphanMultiparts(ctx, cutoff)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if err != nil {
		res.Errors = append(res.Errors, err.Error())
		return nil
	}
	recordMultipartSweep(sweep, emit, res)

	fallback, err := sweepOrphanMultipartsFromDataRoot(ctx, dataRoot, cutoff)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if err != nil {
		res.Errors = append(res.Errors, err.Error())
		return nil
	}
	recordMultipartSweep(fallback, emit, res)
	return nil
}

func sweepOrphanMultipartsFromDataRoot(ctx context.Context, dataRoot string, cutoff time.Time) (storage.OrphanMultipartSweepResult, error) {
	var res storage.OrphanMultipartSweepResult
	partsRoot := filepath.Join(dataRoot, "parts")
	entries, err := os.ReadDir(partsRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		res.Errors = append(res.Errors, err.Error())
		return res, nil
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return res, err
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
		if err := os.RemoveAll(full); err != nil {
			res.Errors = append(res.Errors, err.Error())
			continue
		}
		res.Removed++
		res.RemovedPaths = append(res.RemovedPaths, full)
	}
	return res, nil
}

func recordMultipartSweep(sweep storage.OrphanMultipartSweepResult, emit scrubber.Emitter, res *Result) {
	res.OrphanMultipartRemoved += sweep.Removed
	res.Errors = append(res.Errors, sweep.Errors...)
	for _, path := range sweep.RemovedPaths {
		emitStartup(emit, path, "orphan_multipart", scrubber.OutcomeSuccess)
	}
	for _, msg := range sweep.Errors {
		emitStartup(emit, msg, "orphan_multipart", scrubber.OutcomeFailed)
	}
}
