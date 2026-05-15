package startuprecovery

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// sweepOrphanTmp walks dataRoot for *.tmp files older than tmpInflightGuard.
//
// The WalkDir walkFn absorbs every per-entry error (missing root, permission
// denied, symlink loop, transient stat failures) into res.Errors and returns
// nil, so WalkDir itself only returns non-nil when the caller cancelled the
// context. The tail-end error handling below reflects that contract: we
// propagate context.Canceled, and we defensively record + log any other
// non-context error so a future change to walkFn cannot silently drop signal.
func sweepOrphanTmp(ctx context.Context, root string, emit scrubber.Emitter, res *Result) error {
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
		if d.IsDir() && shouldSkipStartupRecoveryDir(root, path) {
			return filepath.SkipDir
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

func shouldSkipStartupRecoveryDir(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." {
		return false
	}
	first := rel
	if i := strings.IndexRune(rel, os.PathSeparator); i >= 0 {
		first = rel[:i]
	}
	switch first {
	case ".recovery", "groups", "shared-raft-log":
		return true
	default:
		return false
	}
}
