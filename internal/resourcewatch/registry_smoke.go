package resourcewatch

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// smokeStaleMtimeAge — vlog dirs whose newest *.vlog file mtime is older than
// this are classified as stale orphans (likely from removed groups; see TODO
// "Group dir cleanup on RemoveGroup"). Live unregistered dirs (recent mtime)
// fire incidents; stale dirs are log-only.
const smokeStaleMtimeAge = 1 * time.Hour

// SmokeReport summarizes the result of VerifyVlogRegistry.
type SmokeReport struct {
	Live  []string `json:"live"`
	Stale []string `json:"stale"`
}

// VerifyVlogRegistry walks dataDir for *.vlog files, compares parent dirs
// against the registry's known DB dirs, and classifies unregistered dirs as
// live (recent mtime) or stale (old mtime). Strict mode returns error on any
// mismatch; non-strict logs warn and returns the report so callers can emit
// incidents for the live list.
func VerifyVlogRegistry(dataDir string, registry *Registry, strict bool) (SmokeReport, error) {
	if registry == nil {
		registry = Default
	}
	diskDirs := walkVlogDirs(dataDir)
	registered := registeredDirs(registry)

	var report SmokeReport
	now := time.Now()
	for d, mtime := range diskDirs {
		if isExcluded(d) {
			continue
		}
		if _, ok := registered[d]; ok {
			continue
		}
		if now.Sub(mtime) > smokeStaleMtimeAge {
			report.Stale = append(report.Stale, d)
		} else {
			report.Live = append(report.Live, d)
		}
	}
	if strict && (len(report.Live) > 0 || len(report.Stale) > 0) {
		return report, fmt.Errorf("vlog registry under-populated: live=%v stale=%v", report.Live, report.Stale)
	}
	if len(report.Stale) > 0 {
		log.Warn().Strs("stale", report.Stale).Msg("vlog registry: stale orphan dirs (likely from removed groups; see TODO Group dir cleanup on RemoveGroup)")
	}
	if len(report.Live) > 0 {
		log.Warn().Strs("live", report.Live).Msg("vlog registry under-populated — silent leak risk")
	}
	return report, nil
}

func registeredDirs(r *Registry) map[string]struct{} {
	out := make(map[string]struct{})
	for _, e := range r.Snapshot() {
		out[filepath.Clean(e.Dir)] = struct{}{}
	}
	return out
}

// walkVlogDirs returns {parent dir → newest *.vlog mtime within}. Uses SkipDir
// on excluded prefixes (blobs/snapshots/.recovery/wal/shards) to avoid
// descending into directories with millions of files.
func walkVlogDirs(root string) map[string]time.Time {
	out := make(map[string]time.Time)
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if p != root && isExcluded(p) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".vlog") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		dir := filepath.Clean(filepath.Dir(p))
		if cur, ok := out[dir]; !ok || info.ModTime().After(cur) {
			out[dir] = info.ModTime()
		}
		return nil
	})
	return out
}

// isExcluded — hardcoded list of directory components that never host a
// registered BadgerDB. Source-of-truth in code (D6 sub-decision); keep in
// sync with cmd/grainfs subdirectory layout.
func isExcluded(dir string) bool {
	for _, prefix := range []string{"blobs", "snapshots", ".recovery", "wal", "shards"} {
		if strings.Contains(dir, "/"+prefix+"/") || strings.HasSuffix(dir, "/"+prefix) {
			return true
		}
	}
	return false
}
