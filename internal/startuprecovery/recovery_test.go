package startuprecovery

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// newOpsForTest constructs a *storage.Operations rooted at dir for tests
// that exercise the multipart sweep capability path. Returns nil if dir
// already has BadgerDB state we don't want to disturb (callers fall back
// to ops=nil for tmp-only scenarios).
func newOpsForTest(t *testing.T, dir string) *storage.Operations {
	t.Helper()
	b, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return storage.NewOperations(b)
}

// captureEmitter is a HealEvent sink for assertions; lives here too so the
// startup tests stay independent of the scrubber test helpers.
type captureSrvEmitter struct {
	events []scrubber.HealEvent
}

func (c *captureSrvEmitter) Emit(ev scrubber.HealEvent) {
	c.events = append(c.events, ev)
}

type noMultipartSweepBackend struct{ storage.Backend }

func TestStartupRecovery_DeletesOldTmpFiles(t *testing.T) {
	root := t.TempDir()

	old := filepath.Join(root, "shards", "b", "k", "0.tmp")
	require.NoError(t, os.MkdirAll(filepath.Dir(old), 0o755))
	require.NoError(t, os.WriteFile(old, []byte("partial"), 0o644))
	// Backdate so it falls outside the 5-min in-flight protection window.
	past := time.Now().Add(-30 * time.Minute)
	require.NoError(t, os.Chtimes(old, past, past))

	fresh := filepath.Join(root, "shards", "b", "k", "1.tmp")
	require.NoError(t, os.WriteFile(fresh, []byte("inflight"), 0o644))

	cap := &captureSrvEmitter{}
	res, err := Run(context.Background(), root, nil, cap)
	require.NoError(t, err)

	require.Equal(t, 1, res.OrphanTmpRemoved, "old .tmp must be removed, fresh must be kept")
	_, statErr := os.Stat(old)
	require.True(t, os.IsNotExist(statErr), "old .tmp file should be gone")
	_, statErr = os.Stat(fresh)
	require.NoError(t, statErr, "fresh .tmp file must be preserved (in-flight write protection)")

	require.NotEmpty(t, cap.events)
	require.Equal(t, scrubber.PhaseStartup, cap.events[0].Phase)
	require.Equal(t, "orphan_tmp", cap.events[0].ErrCode)
}

func TestStartupRecovery_SkipsRaftBadgerInternalDirs(t *testing.T) {
	root := t.TempDir()
	past := time.Now().Add(-30 * time.Minute)

	oldShardTmp := filepath.Join(root, "shards", "b", "k", "0.tmp")
	require.NoError(t, os.MkdirAll(filepath.Dir(oldShardTmp), 0o755))
	require.NoError(t, os.WriteFile(oldShardTmp, []byte("partial"), 0o644))
	require.NoError(t, os.Chtimes(oldShardTmp, past, past))

	for _, p := range []string{
		filepath.Join(root, "groups", "group-1", "raft", "000001.sst.tmp"),
		filepath.Join(root, "shared-raft-log", "000002.sst.tmp"),
		filepath.Join(root, ".recovery", "entries", "entry-1.tmp"),
	} {
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte("badger-internal"), 0o644))
		require.NoError(t, os.Chtimes(p, past, past))
	}

	res, err := Run(context.Background(), root, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, res.OrphanTmpRemoved)
	require.NoFileExists(t, oldShardTmp)
	require.FileExists(t, filepath.Join(root, "groups", "group-1", "raft", "000001.sst.tmp"))
	require.FileExists(t, filepath.Join(root, "shared-raft-log", "000002.sst.tmp"))
	require.FileExists(t, filepath.Join(root, ".recovery", "entries", "entry-1.tmp"))
}

func TestStartupRecovery_DeletesOldMultipartParts(t *testing.T) {
	root := t.TempDir()

	oldUpload := filepath.Join(root, "parts", "abandoned-upload-id")
	require.NoError(t, os.MkdirAll(oldUpload, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(oldUpload, "00001"), []byte("part1"), 0o644))
	past := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(oldUpload, past, past))

	freshUpload := filepath.Join(root, "parts", "active-upload-id")
	require.NoError(t, os.MkdirAll(freshUpload, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(freshUpload, "00001"), []byte("p"), 0o644))

	cap := &captureSrvEmitter{}
	ops := newOpsForTest(t, root)
	res, err := Run(context.Background(), root, ops, cap)
	require.NoError(t, err)

	require.Equal(t, 1, res.OrphanMultipartRemoved)
	_, statErr := os.Stat(oldUpload)
	require.True(t, os.IsNotExist(statErr), "abandoned upload dir should be gone")
	_, statErr = os.Stat(freshUpload)
	require.NoError(t, statErr, "active upload must survive")

	require.NotEmpty(t, cap.events)
	hasMultipart := false
	for _, e := range cap.events {
		if e.ErrCode == "orphan_multipart" {
			hasMultipart = true
			require.Equal(t, scrubber.PhaseStartup, e.Phase)
		}
	}
	require.True(t, hasMultipart, "expected an orphan_multipart HealEvent")
}

func TestStartupRecovery_DeletesOldMultipartPartsWhenOpsCannotReachSweeper(t *testing.T) {
	root := t.TempDir()

	oldUpload := filepath.Join(root, "parts", "abandoned-upload-id")
	require.NoError(t, os.MkdirAll(oldUpload, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(oldUpload, "00001"), []byte("part1"), 0o644))
	past := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(oldUpload, past, past))

	local, err := storage.NewLocalBackend(root)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	cap := &captureSrvEmitter{}
	ops := storage.NewOperations(noMultipartSweepBackend{Backend: local})
	res, err := Run(context.Background(), root, ops, cap)
	require.NoError(t, err)

	require.Equal(t, 1, res.OrphanMultipartRemoved)
	require.NoFileExists(t, oldUpload, "abandoned upload dir should be gone even when ops cannot unwrap to the local sweeper")

	require.NotEmpty(t, cap.events)
	require.Equal(t, "orphan_multipart", cap.events[0].ErrCode)
	require.Equal(t, scrubber.PhaseStartup, cap.events[0].Phase)
}

func TestStartupRecovery_NothingToCleanEmitsNoEvents(t *testing.T) {
	// A clean restart must NOT spam the dashboard with no-op events.
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "shards"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "parts"), 0o755))

	cap := &captureSrvEmitter{}
	ops := newOpsForTest(t, root)
	res, err := Run(context.Background(), root, ops, cap)
	require.NoError(t, err)
	require.Equal(t, 0, res.OrphanTmpRemoved+res.OrphanMultipartRemoved)
	require.Empty(t, cap.events, "clean restart must not emit per-action HealEvents")
}

func TestStartupRecovery_MissingDataRoot(t *testing.T) {
	// Pointing at a non-existent root must return cleanly — operator might be
	// running --data on first boot before any state exists.
	cap := &captureSrvEmitter{}
	res, err := Run(context.Background(), "/nonexistent/grainfs/data", nil, cap)
	require.NoError(t, err)
	require.Equal(t, 0, res.OrphanTmpRemoved)
	require.Equal(t, 0, res.OrphanMultipartRemoved)
}

func TestStartupRecovery_NilEmitterIsSafe(t *testing.T) {
	root := t.TempDir()
	tmp := filepath.Join(root, "x.tmp")
	require.NoError(t, os.WriteFile(tmp, []byte{}, 0o644))
	past := time.Now().Add(-10 * time.Minute)
	require.NoError(t, os.Chtimes(tmp, past, past))

	_, err := Run(context.Background(), root, nil, nil)
	require.NoError(t, err)
}

func TestStartupRecovery_ContextCancelStops(t *testing.T) {
	// A cancelled context must short-circuit so a slow shutdown does not block
	// the next boot waiting for a giant data dir to finish scanning.
	root := t.TempDir()
	for i := range 10 {
		p := filepath.Join(root, "f"+string(rune('a'+i))+".tmp")
		require.NoError(t, os.WriteFile(p, []byte{}, 0o644))
		past := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(p, past, past))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Run(ctx, root, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}
