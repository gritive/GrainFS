package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// dirSyncPathRec records the ORDER of directory paths passed to the syncDirHook
// seam. syncDirChain never touches disk when the hook is set, so tests drive it
// with arbitrary path strings and assert the exact fsync sequence.
type dirSyncPathRec struct {
	mu     sync.Mutex
	paths  []string
	failOn map[string]error // optional: return this error when the path is fsynced
}

func (r *dirSyncPathRec) hook(p string) error {
	r.mu.Lock()
	r.paths = append(r.paths, p)
	err := r.failOn[p]
	r.mu.Unlock()
	return err
}

func (r *dirSyncPathRec) seq() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.paths))
	copy(out, r.paths)
	return out
}

func (r *dirSyncPathRec) reset() {
	r.mu.Lock()
	r.paths = nil
	r.mu.Unlock()
}

// newDedupSvc builds a LocalShardStore usable for syncDirChain unit tests — the
// durability state machine now lives there, so the test drives it directly (no
// transport, no facade). NewLocalShardStore PANICS without a sealer ("at-rest
// sealer is mandatory"), so we MUST pass WithLocalShardDEKKeeper exactly like the
// other cluster tests (cf. testDEKKeeper). Once it constructs, the syncDirHook
// seam short-circuits all disk I/O in syncDirChain, so the unit tests still drive
// it with arbitrary path strings.
func newDedupSvc(t *testing.T, rec *dirSyncPathRec) *LocalShardStore {
	t.Helper()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewLocalShardStore([]string{t.TempDir()}, WithLocalShardDEKKeeper(keeper, clusterID))
	svc.syncDirHook = rec.hook
	return svc
}

// TestSyncDirChain_FirstWrite_FsyncsFullChain proves the first write to a fresh
// chain fsyncs leaf + every ancestor up to (exclusive) stop — identical to the
// pre-optimization behavior (no regression on the establishing write).
func TestSyncDirChain_FirstWrite_FsyncsFullChain(t *testing.T) {
	rec := &dirSyncPathRec{}
	svc := newDedupSvc(t, rec)

	err := svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards")
	require.NoError(t, err)
	require.Equal(t,
		[]string{"/data/shards/b/seg/blob1", "/data/shards/b/seg", "/data/shards/b"},
		rec.seq(),
		"first write: leaf + all ancestors up to (exclusive) stop, leaf-first")
}

// TestSyncDirChain_LeafEqualsStop_NoOp pins the degenerate guard: when leaf==stop
// the call fsyncs NOTHING and returns nil (preserving the pre-dedup `d != stop`
// loop's behavior). Unreachable for real shards (dir is always strictly below the
// data dir), but guards against a future refactor reintroducing a walk-above-stop.
func TestSyncDirChain_LeafEqualsStop_NoOp(t *testing.T) {
	rec := &dirSyncPathRec{}
	svc := newDedupSvc(t, rec)

	require.NoError(t, svc.syncDirChain("/data/shards", "/data/shards"))
	require.Empty(t, rec.seq(), "leaf==stop must fsync nothing")
}

// TestSyncDirChain_SameLeafRepeat_FsyncsLeafOnly proves a repeat write to the
// SAME leaf (a new shard FILE, no new dirs) fsyncs ONLY the leaf — the ancestor
// chain was made durable by the first write.
func TestSyncDirChain_SameLeafRepeat_FsyncsLeafOnly(t *testing.T) {
	rec := &dirSyncPathRec{}
	svc := newDedupSvc(t, rec)

	require.NoError(t, svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards"))
	rec.reset() // measure only the SECOND write

	require.NoError(t, svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards"))
	require.Equal(t, []string{"/data/shards/b/seg/blob1"}, rec.seq(),
		"same-leaf repeat: only the leaf is re-fsynced (new shard file); ancestors already durable")
}

// TestSyncDirChain_CrossSegment_SkipsSharedAncestor proves the headline win: a
// SECOND blob leaf under the SAME <obj>_segments dir fsyncs its own new leaf +
// the shared <obj>_segments (to persist the new blob entry) but SKIPS the bucket
// dir, which the first blob already made durable. This is the chunking-amplified
// redundancy this change removes.
func TestSyncDirChain_CrossSegment_SkipsSharedAncestor(t *testing.T) {
	rec := &dirSyncPathRec{}
	svc := newDedupSvc(t, rec)

	require.NoError(t, svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards"))
	rec.reset()

	require.NoError(t, svc.syncDirChain("/data/shards/b/seg/blob2", "/data/shards"))
	require.Equal(t,
		[]string{"/data/shards/b/seg/blob2", "/data/shards/b/seg"},
		rec.seq(),
		"cross-segment: new leaf + shared seg dir (new blob entry), bucket skipped (durable)")
	require.NotContains(t, rec.seq(), "/data/shards/b",
		"bucket must NOT be re-fsynced — it was made durable by blob1")
}

// TestSyncDirChain_NewSiblingUnderBucket_RefsyncsBucket proves we do NOT
// over-skip: a NEW immediate child of bucket (a different object's segments dir)
// re-fsyncs bucket to persist that new child's entry. Skipping it here would lose
// the new dir's namespace link on crash.
func TestSyncDirChain_NewSiblingUnderBucket_RefsyncsBucket(t *testing.T) {
	rec := &dirSyncPathRec{}
	svc := newDedupSvc(t, rec)

	require.NoError(t, svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards"))
	rec.reset()

	require.NoError(t, svc.syncDirChain("/data/shards/b/seg2/blobX", "/data/shards"))
	require.Equal(t,
		[]string{"/data/shards/b/seg2/blobX", "/data/shards/b/seg2", "/data/shards/b"},
		rec.seq(),
		"new sibling seg2 under bucket: bucket MUST be re-fsynced to persist seg2's entry")
}

// TestSyncDirChain_AncestorFsyncError_DoesNotMarkDurable proves the durable mark
// is post-fsync only: if an ancestor fsync fails, that level is left UNMARKED, so
// a later successful write re-establishes the full chain (no durability hole from
// a half-synced chain being treated as durable).
func TestSyncDirChain_AncestorFsyncError_DoesNotMarkDurable(t *testing.T) {
	rec := &dirSyncPathRec{
		failOn: map[string]error{"/data/shards/b/seg": errors.New("injected ancestor fsync failure")},
	}
	svc := newDedupSvc(t, rec)

	err := svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards")
	require.Error(t, err)
	require.ErrorContains(t, err, "injected ancestor fsync failure")
	// blob1's parent (seg) fsync failed → durable(blob1) must NOT be set.
	_, durable := svc.dirDurable.Load("/data/shards/b/seg/blob1")
	require.False(t, durable, "leaf must not be marked durable after a failed ancestor fsync")

	// Clear the injected failure; a retry must redo the FULL chain (proving the
	// errored level was not silently treated as durable).
	rec.mu.Lock()
	rec.failOn = nil
	rec.paths = nil
	rec.mu.Unlock()
	require.NoError(t, svc.syncDirChain("/data/shards/b/seg/blob1", "/data/shards"))
	require.Equal(t,
		[]string{"/data/shards/b/seg/blob1", "/data/shards/b/seg", "/data/shards/b"},
		rec.seq(),
		"after a prior failed ancestor fsync, the retry must re-fsync the full chain")
}

// TestSyncDirChain_OptimizedPath_ReadsBack proves the optimized dir-fsync path
// does not corrupt or lose data end-to-end: a no-redundancy multi-shard object
// (2 data shards on a single data dir → both shards land in the same leaf,
// exercising the dedup AND, under -race, the concurrent same-leaf path) writes
// and reads back byte-identical.
//
// Concurrency note: data shards may be written via errgroup, so exact fsync
// COUNTS are non-deterministic here — the deterministic dedup proof lives in the
// syncDirChain unit tests above. This test asserts correctness (readback) only.
func TestSyncDirChain_OptimizedPath_ReadsBack(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 2, ParityShards: 0}, []string{"self", "self"},
		WithNoRedundancy(func() bool { return true }))

	payload := bytes.Repeat([]byte("dedup-readback-"), 1<<16)
	_, err := backend.PutObject(context.Background(), "b", "obj-dedup",
		bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)

	rc, _, err := backend.GetObject(context.Background(), "b", "obj-dedup")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got, "optimized dir-fsync path must read back byte-identical")
}
