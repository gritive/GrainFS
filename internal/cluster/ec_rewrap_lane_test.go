package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// setupECRewrapBackend mirrors setupECBackend but returns the keeper so the test
// can rotate the active DEK generation (gen 0 -> 1) after seeding a shard.
func setupECRewrapBackend(t *testing.T) (*DistributedBackend, *encrypt.DEKKeeper) {
	t.Helper()
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "self"
	shardDir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(shardDir, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr, selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	return backend, keeper
}

// shardGenOnDisk reads the raw on-disk shard bytes (standalone file) and returns
// the encrypted header generation.
func shardGenOnDisk(t *testing.T, b *DistributedBackend, bucket, canonicalKey string, shardIdx int) uint32 {
	t.Helper()
	path := mustShardPath(b.shardSvc, bucket, canonicalKey, shardIdx)
	raw, err := readFileOrPack(b, bucket, canonicalKey, shardIdx, path)
	require.NoError(t, err)
	gen, ok := eccodec.EncryptedShardGen(raw)
	require.True(t, ok, "expected an encrypted shard header on disk")
	return gen
}

func readFileOrPack(b *DistributedBackend, bucket, canonicalKey string, shardIdx int, path string) ([]byte, error) {
	if b.shardSvc.shardPack != nil {
		if raw, ok, err := b.shardSvc.shardPack.get(bucket, canonicalKey, shardIdx); err != nil {
			return nil, err
		} else if ok {
			return raw, nil
		}
	}
	return os.ReadFile(path)
}

func TestRewrapShardIfStale(t *testing.T) {
	backend, keeper := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	content := bytes.Repeat([]byte("rewrap-shard-payload-"), 64)
	obj, err := backend.PutObject(ctx, "b", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	canonicalKey := ecObjectShardKey("obj", obj.VersionID)
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b", canonicalKey, 0), "seeded at gen 0")

	// Rotate the active DEK generation to 1.
	require.NoError(t, keeper.Rotate())
	if _, active := keeper.VersionsAndActive(); active != 1 {
		t.Fatalf("expected active gen 1 after rotate, got %d", active)
	}

	// First call migrates the stale (gen 0) shard onto the active gen (1).
	did, err := backend.RewrapShardIfStale("b", "obj", obj.VersionID, 0, 1)
	require.NoError(t, err)
	assert.True(t, did, "stale shard must be migrated")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b", canonicalKey, 0), "shard now at active gen")

	// Object still reads back the same plaintext after rewrap.
	rc, _, err := backend.GetObject(ctx, "b", "obj")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	assert.Equal(t, content, got, "plaintext preserved across rewrap")

	// Second call is idempotent (already at active gen).
	did, err = backend.RewrapShardIfStale("b", "obj", obj.VersionID, 0, 1)
	require.NoError(t, err)
	assert.False(t, did, "already-migrated shard must be a no-op")
}

// TestECRewrap_UpgradeReSplitSerial de-risks the race harness: it proves
// upgradeObjectEC works in the singleton fixture (k=1 -> k=2) and that the new
// acquireShardWriteLock wrap does not self-deadlock.
func TestECRewrap_UpgradeReSplitSerial(t *testing.T) {
	backend, _ := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	content := bytes.Repeat([]byte("upgrade-resplit-serial-"), 200)
	obj, err := backend.PutObject(ctx, "b", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	require.NoError(t, backend.upgradeObjectEC(ctx, "b", "obj", PlacementRecord{}, ECConfig{DataShards: 2, ParityShards: 1}))
	backend.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})

	rc, _, err := backend.GetObject(ctx, "b", "obj")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	assert.Equal(t, content, got, "object must round-trip after k=1 -> k=2 upgrade")
	_ = obj
}

// TestECRewrap_ConfigUpgradeRace is the mandatory -race deliverable. It runs a
// config-upgrade re-split (upgradeObjectEC, k=1 -> k=2) concurrently with
// RewrapShardIfStale on the SAME object (same versionID, shard 0) across many
// iterations and asserts the object always reads back the UPGRADED content with
// no corruption and no upgrade error.
//
// This is a concurrency-safety guard: it exercises the contended path under the
// race detector and proves the two writers never produce a torn/undecodable
// object. It is NOT a strict regression guard for the lock's PRESENCE: the
// clobber is a timing-narrow filesystem last-writer race (the detector does not
// flag it), and even a clobbered shard 0 decodes to correct GET bytes because
// the stale whole-object fragment's prefix equals the correct first-half
// fragment under EC reconstruction. The deterministic proof that the lock is
// necessary lives in TestECRewrap_ConfigUpgradeLockSerializesWrite, which
// asserts at the on-disk shard layer (where the clobber is observable).
func TestECRewrap_ConfigUpgradeRace(t *testing.T) {
	backend, keeper := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	const iterations = 80
	for it := 0; it < iterations; it++ {
		key := "obj-" + strconv.Itoa(it)
		// Distinct first/second halves so a whole-object clobber is detectable.
		content := append(bytes.Repeat([]byte("AAAA"), 256), bytes.Repeat([]byte("BBBB"), 256)...)

		backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
		obj, err := backend.PutObject(ctx, "b", key, bytes.NewReader(content), "application/octet-stream")
		require.NoError(t, err)
		require.NotEmpty(t, obj.VersionID)

		// Rotate so the seeded shard (gen G) is stale vs the new active gen G+1.
		require.NoError(t, keeper.Rotate())
		_, activeGen := keeper.VersionsAndActive()

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var upErr error
		go func() {
			defer wg.Done()
			<-start
			upErr = backend.upgradeObjectEC(ctx, "b", key, PlacementRecord{}, ECConfig{DataShards: 2, ParityShards: 1})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = backend.RewrapShardIfStale("b", key, obj.VersionID, 0, activeGen)
		}()
		close(start)
		wg.Wait()
		require.NoError(t, upErr, "iteration %d: upgrade must not error", it)

		backend.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})
		rc, _, err := backend.GetObject(ctx, "b", key)
		require.NoError(t, err, "iteration %d: GetObject", it)
		got, err := io.ReadAll(rc)
		rc.Close()
		require.NoError(t, err, "iteration %d: read body", it)
		require.Equal(t, content, got, "iteration %d: object must read back upgraded content, not stale-clobbered bytes", it)
	}
}

// TestECRewrap_ConfigUpgradeLockSerializesWrite is the deterministic teeth guard
// for the A3 write lock. It uses rewrapTestHook to pin the rewrap inside its
// read-modify-write window (plaintext read done, re-seal pending) while a
// config-upgrade re-split (k=1 -> k=2) writes shard 0. With the lock the rewrap
// holds the per-shard write lock across the window, so the upgrade's shard-0
// write blocks until the rewrap releases and is therefore the LAST writer: the
// on-disk shard 0 ends up as the upgraded first-half fragment. Removing the lock
// lines in upgradeObjectEC makes the upgrade write land DURING the window and
// the rewrap clobber it back to the stale whole-object fragment, flipping this
// assertion. (GET-bytes cannot distinguish the two because the stale whole-object
// shard's prefix equals the correct first-half fragment under EC reconstruction,
// which is why this asserts at the on-disk shard layer.)
func TestECRewrap_ConfigUpgradeLockSerializesWrite(t *testing.T) {
	backend, keeper := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	key := "obj"
	firstHalf := bytes.Repeat([]byte("AAAA"), 256)  // 1024 bytes
	secondHalf := bytes.Repeat([]byte("BBBB"), 256) // 1024 bytes
	content := append(append([]byte{}, firstHalf...), secondHalf...)

	// The k=2 shard-0 data fragment (the bytes the upgrade re-split writes) — the
	// raw split fragment, distinct from both the whole object and a naive half.
	upgradeShards, err := ECSplit(ECConfig{DataShards: 2, ParityShards: 1}, content)
	require.NoError(t, err)
	upgradeShard0 := upgradeShards[0]
	require.NotEqual(t, len(content), len(upgradeShard0), "upgrade shard 0 must be a fragment, not the whole object")

	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	obj, err := backend.PutObject(ctx, "b", key, bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NoError(t, keeper.Rotate())
	_, activeGen := keeper.VersionsAndActive()

	rewrapTestHook = func() { time.Sleep(80 * time.Millisecond) }
	t.Cleanup(func() { rewrapTestHook = nil })

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = backend.RewrapShardIfStale("b", key, obj.VersionID, 0, activeGen)
	}()
	// Let the rewrap reach the hook (plaintext read complete, lock held, sleeping)
	// before the upgrade attempts its shard-0 write.
	time.Sleep(20 * time.Millisecond)
	go func() {
		defer wg.Done()
		_ = backend.upgradeObjectEC(ctx, "b", key, PlacementRecord{}, ECConfig{DataShards: 2, ParityShards: 1})
	}()
	wg.Wait()

	canonicalKey := ecObjectShardKey(key, obj.VersionID)
	plain, err := backend.shardSvc.ReadLocalShard("b", canonicalKey, 0)
	require.NoError(t, err)
	require.Equal(t, upgradeShard0, plain,
		"upgrade must be the last writer of shard 0 (k=2 fragment); a rewrap clobber would leave the stale whole-object plaintext")
	require.Equal(t, uint32(activeGen), shardGenOnDisk(t, backend, "b", canonicalKey, 0),
		"shard 0 sealed at the active gen")
}

// fakeECRewrapBackend is one fake data-group backend: it yields its objects via
// ScanGroupObjects and records RewrapShardIfStale calls.
type fakeECRewrapBackend struct {
	nodeID     string
	objects    []scrubber.ObjectRecord // objects stored in this group
	owned      map[string][]int        // key|version -> shard indices
	rewrapErr  error
	mu         sync.Mutex
	calls      []rewrapCall
	didOnFirst map[string]bool // key|version|idx -> already returned true
}

type rewrapCall struct {
	bucket, key, version string
	idx                  int
	activeGen            uint32
}

func (f *fakeECRewrapBackend) ScanGroupObjects() <-chan scrubber.ObjectRecord {
	ch := make(chan scrubber.ObjectRecord, len(f.objects))
	for _, r := range f.objects {
		ch <- r
	}
	close(ch)
	return ch
}

func (f *fakeECRewrapBackend) OwnedShards(bucket, key, versionID, nodeID string) []int {
	if nodeID != f.nodeID {
		return nil
	}
	return f.owned[key+"|"+versionID]
}

func (f *fakeECRewrapBackend) RewrapShardIfStale(bucket, key, versionID string, shardIdx int, activeGen uint32) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, rewrapCall{bucket, key, versionID, shardIdx, activeGen})
	if f.rewrapErr != nil {
		return false, f.rewrapErr
	}
	k := key + "|" + versionID + "|" + strconv.Itoa(shardIdx)
	if f.didOnFirst == nil {
		f.didOnFirst = map[string]bool{}
	}
	if !f.didOnFirst[k] {
		f.didOnFirst[k] = true
		return true, nil
	}
	return false, nil
}

func laneFromGroups(nodeID string, gbs ...ECRewrapShardBackend) *ECRewrapLane {
	return NewECRewrapLane(nodeID, func() []ECRewrapShardBackend { return gbs })
}

func TestECRewrapLane_SweepAllOwnedShards(t *testing.T) {
	fake := &fakeECRewrapBackend{
		nodeID:  "n1",
		objects: []scrubber.ObjectRecord{{Bucket: "b", Key: "k1", VersionID: "v1"}},
		owned:   map[string][]int{"k1|v1": {0, 2, 5}},
	}

	before := counterValue(t, 7)
	lane := laneFromGroups("n1", fake)
	require.Equal(t, "ec", lane.Name())
	require.NoError(t, lane.RewrapByGen(context.Background(), 3, 7))

	require.Len(t, fake.calls, 3, "every owned shard visited")
	for _, c := range fake.calls {
		assert.Equal(t, uint32(7), c.activeGen)
	}
	assert.Equal(t, float64(3), counterValue(t, 7)-before, "counter increments per did==true")
}

// TestECRewrapLane_SweepsAllGroups proves the lane visits objects across EVERY
// data group, not just one (the bug the e2e caught: an object can live in a
// group different from its bucket's router assignment).
func TestECRewrapLane_SweepsAllGroups(t *testing.T) {
	g1 := &fakeECRewrapBackend{
		nodeID:  "n1",
		objects: []scrubber.ObjectRecord{{Bucket: "b", Key: "in-g1", VersionID: "v1"}},
		owned:   map[string][]int{"in-g1|v1": {0}},
	}
	g2 := &fakeECRewrapBackend{
		nodeID:  "n1",
		objects: []scrubber.ObjectRecord{{Bucket: "b", Key: "in-g2", VersionID: "v1"}},
		owned:   map[string][]int{"in-g2|v1": {0}},
	}
	lane := laneFromGroups("n1", g1, g2)
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 5))
	require.Len(t, g1.calls, 1, "group 1 object swept")
	require.Len(t, g2.calls, 1, "group 2 object swept")
}

func TestECRewrapLane_CtxCancelStops(t *testing.T) {
	fake := &fakeECRewrapBackend{
		nodeID:  "n1",
		objects: []scrubber.ObjectRecord{{Bucket: "b", Key: "k1", VersionID: "v1"}},
		owned:   map[string][]int{"k1|v1": {0}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lane := laneFromGroups("n1", fake)
	err := lane.RewrapByGen(ctx, 0, 4)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, fake.calls, "cancelled ctx stops before any rewrap")
}

func counterValue(t *testing.T, activeGen uint32) float64 {
	t.Helper()
	return testutil.ToFloat64(RewrapECShardsTotal.WithLabelValues(strconv.FormatUint(uint64(activeGen), 10)))
}
