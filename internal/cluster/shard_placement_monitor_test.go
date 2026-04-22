package cluster

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestShardService returns a ShardService rooted at a fresh temp dir.
// Transport is nil — the service is used only for its local-disk layout here.
func newTestShardService(t *testing.T) (*ShardService, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "shard-svc-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	svc := NewShardService(dir, nil)
	return svc, dir
}

func TestShardPlacementMonitor_Scan_AllPresent(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	const self = "node-A"
	monitor := NewShardPlacementMonitor(fsm, svc, self, time.Second)

	// Write a placement where self holds shard 1.
	nodes := []string{"node-B", self, "node-C"}
	raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: nodes,
	})
	require.NoError(t, fsm.Apply(raw))

	// Create the shard that self is supposed to hold.
	require.NoError(t, svc.WriteLocalShard("b", "k", 1, []byte("payload")))

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing)
}

func TestShardPlacementMonitor_Scan_DetectsMissing(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	const self = "node-A"
	monitor := NewShardPlacementMonitor(fsm, svc, self, time.Second)

	// Two placements — self is holder at different shardIdx in each. We don't
	// create the shard files, so both should show up as missing.
	p1 := PutShardPlacementCmd{
		Bucket: "b", Key: "obj1", NodeIDs: []string{self, "other", "other2"},
	}
	p2 := PutShardPlacementCmd{
		Bucket: "b", Key: "obj2", NodeIDs: []string{"other", "other2", self},
	}
	for _, p := range []PutShardPlacementCmd{p1, p2} {
		raw, _ := EncodeCommand(CmdPutShardPlacement, p)
		require.NoError(t, fsm.Apply(raw))
	}

	var reported []string
	monitor.SetOnMissing(func(bucket, key string, shardIdx int) {
		reported = append(reported, fmtShardRef(bucket, key, shardIdx))
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, missing)
	assert.ElementsMatch(t, []string{"b/obj1/0", "b/obj2/2"}, reported)
}

func TestShardPlacementMonitor_Scan_IgnoresPeerShards(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	const self = "node-A"
	monitor := NewShardPlacementMonitor(fsm, svc, self, time.Second)

	// self is NOT in the placement — monitor should skip every shard.
	raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"node-B", "node-C"},
	})
	require.NoError(t, fsm.Apply(raw))

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing, "peer-assigned shards should not count as self-missing")
}

func TestShardPlacementMonitor_Scan_NoPlacements(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	monitor := NewShardPlacementMonitor(fsm, svc, "anyone", time.Second)
	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing)
}

func TestShardPlacementMonitor_Stats(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	monitor := NewShardPlacementMonitor(fsm, svc, "node-A", time.Second)
	pre := monitor.Stats()
	assert.Zero(t, pre.TotalScans)
	assert.Zero(t, pre.LastScanUnixNano)

	_, _ = monitor.Scan(context.Background())
	post := monitor.Stats()
	assert.Equal(t, uint64(1), post.TotalScans)
	assert.Greater(t, post.LastScanUnixNano, int64(0))
}

func TestShardPlacementMonitor_Scan_ContextCancel(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	// Seed ~100 placements so iteration has something to traverse.
	for i := 0; i < 100; i++ {
		raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
			Bucket: "b", Key: fmtKey(i), NodeIDs: []string{"node-A"},
		})
		require.NoError(t, fsm.Apply(raw))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	monitor := NewShardPlacementMonitor(fsm, svc, "node-A", time.Second)
	_, err := monitor.Scan(ctx)
	// Either ctx.Err wraps to non-nil scan error, or scan completes before
	// first ctx check — both are acceptable. We only assert no panic.
	_ = err
}

func TestFSM_IterShardPlacements(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Empty FSM: callback never invoked.
	count := 0
	err := fsm.IterShardPlacements(func(bucket, key string, nodes []string) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, count)

	// Seed a handful of placements.
	entries := []PutShardPlacementCmd{
		{Bucket: "b1", Key: "k1", NodeIDs: []string{"n0", "n1"}},
		{Bucket: "b2", Key: "k/with/slashes", NodeIDs: []string{"n2", "n3", "n4"}},
		{Bucket: "버킷", Key: "한글", NodeIDs: []string{"n0"}},
	}
	for _, e := range entries {
		raw, _ := EncodeCommand(CmdPutShardPlacement, e)
		require.NoError(t, fsm.Apply(raw))
	}

	seen := make(map[string][]string)
	err = fsm.IterShardPlacements(func(bucket, key string, nodes []string) error {
		seen[bucket+"/"+key] = nodes
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, seen, 3)
	assert.Equal(t, []string{"n2", "n3", "n4"}, seen["b2/k/with/slashes"])
	assert.Equal(t, []string{"n0"}, seen["버킷/한글"])
}

func TestShardPlacementMonitor_Scan_NilShardSvc(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	monitor := NewShardPlacementMonitor(fsm, nil, "node-A", time.Second)
	_, err := monitor.Scan(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard service not configured")
}

func TestShardPlacementMonitor_Scan_NonEnoentError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("chmod 000 has no effect as root")
	}
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, dir := newTestShardService(t)

	const self = "node-A"
	monitor := NewShardPlacementMonitor(fsm, svc, self, time.Second)

	raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{self},
	})
	require.NoError(t, fsm.Apply(raw))

	// Write shard, then make it unreadable (permission error, not ENOENT).
	// NewShardService roots data at dir+"/shards/", so the full shard path is:
	// dir/shards/b/k/shard_0
	require.NoError(t, svc.WriteLocalShard("b", "k", 0, []byte("data")))
	shardPath := dir + "/shards/b/k/shard_0"
	require.NoError(t, os.Chmod(shardPath, 0o000))
	t.Cleanup(func() { _ = os.Chmod(shardPath, 0o600) })

	// Non-ENOENT error: shard exists on disk but is unreadable.
	// Scan must not count it as "missing" and must not panic.
	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing, "unreadable (non-ENOENT) shard must not count as missing")
}

func TestShardPlacementMonitor_Start_StopsOnCtxCancel(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	monitor := NewShardPlacementMonitor(fsm, svc, "node-A", 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.Start(ctx)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not stop within 2s after ctx cancel")
	}
}

func TestShardPlacementMonitor_Scan_CtxCancelMidRepair(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	svc, _ := newTestShardService(t)

	const self = "node-A"
	// Seed 5 missing shards to ensure the repair loop has entries to iterate.
	for i := 0; i < 5; i++ {
		raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
			Bucket: "b", Key: fmtKey(i), NodeIDs: []string{self},
		})
		require.NoError(t, fsm.Apply(raw))
	}

	monitor := NewShardPlacementMonitor(fsm, svc, self, time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	// onMissing cancels the context on the first call, simulating mid-repair cancel.
	called := 0
	monitor.SetOnMissing(func(bucket, key string, shardIdx int) {
		called++
		cancel()
	})

	_, err := monitor.Scan(ctx)
	// Scan must not panic and must have called onMissing at most once.
	assert.LessOrEqual(t, called, 1, "mid-repair ctx cancel must stop after first callback")
	_ = err // may or may not propagate ctx error — both are acceptable
}

func fmtShardRef(bucket, key string, idx int) string {
	return bucket + "/" + key + "/" + itoa(idx)
}

func fmtKey(i int) string {
	return "obj-" + itoa(i)
}

// itoa avoids pulling strconv into the test for tiny helpers. 0-999 is fine
// since the tests never exceed that range.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
