package cluster

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// newTestShardService returns a ShardService rooted at a fresh temp dir.
// Transport is nil — the service is used only for its local-disk layout here.
func newTestShardService(t *testing.T) (*ShardService, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "shard-svc-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	svc := NewShardService(dir, nil, withTestWAL(t))
	return svc, dir
}

func TestShardPlacementMonitor_Scan_AllPresent(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	// Seed an EC segment object: self is shard-0 owner, two peers hold shards 1+2.
	segNodes := []string{self, "node-B", "node-C"}
	seedLatestObjectMetaVersion(t, backend, "b", "obj", "v1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: segNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "seg-0", ECData: 2, ECParity: 1, NodeIDs: segNodes},
		},
	})

	// Write the local shard that self is responsible for (shard index 0).
	require.NoError(t, svc.WriteLocalShard("b", "obj/segments/seg-0", 0, []byte("payload")))

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing)
}

// CmdPutShardPlacement is a no-op; Scan finds no placement rows and reports 0 missing.
func TestShardPlacementMonitor_Scan_DetectsMissing(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	svc, _ := newTestShardService(t)

	const self = "node-A"
	monitor := NewShardPlacementMonitor(fsm, nil, svc, self, time.Second)

	// These applies are no-ops; no placement rows are written.
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
	monitor.SetOnMissing(func(target ECShardScanTarget, shardIdx int) {
		// ShardKey is empty for object-version targets; format from
		// ObjectKey/VersionID to match the other callbacks. (Never fires here.)
		reported = append(reported, fmtShardRef(target.Bucket, target.ObjectKey+"/"+target.VersionID, shardIdx))
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing)
	assert.Empty(t, reported)
}

func TestShardPlacementMonitor_Scan_DetectsMetadataOnlyMissingShard(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "b",
		Key:         "obj",
		VersionID:   "v1",
		Size:        10,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{self, "node-B", "node-C"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	var reported []string
	monitor.SetOnMissing(func(target ECShardScanTarget, shardIdx int) {
		reported = append(reported, fmtShardRef(target.Bucket, target.ObjectKey+"/"+target.VersionID, shardIdx))
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, missing)
	assert.Equal(t, []string{"b/obj/v1/0"}, reported)
}

func TestShardPlacementMonitor_Scan_IgnoresPeerShards(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	// Seed an EC segment object where self is NOT in the node list.
	// The monitor must skip all shards — they belong to peers.
	peerNodes := []string{"node-B", "node-C", "node-D"}
	seedLatestObjectMetaVersion(t, backend, "b", "obj", "v1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: peerNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "seg-peer", ECData: 2, ECParity: 1, NodeIDs: peerNodes},
		},
	})
	// Do NOT write any local shard — self holds nothing.

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing, "peer-assigned shards should not count as self-missing")
}

func TestShardPlacementMonitor_Scan_NoPlacements(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	svc, _ := newTestShardService(t)

	monitor := NewShardPlacementMonitor(fsm, nil, svc, "anyone", time.Second)
	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing)
}

func TestShardPlacementMonitor_Stats(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	svc, _ := newTestShardService(t)

	monitor := NewShardPlacementMonitor(fsm, nil, svc, "node-A", time.Second)
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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	// Seed several EC segment objects so iteration has real targets to traverse.
	for i := 0; i < 10; i++ {
		nodes := []string{self, "node-B", "node-C"}
		seedLatestObjectMetaVersion(t, backend, "b", fmtKey(i), "v1", objectMeta{
			ECData: 2, ECParity: 1, NodeIDs: nodes,
			Segments: []storage.SegmentRef{
				{BlobID: "seg-" + fmtKey(i), ECData: 2, ECParity: 1, NodeIDs: nodes},
			},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	_, err := monitor.Scan(ctx)
	// Either ctx.Err wraps to non-nil scan error, or scan completes before
	// first ctx check — both are acceptable. We only assert no panic.
	_ = err
}

func TestFSM_IterShardPlacements(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Empty FSM: callback never invoked.
	count := 0
	err := fsm.IterShardPlacements(func(bucket, key string, rec PlacementRecord) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, count)

	// CmdPutShardPlacement is now a no-op — no placement rows are written.
	entries := []PutShardPlacementCmd{
		{Bucket: "b1", Key: "k1", NodeIDs: []string{"n0", "n1"}, K: 2, M: 1},
		{Bucket: "b2", Key: "k/with/slashes", NodeIDs: []string{"n2", "n3", "n4"}, K: 3, M: 2},
		{Bucket: "버킷", Key: "한글", NodeIDs: []string{"n0"}, K: 1, M: 1},
	}
	for _, e := range entries {
		raw, _ := EncodeCommand(CmdPutShardPlacement, e)
		require.NoError(t, fsm.Apply(raw))
	}

	seen := make(map[string][]string)
	err = fsm.IterShardPlacements(func(bucket, key string, rec PlacementRecord) error {
		seen[bucket+"/"+key] = rec.Nodes
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, seen, 0)
}

func TestShardPlacementMonitor_Scan_NilShardSvc(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	monitor := NewShardPlacementMonitor(fsm, nil, nil, "node-A", time.Second)
	_, err := monitor.Scan(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard service not configured")
}

func TestShardPlacementMonitor_Scan_NonEnoentError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("chmod 000 has no effect as root")
	}
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, dir := newTestShardService(t)

	const self = "node-A"
	// Seed an EC segment object with self as shard-0 owner.
	segNodes := []string{self, "node-B", "node-C"}
	seedLatestObjectMetaVersion(t, backend, "b", "obj", "v1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: segNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "seg-0", ECData: 2, ECParity: 1, NodeIDs: segNodes},
		},
	})

	// Write the shard then make it unreadable (permission error, not ENOENT).
	// ShardKey for segment: "obj/segments/seg-0"; path: dir/shards/b/obj/segments/seg-0/shard_0
	require.NoError(t, svc.WriteLocalShard("b", "obj/segments/seg-0", 0, []byte("data")))
	shardPath := dir + "/shards/b/obj/segments/seg-0/shard_0"
	require.NoError(t, os.Chmod(shardPath, 0o000))
	t.Cleanup(func() { _ = os.Chmod(shardPath, 0o600) })

	// Non-ENOENT error: shard exists on disk but is unreadable.
	// Scan must not count it as "missing" and must invoke onCorrupt.
	corruptCalled := 0
	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	monitor.SetOnCorrupt(func(target ECShardScanTarget, shardIdx int, err error) {
		corruptCalled++
	})
	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing, "unreadable (non-ENOENT) shard must not count as missing")
	assert.Equal(t, 1, corruptCalled, "non-ENOENT shard error must invoke onCorrupt")
}

func TestShardPlacementMonitor_Scan_ReportsCorruptShard(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, dir := newTestShardService(t)

	const self = "node-A"
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "b",
		Key:         "obj",
		VersionID:   "v1",
		Size:        10,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{self, "node-B", "node-C"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))
	require.NoError(t, svc.WriteLocalShard("b", "obj/v1", 0, []byte("payload")))

	shardPath := dir + "/shards/b/obj/v1/shard_0"
	f, err := os.OpenFile(shardPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	var reported []string
	monitor.SetOnCorrupt(func(target ECShardScanTarget, shardIdx int, err error) {
		reported = append(reported, fmtShardRef(target.Bucket, target.ObjectKey+"/"+target.VersionID, shardIdx))
		require.Error(t, err)
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing)
	assert.Equal(t, []string{"b/obj/v1/0"}, reported)
}

// A chunked object with an EC segment whose local shard (held by this node) is
// absent on disk must surface via onMissing carrying an ECShardSegment target
// with the correct ObjectKey/VersionID/ShardKey/Placement and shardIdx.
func TestShardPlacementMonitor_Scan_DetectsMissingSegmentShard(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	segNodes := []string{self, "node-B", "node-C"}
	seedLatestObjectMetaVersion(t, backend, "b", "chunked", "cv1", objectMeta{
		// Top-level EC fields mirror segment-0; presence of Segments means no
		// object-version target is emitted for this object.
		ECData: 2, ECParity: 1, NodeIDs: segNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "seg-ok", ECData: 2, ECParity: 1, NodeIDs: segNodes},
		},
	})

	// Do NOT write the local shard — self holds shard 0 of the segment, absent on disk.

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	var reported []ECShardScanTarget
	var reportedIdx []int
	monitor.SetOnMissing(func(target ECShardScanTarget, shardIdx int) {
		reported = append(reported, target)
		reportedIdx = append(reportedIdx, shardIdx)
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, missing)
	require.Len(t, reported, 1)
	assert.Equal(t, []int{0}, reportedIdx)
	assert.Equal(t, ECShardScanTarget{
		Kind:      ECShardSegment,
		Bucket:    "b",
		ObjectKey: "chunked",
		VersionID: "cv1",
		ShardKey:  "chunked/segments/seg-ok",
		Placement: PlacementRecord{Nodes: segNodes, K: 2, M: 1},
	}, reported[0])
}

// A chunked object with an EC coalesced ref whose local shard (held by this
// node) is absent on disk must surface via onMissing carrying an
// ECShardCoalesced target with the AUTHORITATIVE (pre-populated) ShardKey —
// not a derived one — plus the right Placement and shardIdx.
func TestShardPlacementMonitor_Scan_DetectsMissingCoalescedShard(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	coalNodes := []string{self, "node-B", "node-C"}
	seedLatestObjectMetaVersion(t, backend, "b", "chunked", "cv1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: coalNodes,
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", ShardKey: "chunked/coalesced/c1", ECData: 2, ECParity: 1, NodeIDs: coalNodes},
		},
	})

	// Do NOT write the local shard — self holds shard 0 of the coalesced blob.

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	var reported []ECShardScanTarget
	var reportedIdx []int
	monitor.SetOnMissing(func(target ECShardScanTarget, shardIdx int) {
		reported = append(reported, target)
		reportedIdx = append(reportedIdx, shardIdx)
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, missing)
	require.Len(t, reported, 1)
	assert.Equal(t, []int{0}, reportedIdx)
	assert.Equal(t, ECShardScanTarget{
		Kind:      ECShardCoalesced,
		Bucket:    "b",
		ObjectKey: "chunked",
		VersionID: "cv1",
		ShardKey:  "chunked/coalesced/c1",
		Placement: PlacementRecord{Nodes: coalNodes, K: 2, M: 1},
	}, reported[0])
}

// A chunked object with an EC segment whose local shard exists on disk but is
// unreadable (non-ENOENT error) must surface via onCorrupt carrying the
// ECShardSegment target. Reuses the corruption mechanism from the
// object-version corrupt test (flip the last byte so the integrity check fails).
func TestShardPlacementMonitor_Scan_ReportsCorruptSegmentShard(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, dir := newTestShardService(t)

	const self = "node-A"
	segNodes := []string{self, "node-B", "node-C"}
	seedLatestObjectMetaVersion(t, backend, "b", "chunked", "cv1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: segNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "seg-ok", ECData: 2, ECParity: 1, NodeIDs: segNodes},
		},
	})

	// Write the segment shard, then corrupt its trailing byte so ReadLocalShard
	// fails with a non-ENOENT integrity error. ShardKey is "chunked/segments/seg-ok".
	require.NoError(t, svc.WriteLocalShard("b", "chunked/segments/seg-ok", 0, []byte("payload")))
	shardPath := dir + "/shards/b/chunked/segments/seg-ok/shard_0"
	f, err := os.OpenFile(shardPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	var reported []ECShardScanTarget
	var reportedIdx []int
	monitor.SetOnCorrupt(func(target ECShardScanTarget, shardIdx int, cerr error) {
		reported = append(reported, target)
		reportedIdx = append(reportedIdx, shardIdx)
		require.Error(t, cerr)
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, missing, "corrupt (non-ENOENT) shard must not count as missing")
	require.Len(t, reported, 1)
	assert.Equal(t, []int{0}, reportedIdx)
	assert.Equal(t, ECShardScanTarget{
		Kind:      ECShardSegment,
		Bucket:    "b",
		ObjectKey: "chunked",
		VersionID: "cv1",
		ShardKey:  "chunked/segments/seg-ok",
		Placement: PlacementRecord{Nodes: segNodes, K: 2, M: 1},
	}, reported[0])
}

func TestShardPlacementMonitor_Start_StopsOnCtxCancel(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	svc, _ := newTestShardService(t)

	monitor := NewShardPlacementMonitor(fsm, nil, svc, "node-A", 10*time.Millisecond)

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	backend := &DistributedBackend{db: db, fsm: fsm}
	svc, _ := newTestShardService(t)

	const self = "node-A"
	// Seed 5 EC segment objects with self as shard-0 owner but NO local shards on
	// disk — so each will fire onMissing. We only cancel after the first.
	for i := 0; i < 5; i++ {
		nodes := []string{self, "node-B", "node-C"}
		seedLatestObjectMetaVersion(t, backend, "b", fmtKey(i), "v1", objectMeta{
			ECData: 2, ECParity: 1, NodeIDs: nodes,
			Segments: []storage.SegmentRef{
				{BlobID: "seg-" + fmtKey(i), ECData: 2, ECParity: 1, NodeIDs: nodes},
			},
		})
	}

	monitor := NewShardPlacementMonitor(fsm, backend, svc, self, time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	// onMissing cancels the context on the first call, simulating mid-repair cancel.
	called := 0
	monitor.SetOnMissing(func(target ECShardScanTarget, shardIdx int) {
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
