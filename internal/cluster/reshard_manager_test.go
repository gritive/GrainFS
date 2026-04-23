package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLeader lets a test flip leader state.
type fakeLeader struct{ leader bool }

func (l *fakeLeader) IsLeader() bool { return l.leader }

// fakeConverter replaces DistributedBackend for reshard manager unit tests.
// It tracks which (bucket,key) pairs were converted/upgraded and can simulate errors.
type fakeConverter struct {
	fsm       *FSM
	active    bool
	converted []string
	upgraded  []string
	failOn    map[string]error
}

func (c *fakeConverter) ConvertObjectToEC(ctx context.Context, bucket, key string) error {
	ref := bucket + "/" + key
	if c.failOn != nil {
		if err, ok := c.failOn[ref]; ok {
			return err
		}
	}
	// Simulate a successful convert: stamp a placement so subsequent scans skip it.
	raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: bucket, Key: key, NodeIDs: []string{"n0", "n1", "n2", "n3", "n4"},
	})
	_ = c.fsm.Apply(raw)
	c.converted = append(c.converted, ref)
	return nil
}

func (c *fakeConverter) FSMRef() *FSM   { return c.fsm }
func (c *fakeConverter) ECActive() bool { return c.active }
func (c *fakeConverter) EffectiveECConfig() ECConfig {
	return ECConfig{DataShards: 4, ParityShards: 2}
}
func (c *fakeConverter) upgradeObjectEC(_ context.Context, bucket, key string, _ PlacementRecord, _ ECConfig) error {
	c.upgraded = append(c.upgraded, bucket+"/"+key)
	return nil
}

// seedObjectMeta writes an object metadata record directly without going
// through the full PutObject path — suitable for reshard manager tests.
func seedObjectMeta(t *testing.T, fsm *FSM, bucket, key, etag string, size int64) {
	t.Helper()
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: size,
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))
}

func TestReshardManager_Run_ConvertsObjectsWithoutPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Seed three objects, all N× (no placement).
	seedObjectMeta(t, fsm, "bkt", "obj1", "e1", 10)
	seedObjectMeta(t, fsm, "bkt", "obj2", "e2", 20)
	seedObjectMeta(t, fsm, "bkt", "obj3", "e3", 30)

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Equal(t, 3, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 0, errs)
	assert.ElementsMatch(t, []string{"bkt/obj1", "bkt/obj2", "bkt/obj3"}, conv.converted)
}

func TestReshardManager_Run_SkipsObjectsWithPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	seedObjectMeta(t, fsm, "bkt", "new", "e1", 10)
	seedObjectMeta(t, fsm, "bkt", "existing", "e2", 20)
	// "existing" already has placement — reshard should skip it.
	raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "bkt", Key: "existing", NodeIDs: []string{"n0"},
	})
	require.NoError(t, fsm.Apply(raw))

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Equal(t, 1, cv)
	assert.Equal(t, 1, skip)
	assert.Equal(t, 0, errs)
	assert.Equal(t, []string{"bkt/new"}, conv.converted)
}

func TestReshardManager_Run_SkipsWhenNotLeader(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	seedObjectMeta(t, fsm, "bkt", "obj", "e1", 10)

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: false}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Zero(t, cv)
	assert.Zero(t, skip)
	assert.Zero(t, errs)
	assert.Empty(t, conv.converted)
}

func TestReshardManager_Run_SkipsWhenECInactive(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	seedObjectMeta(t, fsm, "bkt", "obj", "e1", 10)

	conv := &fakeConverter{fsm: fsm, active: false} // EC not active
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Zero(t, cv)
	assert.Zero(t, skip)
	assert.Zero(t, errs)
}

func TestReshardManager_Run_ContinuesOnConvertError(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	seedObjectMeta(t, fsm, "bkt", "good1", "e1", 10)
	seedObjectMeta(t, fsm, "bkt", "bad", "e2", 20)
	seedObjectMeta(t, fsm, "bkt", "good2", "e3", 30)

	conv := &fakeConverter{
		fsm:    fsm,
		active: true,
		failOn: map[string]error{"bkt/bad": fmt.Errorf("simulated write failure")},
	}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Equal(t, 2, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 1, errs)
	assert.ElementsMatch(t, []string{"bkt/good1", "bkt/good2"}, conv.converted)

	stats := mgr.Stats()
	assert.Equal(t, uint64(2), stats.TotalConverted)
	assert.Equal(t, uint64(1), stats.TotalErrors)
	assert.Equal(t, uint64(1), stats.TotalRuns)
}

func TestReshardManager_Run_ContextCancel(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	for i := 0; i < 50; i++ {
		seedObjectMeta(t, fsm, "b", fmt.Sprintf("obj-%d", i), fmt.Sprintf("e-%d", i), 1)
	}
	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	_, _, _ = mgr.Run(ctx)
	// Just verify no panic.
}

func TestReshardManager_Stats_InitialState(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	s := mgr.Stats()
	assert.Zero(t, s.TotalConverted)
	assert.Zero(t, s.TotalSkipped)
	assert.Zero(t, s.TotalErrors)
	assert.Zero(t, s.TotalRuns)
}

func TestReshardManager_Run_UpgradesECObjects_OnKMismatch(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Seed one N×-replicated object (no placement) and one EC object
	// whose stored k,m differs from the effective config.
	seedObjectMeta(t, fsm, "bkt", "nx-obj", "e1", 10)
	seedObjectMeta(t, fsm, "bkt", "ec-obj", "e2", 20)

	// ec-obj has placement with k=2,m=1, but effective config is k=4,m=2.
	raw, err := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "bkt", Key: "ec-obj",
		NodeIDs: []string{"n0", "n1", "n2"}, // k+m = 3 shards
		K: 2, M: 1,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	conv := &fakeConverter{
		fsm:    fsm,
		active: true,
		// EffectiveECConfig returns {4,2} — mismatch with stored {2,1}.
	}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	// nx-obj → ConvertObjectToEC; ec-obj (k mismatch) → upgradeObjectEC
	assert.Equal(t, 2, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 0, errs)
	assert.Equal(t, []string{"bkt/nx-obj"}, conv.converted)
	assert.Equal(t, []string{"bkt/ec-obj"}, conv.upgraded)
}

func TestReshardManager_Run_SkipsECObjects_OnKMatch(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	seedObjectMeta(t, fsm, "bkt", "ec-obj", "e1", 10)

	// ec-obj placement k,m matches effective config {4,2} exactly.
	raw, err := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "bkt", Key: "ec-obj",
		NodeIDs: []string{"n0", "n1", "n2", "n3", "n4", "n5"},
		K: 4, M: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Equal(t, 0, cv)
	assert.Equal(t, 1, skip)
	assert.Equal(t, 0, errs)
	assert.Empty(t, conv.upgraded)
}

// TestUpgradeObjectEC_RoundTrip tests upgradeObjectEC end-to-end on a real
// single-node backend: write shards with k=2,m=1; upgrade to k=3,m=2; verify
// the object is still readable after the upgrade.
func TestUpgradeObjectEC_RoundTrip(t *testing.T) {
	backend := NewSingletonBackendForTest(t)

	// Wire a local-only ShardService (nil transport — no remote calls needed).
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil)
	backend.SetShardService(svc, []string{"self"})
	backend.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})

	bucket, key := "testbkt", "roundtrip/obj"
	data := bytes.Repeat([]byte("grainfs-ec-upgrade-"), 200) // 3800 bytes

	// Create the bucket through Raft so HeadBucket passes.
	require.NoError(t, backend.CreateBucket(bucket))

	// Seed object metadata directly into the FSM (no versionID → legacy layout).
	etag := fmt.Sprintf("%x", md5.Sum(data))
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: int64(len(data)),
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
	})
	require.NoError(t, err)
	require.NoError(t, backend.fsm.Apply(raw))

	// Write old shards (k=2, m=1 → 3 shards) directly to local storage.
	oldCfg := ECConfig{DataShards: 2, ParityShards: 1}
	oldShards, err := ECSplit(oldCfg, data)
	require.NoError(t, err)
	for i, shard := range oldShards {
		require.NoError(t, svc.WriteLocalShard(bucket, key, i, shard))
	}

	// Seed old placement record (k=2,m=1, all 3 shards on "self").
	selfNodes := make([]string, oldCfg.NumShards())
	for i := range selfNodes {
		selfNodes[i] = "self"
	}
	raw, err = EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: bucket, Key: key, NodeIDs: selfNodes, K: 2, M: 1,
	})
	require.NoError(t, err)
	require.NoError(t, backend.fsm.Apply(raw))

	// Confirm the old placement is readable before upgrade.
	oldRec, err := backend.fsm.LookupShardPlacement(bucket, key)
	require.NoError(t, err)
	require.Equal(t, 3, len(oldRec.Nodes))
	require.Equal(t, 2, oldRec.K)
	require.Equal(t, 1, oldRec.M)

	// Upgrade from k=2,m=1 to k=3,m=2.
	newCfg := ECConfig{DataShards: 3, ParityShards: 2}
	ctx := context.Background()
	require.NoError(t, backend.upgradeObjectEC(ctx, bucket, key, oldRec, newCfg))

	// Verify new placement record was committed via Raft.
	newRec, err := backend.fsm.LookupShardPlacement(bucket, key)
	require.NoError(t, err)
	assert.Equal(t, newCfg.NumShards(), len(newRec.Nodes), "new placement shard count")
	assert.Equal(t, 3, newRec.K)
	assert.Equal(t, 2, newRec.M)

	// Verify the object is still fully readable after the upgrade.
	rc, obj, err := backend.GetObject(bucket, key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got, "object content must survive EC upgrade")
}
