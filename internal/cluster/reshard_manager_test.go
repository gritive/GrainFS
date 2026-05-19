package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLeader lets a test flip leader state.
type fakeLeader struct{ leader bool }

func (l *fakeLeader) IsLeader() bool { return l.leader }

// fakeConverter replaces DistributedBackend for reshard manager unit tests.
// It tracks which (bucket,key) pairs were converted/upgraded and can simulate errors.
type fakeConverter struct {
	fsm                *FSM
	active             bool
	converted          []string
	upgraded           []string
	failOn             map[string]error
	shardGroups        map[string]ShardGroupEntry
	lastUpgradeCfg     ECConfig
	currentRingVersion RingVersion
	reshardToRingCalls int
	reshardToRingErr   error
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
func (c *fakeConverter) upgradeObjectEC(_ context.Context, bucket, key string, _ PlacementRecord, cfg ECConfig) error {
	c.upgraded = append(c.upgraded, bucket+"/"+key)
	c.lastUpgradeCfg = cfg
	return nil
}
func (c *fakeConverter) CurrentRingVersion() RingVersion { return c.currentRingVersion }
func (c *fakeConverter) ReshardToRing(_ context.Context, _, _ string, _ RingVersion) error {
	c.reshardToRingCalls++
	return c.reshardToRingErr
}
func (c *fakeConverter) ResolvePlacement(ctx context.Context, bucket, key string, meta PlacementMeta) (ResolvedPlacement, error) {
	return (&DistributedBackend{db: c.fsm.db, fsm: c.fsm}).ResolvePlacement(ctx, bucket, key, meta)
}
func (c *fakeConverter) ShardGroup(id string) (ShardGroupEntry, bool) {
	g, ok := c.shardGroups[id]
	return g, ok
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

func seedObjectMetaEC(t *testing.T, fsm *FSM, bucket, key, etag string, size int64, k, m uint8, nodes []string) {
	t.Helper()
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: size,
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
		ECData: k, ECParity: m, NodeIDs: nodes,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))
}

func seedObjectMetaECInGroup(t *testing.T, fsm *FSM, bucket, key, etag string, size int64, groupID string, k, m uint8, nodes []string) {
	t.Helper()
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: size,
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
		PlacementGroupID: groupID,
		ECData:           k, ECParity: m, NodeIDs: nodes,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))
}

func seedObjectMetaECWithRing(t *testing.T, fsm *FSM, bucket, key, etag string, size int64, k, m uint8, nodes []string, ringVer RingVersion) {
	t.Helper()
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: size,
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
		ECData: k, ECParity: m, NodeIDs: nodes,
		RingVersion: ringVer,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))
}

func TestReshardManager_Run_ConvertsObjectsWithoutPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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

// CmdPutShardPlacement is a no-op; both objects are converted (no skip).
func TestReshardManager_Run_SkipsObjectsWithPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	seedObjectMeta(t, fsm, "bkt", "new", "e1", 10)
	seedObjectMeta(t, fsm, "bkt", "existing", "e2", 20)
	// No-op: CmdPutShardPlacement no longer writes to BadgerDB.
	raw, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "bkt", Key: "existing", NodeIDs: []string{"n0"},
	})
	require.NoError(t, fsm.Apply(raw))

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	// Both objects have no placement record — both are converted.
	assert.Equal(t, 2, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 0, errs)
	assert.ElementsMatch(t, []string{"bkt/new", "bkt/existing"}, conv.converted)
}

func TestReshardManager_Run_SkipsWhenNotLeader(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	s := mgr.Stats()
	assert.Zero(t, s.TotalConverted)
	assert.Zero(t, s.TotalSkipped)
	assert.Zero(t, s.TotalErrors)
	assert.Zero(t, s.TotalRuns)
}

// CmdPutShardPlacement is a no-op; ec-obj has no placement → treated as N× → both convert.
func TestReshardManager_Run_UpgradesECObjects_OnKMismatch(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	seedObjectMeta(t, fsm, "bkt", "nx-obj", "e1", 10)
	seedObjectMeta(t, fsm, "bkt", "ec-obj", "e2", 20)

	// No-op: no placement record written.
	raw, err := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "bkt", Key: "ec-obj",
		NodeIDs: []string{"n0", "n1", "n2"},
		K:       2, M: 1,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	// Both objects have no placement → both are converted as N× objects.
	assert.Equal(t, 2, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 0, errs)
	assert.ElementsMatch(t, []string{"bkt/nx-obj", "bkt/ec-obj"}, conv.converted)
	assert.Empty(t, conv.upgraded)
}

func TestReshardManager_Run_UsesObjectPlacementGroupDesiredProfile(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	seedObjectMetaECInGroup(t, fsm, "bkt", "obj", "e1", 10, "group-1", 2, 1, []string{"n1", "n2", "n3"})

	conv := &fakeConverter{
		fsm:    fsm,
		active: true,
		shardGroups: map[string]ShardGroupEntry{
			"group-1": {ID: "group-1", PeerIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
		},
	}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 1, cv)
	require.Equal(t, 0, skip)
	require.Equal(t, 0, errs)
	require.Equal(t, []string{"bkt/obj"}, conv.upgraded)
	require.Equal(t, ECConfig{DataShards: 4, ParityShards: 2}, conv.lastUpgradeCfg)
}

// TestRingReshardManager_Run_ReshardsMismatchedVersion: ring version이 다른 EC 오브젝트를
// ReshardToRing으로 처리한다.
func TestRingReshardManager_Run_ReshardsMismatchedVersion(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	seedObjectMetaECWithRing(t, fsm, "bkt", "obj", "e1", 10, 4, 2,
		[]string{"n1", "n2", "n3", "n4", "n5", "n6"}, 1)

	conv := &fakeConverter{fsm: fsm, active: true, currentRingVersion: 2}
	mgr := NewRingReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 1, cv)
	require.Equal(t, 0, skip)
	require.Equal(t, 0, errs)
	require.Equal(t, 1, conv.reshardToRingCalls)
}

// TestRingReshardManager_Run_SkipsMatchingVersion: ring version이 같으면 skip한다.
func TestRingReshardManager_Run_SkipsMatchingVersion(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	seedObjectMetaECWithRing(t, fsm, "bkt", "obj", "e1", 10, 4, 2,
		[]string{"n1", "n2", "n3", "n4", "n5", "n6"}, 2)

	conv := &fakeConverter{fsm: fsm, active: true, currentRingVersion: 2}
	mgr := NewRingReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 0, cv)
	require.Equal(t, 1, skip)
	require.Equal(t, 0, errs)
	require.Equal(t, 0, conv.reshardToRingCalls)
}

// TestRingReshardManager_Run_SkipsWhenNoRing: 클러스터에 ring이 없으면(currentRingVersion==0)
// 아무것도 하지 않는다.
func TestRingReshardManager_Run_SkipsWhenNoRing(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	seedObjectMetaEC(t, fsm, "bkt", "obj", "e1", 10, 4, 2,
		[]string{"n1", "n2", "n3", "n4", "n5", "n6"})

	conv := &fakeConverter{fsm: fsm, active: true, currentRingVersion: 0}
	mgr := NewRingReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 0, cv)
	require.Equal(t, 0, skip)
	require.Equal(t, 0, errs)
	require.Equal(t, 0, conv.reshardToRingCalls)
}

// TestRingReshardManager_Run_ReshardLegacyObject: ring 없이 저장된 EC 오브젝트
// (RingVersion==0)도 현재 ring이 있으면 reshard 대상이다.
func TestRingReshardManager_Run_ReshardLegacyObject(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	seedObjectMetaEC(t, fsm, "bkt", "obj", "e1", 10, 4, 2,
		[]string{"n1", "n2", "n3", "n4", "n5", "n6"})

	conv := &fakeConverter{fsm: fsm, active: true, currentRingVersion: 2}
	mgr := NewRingReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 1, cv)
	require.Equal(t, 0, skip)
	require.Equal(t, 0, errs)
	require.Equal(t, 1, conv.reshardToRingCalls)
}

// TestRingReshardManager_Run_ErrorPath: ReshardToRing 실패 시 errs++ totalErrors 증가.
func TestRingReshardManager_Run_ErrorPath(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	seedObjectMetaECWithRing(t, fsm, "bkt", "obj", "e1", 10, 4, 2,
		[]string{"n1", "n2", "n3", "n4", "n5", "n6"}, 1)

	conv := &fakeConverter{
		fsm:                fsm,
		active:             true,
		currentRingVersion: 2,
		reshardToRingErr:   fmt.Errorf("simulated ring reshard failure"),
	}
	mgr := NewRingReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 0, cv)
	require.Equal(t, 0, skip)
	require.Equal(t, 1, errs)
	require.Equal(t, 1, conv.reshardToRingCalls)
	require.Equal(t, uint64(1), mgr.Stats().TotalErrors)
}

// TestRingReshardManager_Run_SkipsNonECObjects: N× 오브젝트(ECData==0)는 ring reshard
// 대상이 아니므로 skip한다.
func TestRingReshardManager_Run_SkipsNonECObjects(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	// N× 오브젝트: ECData/ECParity 없음
	seedObjectMeta(t, fsm, "bkt", "nx-obj", "e1", 10)

	conv := &fakeConverter{fsm: fsm, active: true, currentRingVersion: 2}
	mgr := NewRingReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	require.Equal(t, 0, cv)
	require.Equal(t, 1, skip) // N× 오브젝트는 skip
	require.Equal(t, 0, errs)
	require.Equal(t, 0, conv.reshardToRingCalls)
}

func TestReshardManager_Run_HonorsMaxObjects(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	for i := 0; i < 3; i++ {
		seedObjectMeta(t, fsm, "bkt", fmt.Sprintf("obj-%d", i), "e", 10)
	}

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	mgr.maxObjectsPerRun = 2
	cv, _, _ := mgr.Run(context.Background())

	require.Equal(t, 2, cv)
}

// CmdPutShardPlacement is a no-op; ec-obj has no placement → treated as N× → converted.
func TestReshardManager_Run_SkipsECObjects_OnKMatch(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	seedObjectMeta(t, fsm, "bkt", "ec-obj", "e1", 10)

	// No-op: no placement record written.
	raw, err := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "bkt", Key: "ec-obj",
		NodeIDs: []string{"n0", "n1", "n2", "n3", "n4", "n5"},
		K:       4, M: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	// No placement → treated as N× → converted (not skipped).
	assert.Equal(t, 1, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 0, errs)
	assert.Empty(t, conv.upgraded)
}

func TestReshardManager_Run_SkipsMetadataOnlyECObjects_OnKMatch(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	seedObjectMetaEC(t, fsm, "bkt", "ec-obj", "e1", 10, 4, 2, []string{"n0", "n1", "n2", "n3", "n4", "n5"})

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Equal(t, 0, cv)
	assert.Equal(t, 1, skip)
	assert.Equal(t, 0, errs)
	assert.Empty(t, conv.converted)
	assert.Empty(t, conv.upgraded)
}

func TestReshardManager_Run_DoesNotConvertCorruptPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	seedObjectMetaEC(t, fsm, "bkt", "bad-ec", "e1", 10, 4, 2, []string{"n0"})

	conv := &fakeConverter{fsm: fsm, active: true}
	mgr := NewReshardManager(conv, &fakeLeader{leader: true}, time.Minute)
	cv, skip, errs := mgr.Run(context.Background())

	assert.Equal(t, 0, cv)
	assert.Equal(t, 0, skip)
	assert.Equal(t, 1, errs)
	assert.Empty(t, conv.converted)
	assert.Empty(t, conv.upgraded)
}

// TestUpgradeObjectEC_RoundTrip tests upgradeObjectEC end-to-end against the
// current ObjectMeta placement model.
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
	require.NoError(t, backend.CreateBucket(context.Background(), bucket))

	// Seed old metadata directly into the FSM (no versionID → legacy layout).
	etag := fmt.Sprintf("%x", md5.Sum(data))
	oldCfg := ECConfig{DataShards: 2, ParityShards: 1}
	oldNodes := make([]string, oldCfg.NumShards())
	for i := range oldNodes {
		oldNodes[i] = "self"
	}
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: int64(len(data)),
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
		ECData: 2, ECParity: 1, NodeIDs: oldNodes,
	})
	require.NoError(t, err)
	require.NoError(t, backend.fsm.Apply(raw))

	// Write old shards (k=2, m=1 → 3 shards) directly to local storage.
	oldShards, err := ECSplit(oldCfg, data)
	require.NoError(t, err)
	for i, shard := range oldShards {
		require.NoError(t, svc.WriteLocalShard(bucket, key, i, shard))
	}

	_, oldMeta, err := backend.headObjectMeta(context.Background(), bucket, key)
	require.NoError(t, err)
	oldResolved, err := backend.ResolvePlacement(context.Background(), bucket, key, oldMeta)
	require.NoError(t, err)
	require.Equal(t, PlacementSourceMetadata, oldResolved.Source)
	require.Equal(t, oldCfg.NumShards(), len(oldResolved.Record.Nodes))
	require.Equal(t, oldCfg.DataShards, oldResolved.Record.K)
	require.Equal(t, oldCfg.ParityShards, oldResolved.Record.M)

	// Upgrade from k=2,m=1 to k=3,m=2.
	newCfg := ECConfig{DataShards: 3, ParityShards: 2}
	ctx := context.Background()
	require.NoError(t, backend.upgradeObjectEC(ctx, bucket, key, oldResolved.Record, newCfg))

	_, newMeta, err := backend.headObjectMeta(context.Background(), bucket, key)
	require.NoError(t, err)
	newResolved, err := backend.ResolvePlacement(context.Background(), bucket, key, newMeta)
	require.NoError(t, err)
	assert.Equal(t, PlacementSourceMetadata, newResolved.Source)
	assert.Equal(t, newCfg.NumShards(), len(newResolved.Record.Nodes), "new placement shard count")
	assert.Equal(t, newCfg.DataShards, newResolved.Record.K)
	assert.Equal(t, newCfg.ParityShards, newResolved.Record.M)

	// Verify the object is still fully readable after the upgrade.
	rc, obj, err := backend.GetObject(context.Background(), bucket, key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got, "object content must survive EC upgrade")
}

// TestUpgradeObjectEC_PreservesTags is the regression guard for the v0.0.265.0
// Phase 2 fix: extending PutObjectMetaCmd with Tags made upgradeObjectEC's
// previously-safe propose into a tag-clobber path. applyPutObjectMeta writes
// c.Tags unconditionally, so upgradeObjectEC must forward existing tags.
func TestUpgradeObjectEC_PreservesTags(t *testing.T) {
	backend := NewSingletonBackendForTest(t)

	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil)
	backend.SetShardService(svc, []string{"self"})
	backend.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})

	bucket, key := "tagbkt", "tags/obj"
	data := bytes.Repeat([]byte("grainfs-ec-tags-"), 200)

	require.NoError(t, backend.CreateBucket(context.Background(), bucket))

	// Seed old metadata with tags directly into the FSM.
	etag := fmt.Sprintf("%x", md5.Sum(data))
	oldCfg := ECConfig{DataShards: 2, ParityShards: 1}
	oldNodes := make([]string, oldCfg.NumShards())
	for i := range oldNodes {
		oldNodes[i] = "self"
	}
	seededTags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "owner", Value: "alice"},
	}
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, Size: int64(len(data)),
		ContentType: "application/octet-stream", ETag: etag, ModTime: 1,
		ECData: 2, ECParity: 1, NodeIDs: oldNodes,
		Tags: seededTags,
	})
	require.NoError(t, err)
	require.NoError(t, backend.fsm.Apply(raw))

	// Sanity-check the seed: tags are readable before the upgrade.
	preTags, err := backend.GetObjectTags(bucket, key, "")
	require.NoError(t, err)
	require.Len(t, preTags, 2, "seed tags must be present before upgrade")

	// Write old shards so upgradeObjectEC can reconstruct the object.
	oldShards, err := ECSplit(oldCfg, data)
	require.NoError(t, err)
	for i, shard := range oldShards {
		require.NoError(t, svc.WriteLocalShard(bucket, key, i, shard))
	}

	_, oldMeta, err := backend.headObjectMeta(context.Background(), bucket, key)
	require.NoError(t, err)
	oldResolved, err := backend.ResolvePlacement(context.Background(), bucket, key, oldMeta)
	require.NoError(t, err)

	// Trigger the EC config upgrade.
	newCfg := ECConfig{DataShards: 3, ParityShards: 2}
	ctx := context.Background()
	require.NoError(t, backend.upgradeObjectEC(ctx, bucket, key, oldResolved.Record, newCfg))

	// Regression assertion: tags must survive the upgrade.
	postTags, err := backend.GetObjectTags(bucket, key, "")
	require.NoError(t, err)
	require.Len(t, postTags, 2, "tags must be preserved after EC upgrade")
	gotKeys := map[string]string{}
	for _, tag := range postTags {
		gotKeys[tag.Key] = tag.Value
	}
	assert.Equal(t, "prod", gotKeys["env"])
	assert.Equal(t, "alice", gotKeys["owner"])
}
