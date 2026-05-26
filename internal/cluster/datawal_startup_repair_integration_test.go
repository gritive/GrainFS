package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/datawal"
)

func TestDataWALStartupRepair_DiscoversAndRepairsMissingShard(t *testing.T) {
	shardDir := t.TempDir()
	// WAL must live where RecoverDataWAL replays from: filepath.Dir(dataDirs[0])
	// == shardDir (dataDirs[0] is shardDir/shards). withTestWAL would NOT work.
	dwal, err := datawal.Open(filepath.Join(shardDir, "datawal"), nil)
	require.NoError(t, err)
	collector := NewDataWALRepairCollector()
	svc := NewShardService(shardDir, nil, WithDataWAL(dwal), WithDataWALRepairSink(collector))

	backend := NewSingletonBackendForTest(t)
	const selfAddr = "self"
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr} // 1+1, all shards local
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})

	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	content := bytes.Repeat([]byte("startup-repair-ec-block-"), 1<<17) // > 2MB so shard 0 is metadata-only
	obj, err := backend.PutObject(context.Background(), "b", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	shardKey := "obj/" + obj.VersionID
	info, err := os.Stat(svc.getShardPath("b", shardKey, 0))
	require.NoError(t, err)
	// PutObject already wrote one metadata-only OpShardPut record for shard 0
	// (>1MiB shard => not inlined). Append a second record for the same shard to
	// also exercise the collector's dedup path, then remove the local shard so
	// replay must classify it as a repair candidate.
	_, err = dwal.Append(context.Background(), datawal.Record{
		Op: datawal.OpShardPut, Bucket: "b", Key: shardKey, Target: "0", Size: info.Size(),
	})
	require.NoError(t, err)
	require.NoError(t, dwal.Flush())
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 0)))

	// Replay the WAL: metadata-only record for the now-missing shard must enqueue
	// exactly one repair candidate carrying the physical shard identity.
	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	cands := collector.Candidates()
	require.Len(t, cands, 1)
	require.Equal(t, "b", cands[0].Bucket)
	require.Equal(t, shardKey, cands[0].ShardKey)
	require.Equal(t, 0, cands[0].ShardIdx)
	require.Equal(t, DataWALRepairMissing, cands[0].Reason)

	// Drive the repair the startup worker would run from this candidate. No
	// scrubber/placement monitor is started in this test, proving startup repair
	// works with periodic scrub disabled.
	require.NoError(t, backend.RepairShardLocalWithIncident(t.Context(), IncidentRepairRequest{
		Bucket:    cands[0].Bucket,
		Key:       "obj",
		VersionID: obj.VersionID,
		ShardIdx:  cands[0].ShardIdx,
	}))

	rc, _, err := backend.GetObject(context.Background(), "b", "obj")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// TestDataWALStartupRepair_DiscoversAndRepairsMissingSegmentShard proves the
// startup data WAL repair pipeline reconstructs a SEGMENT EC shard (shardKey
// "<key>/segments/<blobID>"), not just an object-version shard. A chunked
// PutObject (object > chunk threshold, ShardGroupSource wired) routes through
// putObjectChunked, which writes per-segment EC shards and emits one
// metadata-only OpShardPut per shard (shard payload >= the 1 MiB WAL inline
// threshold). We remove one segment shard, replay the WAL, and drive the exact
// resolve→repair flow the serveruntime startup worker runs.
func TestDataWALStartupRepair_DiscoversAndRepairsMissingSegmentShard(t *testing.T) {
	shardDir := t.TempDir()
	// WAL must live where RecoverDataWAL replays from: filepath.Dir(dataDirs[0])
	// == shardDir (dataDirs[0] is shardDir/shards).
	dwal, err := datawal.Open(filepath.Join(shardDir, "datawal"), nil)
	require.NoError(t, err)
	collector := NewDataWALRepairCollector()
	svc := NewShardService(shardDir, nil, WithDataWAL(dwal), WithDataWALRepairSink(collector))

	backend := NewSingletonBackendForTest(t)
	const selfAddr = "self"
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr} // 1+1, all shards local
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	// Small chunk size flips BOTH the chunked-PUT routing threshold and the
	// SegmentWriter chunker (effectiveChunkedPutChunkSize feeds both). With k=1
	// the per-segment data shard ≈ chunkSize (2 MiB) > 1 MiB WAL inline
	// threshold, so each shard is logged metadata-only. The test repairs
	// segment 0, whose full 2 MiB data shard is solidly above the 1 MiB inline
	// threshold; keep content > chunkSize so segment 0 is a full 2 MiB chunk.
	const chunkSize = 2 << 20
	backend.chunkedPutChunkSize = chunkSize
	// ShardGroupSource is required for putObjectChunked (SelectSegmentPlacementGroup).
	// Two "self" peers ⇒ DesiredECConfigForGroup == {1,1} ⇒ group IsActive, and
	// PlacementForNodes maps every shard index to "self" (all local).
	backend.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-0": {ID: "group-0", PeerIDs: []string{selfAddr, selfAddr}},
	}})

	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	// 3 MiB > 2 MiB chunk ⇒ 2 segments, each EC-split into 2 shards (k=1,m=1).
	content := bytes.Repeat([]byte("startup-repair-segment-block-"), (3<<20)/29+1)
	obj, err := backend.PutObject(context.Background(), "b", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	require.NoError(t, dwal.Flush())

	// The chunked PUT EC-wrote per-segment shards on disk and logged a
	// metadata-only OpShardPut for each. Discover a real segment shardKey from
	// the written files, then remove its shard 0 so WAL replay classifies it as
	// MISSING.
	segShardKey := firstSegmentShardKeyOnDisk(t, svc)
	require.Contains(t, segShardKey, "/segments/", "must target the segment path, not object-version")
	require.NoError(t, os.Remove(svc.getShardPath("b", segShardKey, 0)))

	require.NoError(t, svc.RecoverDataWAL(context.Background()))

	// Only the removed shard is missing; every other segment shard is present,
	// so replay must enqueue exactly one candidate.
	require.Len(t, collector.Candidates(), 1)
	cand := collector.Candidates()[0]
	require.Equal(t, "b", cand.Bucket)
	require.Equal(t, segShardKey, cand.ShardKey)
	require.Equal(t, 0, cand.ShardIdx)
	require.Contains(t, cand.ShardKey, "/segments/")
	require.Equal(t, DataWALRepairMissing, cand.Reason)

	objectKey, _, _ := ClassifyStartupRepairShardKey(cand.ShardKey)
	rec, skipReason, err := backend.ResolveShardKeyPlacement(context.Background(), "b", cand.ShardKey, NewShardKeyPlacementScanCache())
	require.NoError(t, err)
	require.Empty(t, skipReason)
	require.NotEmpty(t, rec.Nodes)

	require.NoError(t, backend.RepairShardLocalWithIncident(t.Context(), IncidentRepairRequest{
		Bucket:    cand.Bucket,
		Key:       objectKey,
		ShardKey:  cand.ShardKey,
		Placement: rec,
		ShardIdx:  cand.ShardIdx,
	}))

	rc, _, err := backend.GetObject(context.Background(), "b", "obj")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// TestShardPlacementMonitor_RepairsMissingSegmentShard_EndToEnd proves the full
// continuous-scrub loop: a real chunked PutObject writes per-segment EC shards,
// one segment shard is lost on disk, the periodic ShardPlacementMonitor.Scan
// DETECTS it missing (firing onMissing with an ECShardSegment target carrying the
// authoritative ShardKey/Placement), the repair callback (mirroring the Task 4
// serveruntime boot wiring) RECONSTRUCTS it via RepairShardLocalWithIncident, and
// GetObject then returns the original bytes — proving end-to-end the scrub monitor
// heals a lost segment shard with no startup-WAL involvement.
func TestShardPlacementMonitor_RepairsMissingSegmentShard_EndToEnd(t *testing.T) {
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil, withTestWAL(t))

	backend := NewSingletonBackendForTest(t)
	const selfAddr = "self"
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr} // 1+1, all shards local
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	// Small chunk size flips both the chunked-PUT routing threshold and the
	// SegmentWriter chunker. With k=1 the per-segment data shard ≈ chunkSize.
	const chunkSize = 2 << 20
	backend.chunkedPutChunkSize = chunkSize
	// ShardGroupSource is required for putObjectChunked. Two "self" peers ⇒
	// DesiredECConfigForGroup == {1,1}, group IsActive, every shard index maps to
	// "self" (all local).
	backend.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-0": {ID: "group-0", PeerIDs: []string{selfAddr, selfAddr}},
	}})

	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	// 3 MiB > 2 MiB chunk ⇒ 2 segments, each EC-split into 2 shards (k=1,m=1).
	content := bytes.Repeat([]byte("scrub-monitor-segment-repair-"), (3<<20)/29+1)
	obj, err := backend.PutObject(context.Background(), "b", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	// The chunked PUT EC-wrote real per-segment shards on disk. Pick one and
	// remove its shard 0 so the monitor must classify it as locally missing.
	segShardKey := firstSegmentShardKeyOnDisk(t, svc)
	require.Contains(t, segShardKey, "/segments/", "must target the segment path, not object-version")
	_, err = os.Stat(svc.getShardPath("b", segShardKey, 0))
	require.NoError(t, err, "segment shard 0 must exist before removal")
	require.NoError(t, os.Remove(svc.getShardPath("b", segShardKey, 0)))

	// Construct the monitor over the SAME FSM the chunked PutObject wrote object
	// meta into (backend.fsm), with the backend as resolver and shardSvc.
	monitor := NewShardPlacementMonitor(backend.FSMRef(), backend, svc, selfAddr, time.Second)

	var reported []ECShardScanTarget
	var reportedIdx []int
	monitor.SetOnMissing(func(target ECShardScanTarget, shardIdx int) {
		reported = append(reported, target)
		reportedIdx = append(reportedIdx, shardIdx)
		// Mirror the Task 4 serveruntime boot wiring (boot_phases_scrubber.go):
		// segment/coalesced targets repair off the authoritative ShardKey +
		// Placement. Recorder is nil in this focused test.
		repairReq := IncidentRepairRequest{
			Bucket:    target.Bucket,
			Key:       target.ObjectKey,
			VersionID: target.VersionID,
			ShardIdx:  shardIdx,
		}
		switch target.Kind {
		case ECShardSegment, ECShardCoalesced:
			repairReq.ShardKey = target.ShardKey
			repairReq.Placement = target.Placement
		}
		require.NoError(t, backend.RepairShardLocalWithIncident(context.Background(), repairReq))
	})

	missing, err := monitor.Scan(context.Background())
	require.NoError(t, err)

	// Exactly the one removed segment shard must be detected missing — every other
	// segment shard is still on disk, so a duplicate/extra fire would be a bug.
	assert.Equal(t, 1, missing)
	require.Len(t, reported, 1)
	assert.Equal(t, []int{0}, reportedIdx)
	assert.Equal(t, ECShardSegment, reported[0].Kind)
	assert.Equal(t, "b", reported[0].Bucket)
	assert.Equal(t, "obj", reported[0].ObjectKey)
	assert.Equal(t, segShardKey, reported[0].ShardKey)
	assert.Contains(t, reported[0].ShardKey, "/segments/")
	require.NotEmpty(t, reported[0].Placement.Nodes)

	// The repair callback reconstructed shard 0; it must be back on disk.
	_, err = os.Stat(svc.getShardPath("b", segShardKey, 0))
	require.NoError(t, err, "segment shard 0 must exist again after repair")

	// End-to-end proof: the object reads back byte-for-byte after the scrub repair.
	rc, _, err := backend.GetObject(context.Background(), "b", "obj")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// firstSegmentShardKeyOnDisk returns the "obj/segments/<blobID>" shard key of
// the first segment blob the chunked PUT wrote under bucket "b", object "obj".
func firstSegmentShardKeyOnDisk(t *testing.T, svc *ShardService) string {
	t.Helper()
	dir := svc.getShardDir("b", "obj/segments", 0) // .../shards/b/obj/segments
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "chunked PUT must have written segment shards")
	return "obj/segments/" + entries[0].Name()
}

// TestDataWALStartupRepair_DiscoversAndRepairsMissingCoalescedShard proves the
// startup repair pipeline reconstructs a COALESCED EC shard
// ("<key>/coalesced/<id>").
//
// Synthetic seeding rationale: a genuine coalesced shard is produced only by the
// background coalesce worker after enough appends accumulate (maybeTriggerCoalesce
// → CmdCoalesceSegments raft entry), which does not fit a focused, deterministic
// integration test. Per the task's escape clause we instead (1) seed a real
// objectMeta carrying a populated Coalesced[] entry whose ShardKey/NodeIDs match
// the physical shards, and (2) write the corresponding coalesced EC shards to the
// real ShardService plus a metadata-only OpShardPut to the data WAL. This keeps
// ResolveShardKeyPlacement, RecoverDataWAL, and RepairShardAtShardKey running
// against real on-disk shard files. GetObject is not used to verify (a hand-seeded
// objectMeta lacks the full coalesced read manifest); instead we assert the
// rebuilt shard bytes equal the canonical EC split, exactly as
// TestRepairShardAtShardKey_SegmentKey does.
func TestDataWALStartupRepair_DiscoversAndRepairsMissingCoalescedShard(t *testing.T) {
	shardDir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(shardDir, "datawal"), nil)
	require.NoError(t, err)
	collector := NewDataWALRepairCollector()
	svc := NewShardService(shardDir, nil, WithDataWAL(dwal), WithDataWALRepairSink(collector))

	backend := NewSingletonBackendForTest(t)
	const selfAddr = "self"
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})

	require.NoError(t, backend.CreateBucket(context.Background(), "b"))

	const coalescedID = "c-0001"
	shardKey := "obj/coalesced/" + coalescedID
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	coalNodes := []string{selfAddr, selfAddr}

	// Build the coalesced EC stripe directly at the coalesced shard key and write
	// both shards locally, logging a metadata-only OpShardPut for each (payload
	// >= 1 MiB WAL inline threshold) so startup replay sees them.
	content := bytes.Repeat([]byte("startup-repair-coalesced-block-"), (2<<20)/31+1)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
		// WAL Size must match the on-disk shard size (which includes encryption
		// framing), else replay flags a false size_mismatch on the surviving shard.
		info, statErr := os.Stat(svc.getShardPath("b", shardKey, i))
		require.NoError(t, statErr)
		_, err = dwal.Append(context.Background(), datawal.Record{
			Op: datawal.OpShardPut, Bucket: "b", Key: shardKey, Target: strconv.Itoa(i), Size: info.Size(),
		})
		require.NoError(t, err)
	}
	require.NoError(t, dwal.Flush())

	// Seed a real objectMeta whose Coalesced[] entry matches the physical shards,
	// so ResolveShardKeyPlacement recovers the placement by scanning object meta.
	seedObjectMetaVersion(t, backend, "b", "obj", "v1", objectMeta{
		Coalesced: []CoalescedShardRef{{
			CoalescedID: coalescedID,
			ShardKey:    shardKey,
			ECData:      uint8(cfg.DataShards),
			ECParity:    uint8(cfg.ParityShards),
			NodeIDs:     coalNodes,
		}},
	})

	// Remove shard 0 so replay classifies it as a repair candidate.
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 0)))

	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	// Only shard 0 was removed; shard 1 is present, so replay must enqueue
	// exactly one candidate.
	require.Len(t, collector.Candidates(), 1)
	cand := collector.Candidates()[0]
	require.Equal(t, "b", cand.Bucket)
	require.Equal(t, shardKey, cand.ShardKey)
	require.Equal(t, 0, cand.ShardIdx)
	require.Contains(t, cand.ShardKey, "/coalesced/")
	require.Equal(t, DataWALRepairMissing, cand.Reason)

	objectKey, _, _ := ClassifyStartupRepairShardKey(cand.ShardKey)
	rec, skipReason, err := backend.ResolveShardKeyPlacement(context.Background(), "b", cand.ShardKey, NewShardKeyPlacementScanCache())
	require.NoError(t, err)
	require.Empty(t, skipReason)
	require.NotEmpty(t, rec.Nodes)

	require.NoError(t, backend.RepairShardLocalWithIncident(t.Context(), IncidentRepairRequest{
		Bucket:    cand.Bucket,
		Key:       objectKey,
		ShardKey:  cand.ShardKey,
		Placement: rec,
		ShardIdx:  cand.ShardIdx,
	}))

	// Hand-seeded objectMeta lacks a coalesced read manifest, so verify by reading
	// the rebuilt shard directly (mirrors TestRepairShardAtShardKey_SegmentKey).
	rebuilt, err := svc.ReadLocalShard("b", shardKey, 0)
	require.NoError(t, err)
	require.Equal(t, freshShards[0], rebuilt)
}
