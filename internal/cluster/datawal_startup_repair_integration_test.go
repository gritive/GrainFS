package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

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
