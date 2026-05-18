package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCoalesceCrashRecovery exercises the recovery path when a coalesce
// job dies between EC write and FSM propose:
//
//  1. 16 appends drive the coalesce trigger.
//  2. The first worker run aborts via coalesceFaultAfterECWrite — EC shards
//     under shardSvc exist but the metadata never gets the CoalescedShardRef.
//     Raw segments + owner-local merged file remain on disk.
//  3. Worker re-runs (simulating backstop scan / restart). With the fault
//     hook cleared, the job completes normally — different CoalescedID,
//     consumes the same raw segments, propose succeeds.
//  4. GetObject returns the original body via EC reconstruct.
//
// Orphan EC shards from step 2 stay on disk and would be reaped by the
// scrubber in production; the test does not assert on those.
func TestCoalesceCrashRecovery(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	svc := NewShardService(b.root, nil)
	b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})

	// Disable in-process trigger so we drive the worker manually.
	crashCfg := *b.coalesceCfg.Load()
	crashCfg.SegmentCount = 1 << 30
	crashCfg.SizeBytes = 1 << 30
	crashCfg.IdleTimeout = time.Hour
	b.SetCoalesceConfig(crashCfg)

	bucket, key := "b", "k"
	var off int64
	var expected []byte
	for i := 0; i < 16; i++ {
		chunk := []byte(fmt.Sprintf("c%02d-", i))
		_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader(chunk))
		require.NoError(t, err)
		off += int64(len(chunk))
		expected = append(expected, chunk...)
	}

	// Step 2: arm fault hook → first attempt aborts after EC write.
	var faulted atomic.Bool
	b.coalesceFaultAfterECWrite = func() error {
		faulted.Store(true)
		return fmt.Errorf("simulated crash")
	}
	err := b.processCoalesceJobB3(ctx, coalesceJob{Bucket: bucket, Key: key})
	require.Error(t, err)
	require.True(t, faulted.Load())

	// State after crash: metadata still shows all 16 raw segments, no
	// CoalescedShardRef.
	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Empty(t, obj.Coalesced)
	require.Len(t, obj.Segments, 16)

	// Step 3: clear fault hook, re-run worker — should succeed.
	b.coalesceFaultAfterECWrite = nil
	require.NoError(t, b.processCoalesceJobB3(ctx, coalesceJob{Bucket: bucket, Key: key}))

	obj, err = b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Len(t, obj.Coalesced, 1)
	require.Empty(t, obj.Segments)

	// Step 4: full-body GET via EC reconstruct.
	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, expected, body)
}
