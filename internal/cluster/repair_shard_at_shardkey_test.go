// repair_shard_at_shardkey_test.go: RepairShardAtShardKey reconstructs a local
// shard for an arbitrary physical shard key (here a segment-style key) given an
// already-resolved EC placement, bypassing object-version placement resolution.

package cluster

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepairShardAtShardKey_SegmentKey(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc

	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	// Build an EC stripe (k=1, m=1) directly at a segment-style shard key,
	// writing each shard locally. All placement nodes are "self" so reads/writes
	// are local. We deliberately do NOT go through PutObject/placement records —
	// RepairShardAtShardKey must work off the supplied PlacementRecord alone.
	const shardKey = "obj/segments/seg-blob-0001"
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	content := bytes.Repeat([]byte("repair-at-shardkey-segment-"), 256)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
	}

	// Drop shard 0 so it must be reconstructed from the surviving shard 1.
	require.NoError(t, os.Remove(mustShardPath(svc, "b", shardKey, 0)))

	rec := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	require.NoError(t, backend.RepairShardAtShardKey(t.Context(), "b", shardKey, rec, 0))

	// The rebuilt shard must exist and match the canonical split bytes.
	rebuilt, err := svc.ReadLocalShard("b", shardKey, 0)
	require.NoError(t, err)
	require.Equal(t, freshShards[0], rebuilt)
}

// TestRepairShardAtShardKey_StripedReinterleaves proves repair regenerates a
// missing shard in the SAME stripe-interleaved layout its surviving siblings use,
// not the contiguous ECSplit layout. Geometry matters: stripeBytes must NOT be
// divisible by K — otherwise the wrong-Join and wrong-re-Split of the contiguous
// path coincidentally cancel and produce the interleaved bytes anyway, masking the
// bug. K=3 with a 1 MiB stripe (1048576 % 3 == 1) accumulates intra-stripe padding
// so the layouts genuinely diverge. We repair DATA shard 0, then drop a DIFFERENT
// data shard (1) and de-interleave from {0,2,3} — the repaired shard 0 is in the
// data join, so this recovery only succeeds if it is correctly interleaved.
func TestRepairShardAtShardKey_StripedReinterleaves(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc
	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	const shardKey = "obj/segments/striped-blob-0001"
	const stripeBytes = 1 << 20 // 1048576 % 3 != 0 — breaks the divisible-geometry cancellation
	const repairIdx = 0         // a data shard; participates in the de-interleave join below
	cfg := ECConfig{DataShards: 3, ParityShards: 2}
	payload := make([]byte, 4*1024*1024+777) // multi-stripe, non-aligned tail
	for i := range payload {
		payload[i] = byte((i*131 + 7) % 251)
	}

	// On-disk convention: each shard = 8-byte origSize header + interleaved body.
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)
	hdr := ShardHeader(int64(len(payload)))
	for i := range bodies {
		shard := append(append([]byte(nil), hdr[:]...), bodies[i]...)
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, shard))
	}

	// Drop data shard 0 and repair it from the survivors.
	require.NoError(t, os.Remove(mustShardPath(svc, "b", shardKey, repairIdx)))
	rec := PlacementRecord{Nodes: []string{"self", "self", "self", "self", "self"}, K: 3, M: 2, StripeBytes: stripeBytes}
	require.NoError(t, backend.RepairShardAtShardKey(t.Context(), "b", shardKey, rec, repairIdx))

	// The repaired shard must byte-equal a sibling-format shard (header + interleaved body).
	rebuilt, err := svc.ReadLocalShard("b", shardKey, repairIdx)
	require.NoError(t, err)
	wantShard := append(append([]byte(nil), hdr[:]...), bodies[repairIdx]...)
	require.Equal(t, wantShard, rebuilt, "repaired data shard must match the interleaved sibling layout, not contiguous ECSplit")

	// Cross-shard-drop recovery: provide EXACTLY {0,2,3} (drop data shard 1 and
	// both parity) so RS must use the repaired shard 0 in the join. The object
	// recovers only if shard 0 was regenerated in the correct interleaved layout.
	survivors := make([][]byte, cfg.NumShards())
	for _, i := range []int{0, 2, 3} {
		raw, rerr := svc.ReadLocalShard("b", shardKey, i)
		require.NoError(t, rerr)
		_, body, derr := decodeShardHeader(raw)
		require.NoError(t, derr)
		survivors[i] = body
	}
	rc, err := newStripeDeinterleaveStreamReader(cfg, bodyReaders(survivors), stripeBytes, int64(len(payload)), nil)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got, "object must recover from {0,2,3} after repairing data shard 0")
}

func TestRepairShardAtShardKey_InsufficientSurvivors(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc

	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	const shardKey = "obj/segments/seg-blob-0002"
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	content := bytes.Repeat([]byte("repair-insufficient-survivors-"), 256)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
	}

	// Delete BOTH shards: repairing shard 0 skips shard 0 and finds shard 1 also
	// gone, leaving 0 survivors < DataShards. The error must carry the substring
	// startup repair error classification matches on.
	require.NoError(t, os.Remove(mustShardPath(svc, "b", shardKey, 0)))
	require.NoError(t, os.Remove(mustShardPath(svc, "b", shardKey, 1)))

	rec := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	err = backend.RepairShardAtShardKey(t.Context(), "b", shardKey, rec, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "other shards readable")
}

func TestRepairShardAtShardKey_NilShardService(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	backend.shardSvc = nil
	err := backend.RepairShardAtShardKey(t.Context(), "b", "obj/segments/x", PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}, 0)
	require.Error(t, err)
}
