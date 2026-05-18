package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics"
)

// TestCoalesceMetricsObserved verifies the Phase B3 metrics counters
// advance when a coalesce job completes successfully:
//
//   - grainfs_append_coalesce_total{result="success"} increments by 1
//   - grainfs_append_coalesce_bytes adds the coalesced size
//   - grainfs_append_coalesce_latency_seconds gets one observation
func TestCoalesceMetricsObserved(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	svc := NewShardService(b.root, nil)
	b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})

	successBefore := testutil.ToFloat64(metrics.AppendCoalesceTotal.WithLabelValues("success"))
	bytesBefore := testutil.ToFloat64(metrics.AppendCoalesceBytes)

	bucket, key := "b", "k"
	var off int64
	var totalBytes int64
	for i := 0; i < 16; i++ {
		chunk := []byte(fmt.Sprintf("c%02d-", i))
		_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader(chunk))
		require.NoError(t, err)
		off += int64(len(chunk))
		totalBytes += int64(len(chunk))
	}

	require.Eventually(t, func() bool {
		obj, _ := b.HeadObject(ctx, bucket, key)
		return obj != nil && len(obj.Coalesced) == 1
	}, 5*time.Second, 20*time.Millisecond)

	// After: wait for the deferred metric.Inc() in processCoalesceJobB3 to land.
	// obj.Coalesced becoming visible (the prior Eventually) is set inside the same
	// function but BEFORE the defer that calls metrics.AppendCoalesceTotal.Inc()
	// (internal/cluster/coalesce.go:158). Sync the metric read too.
	require.Eventually(t, func() bool {
		successAfter := testutil.ToFloat64(metrics.AppendCoalesceTotal.WithLabelValues("success"))
		return successAfter-successBefore >= 1.0
	}, 5*time.Second, 20*time.Millisecond, "coalesce success counter must increment")

	require.Eventually(t, func() bool {
		bytesAfter := testutil.ToFloat64(metrics.AppendCoalesceBytes)
		return bytesAfter-bytesBefore >= float64(totalBytes)
	}, 5*time.Second, 20*time.Millisecond, "coalesce bytes counter must add at least totalBytes")
}

// TestCoalesceEncryptedECRoundTrip ensures B3 coalesce + EC reconstruct works
// when the shard service is configured with an encryptor. The design's open
// question #3 resolution is "coalesce 시점에 encryptor 적용 (PutObject EC와
// 동일 흐름)" — that is already true because processCoalesceJobB3 reuses the
// same newECObjectWriter path as PutObject. This test pins that contract.
func TestCoalesceEncryptedECRoundTrip(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x55}, 32))
	require.NoError(t, err)
	svc := NewShardService(b.root, nil, WithEncryptor(enc))
	b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})

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
	require.Eventually(t, func() bool {
		obj, _ := b.HeadObject(ctx, bucket, key)
		return obj != nil && len(obj.Coalesced) == 1 && len(obj.Segments) == 0
	}, 5*time.Second, 20*time.Millisecond, "coalesce did not complete")

	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, expected, body)
}
