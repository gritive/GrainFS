package putpipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataBatcher_PendingVisibleBeforeCommit(t *testing.T) {
	in := make(chan MetadataRecord, 4)
	m := &MetadataBatcher{
		in:         in,
		batchSize:  100, // never auto-flushes here
		flushAfter: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	in <- MetadataRecord{Bucket: "b", Key: "k1", Size: 42}
	require.Eventually(t, func() bool {
		_, ok := m.PeekPending("b", "k1", "")
		return ok
	}, time.Second, 10*time.Millisecond)

	rec, ok := m.PeekPending("b", "k1", "")
	require.True(t, ok)
	require.Equal(t, int64(42), rec.Size)
}

func TestMetadataBatcher_FlushesOnSize(t *testing.T) {
	in := make(chan MetadataRecord, 16)
	var flushes atomic.Int32
	m := &MetadataBatcher{in: in, batchSize: 3, flushAfter: time.Hour}
	m.flushFn = func(buf []MetadataRecord) { flushes.Add(1); m.flush(buf) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	in <- MetadataRecord{Bucket: "b", Key: "1"}
	in <- MetadataRecord{Bucket: "b", Key: "2"}
	in <- MetadataRecord{Bucket: "b", Key: "3"}
	require.Eventually(t, func() bool { return flushes.Load() == 1 }, time.Second, 10*time.Millisecond)
}

func TestMetadataBatcher_FlushesOnTime(t *testing.T) {
	in := make(chan MetadataRecord, 4)
	var flushes atomic.Int32
	m := &MetadataBatcher{in: in, batchSize: 100, flushAfter: 30 * time.Millisecond}
	m.flushFn = func(buf []MetadataRecord) { flushes.Add(1); m.flush(buf) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	in <- MetadataRecord{Bucket: "b", Key: "1"}
	require.Eventually(t, func() bool { return flushes.Load() >= 1 }, time.Second, 10*time.Millisecond)
}
