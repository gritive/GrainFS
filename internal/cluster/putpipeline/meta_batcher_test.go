package putpipeline

import (
	"context"
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
