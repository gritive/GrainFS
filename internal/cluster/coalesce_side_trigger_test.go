package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Side-record appendables keep the manifest's Segments empty (the raw tail
// lives in side records, summarized by the append side SUMMARY). The two
// coalesce enqueue points used to read cmd.Segments and therefore never fired
// for side-mode objects — every appendable created since side records became
// the default accumulated raw segments forever. These tests drive the REAL
// AppendObject → trigger → worker → merge → publish pipeline on the test
// backend and assert a coalesced ref is actually published.

func appendChunks(t *testing.T, b *DistributedBackend, bucket, key string, n int) {
	t.Helper()
	ctx := context.Background()
	var off int64
	for i := 0; i < n; i++ {
		chunk := bytes.Repeat([]byte{byte(i + 1)}, 32)
		_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader(chunk))
		require.NoError(t, err, "append %d", i)
		off += int64(len(chunk))
	}
}

func waitForCoalesced(t *testing.T, b *DistributedBackend, bucket, key string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		obj, err := b.HeadObject(ctx, bucket, key)
		require.NoError(t, err)
		if len(obj.Coalesced) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	t.Fatalf("no coalesced ref published within deadline: Coalesced=%d Segments=%d Size=%d",
		len(obj.Coalesced), len(obj.Segments), obj.Size)
}

// The APPEND-path trigger must fire from the side SUMMARY counts: the
// manifest's Segments stay empty in side mode, so a Segments-based trigger
// never enqueues (the pre-fix behavior — this test failed with a bare
// "no coalesced ref published" timeout).
func TestCoalesceTriggersFromAppendSideSummary(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bk"))
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount:    4,
		SizeBytes:       64 << 20,
		IdleTimeout:     time.Hour, // count is the only reachable trigger here
		CleanupInterval: 0,         // backstop off: the append-path trigger must do it
		SizeCapBytes:    1 << 30,
	})

	appendChunks(t, b, "bk", "k", 4)

	// Prove the object really is side-mode (manifest carries no segments).
	cmd, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.Empty(t, cmd.Segments, "precondition: side-record mode keeps manifest Segments empty")

	waitForCoalesced(t, b, "bk", "k")
}

// The BACKSTOP scan must also see side-mode appendables: it reads the side
// SUMMARY when the manifest carries no segments. Pre-fix it skipped every
// side-mode object (len(cmd.Segments)==0 → continue), so a missed in-process
// trigger was never recovered.
func TestCoalesceBackstopScansSideSummary(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bk"))

	// Thresholds no append can reach: the in-process trigger stays silent, so
	// only the backstop can enqueue this object.
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount:    1 << 20,
		SizeBytes:       1 << 40,
		IdleTimeout:     time.Hour,
		CleanupInterval: 0,
		SizeCapBytes:    1 << 30,
	})
	appendChunks(t, b, "bk", "k", 4)

	cmd, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.Empty(t, cmd.Segments, "precondition: side-record mode keeps manifest Segments empty")

	// Lower the thresholds and run one backstop sweep by hand.
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount:    4,
		SizeBytes:       64 << 20,
		IdleTimeout:     time.Hour,
		CleanupInterval: 0,
		SizeCapBytes:    1 << 30,
	})
	b.scanAppendableAndTrigger(ctx)

	waitForCoalesced(t, b, "bk", "k")
}
