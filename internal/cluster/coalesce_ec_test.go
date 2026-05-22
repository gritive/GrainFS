package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestProcessCoalesceJobB3ECDistributed verifies the Phase B3 coalesce
// worker EC-distributes the coalesced blob across the placement and the
// resulting CoalescedShardRef carries the EC params (NodeIDs / k / m).
//
// Single-node fixture is sufficient because the EC writer treats every
// placement entry equally — when allNodes are all selfAddr the write still
// exercises the 4+2 encode + 6 shard write path. Real multi-node coverage
// lives in the e2e omnibus (Task 20).
func TestProcessCoalesceJobB3ECDistributed(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// Configure 4+2 EC with self-only placement (matches existing EC unit-test
	// patterns in ec_fix_test.go).
	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	svc := NewShardService(b.root, nil)
	b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})

	// Drive coalesce via 16 appends.
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

	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Len(t, obj.Coalesced, 1)
	c := obj.Coalesced[0]

	// EC placement params must be set by the B3 worker.
	if got, want := len(c.NodeIDs), 6; got != want {
		t.Fatalf("NodeIDs len = %d, want %d (k=4+m=2)", got, want)
	}
	if c.ECData != 4 || c.ECParity != 2 {
		t.Fatalf("EC params = k=%d m=%d, want 4+2", c.ECData, c.ECParity)
	}

	// Owner-local coalesced file must be removed — EC shards are now the
	// source of truth.
	localPath := b.coalescedBlobPath(bucket, key, c.CoalescedID)
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("owner-local coalesced still exists at %s (err=%v)", localPath, err)
	}
}
