package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCoalescedReadAfterECDistribute ensures GetObject after Phase B3
// coalesce returns the full body via EC reconstruct — without any owner-local
// coalesced file. This is the read-path counterpart to
// TestProcessCoalesceJobB3ECDistributed (Task 14): write+coalesce → assert
// owner-local file gone → assert GetObject reproduces every byte appended.
func TestCoalescedReadAfterECDistribute(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	svc := NewShardService(b.root, nil)
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

	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Len(t, obj.Coalesced, 1)

	// Confirm owner-local coalesced file does NOT exist.
	localPath := b.coalescedBlobPath(bucket, key, obj.Coalesced[0].CoalescedID)
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("owner-local coalesced still exists at %s", localPath)
	}

	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, expected, body)
}
