package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCoalescedReadAfterOwnerFailure simulates owner failure at the
// single-node test fixture level: after Phase B3 coalesce completes, both
// the owner-local coalesced file AND every raw segment file are removed
// from disk. GetObject must still return the full body via EC reconstruct
// against the peer shards.
//
// Multi-node owner-kill coverage lives in the e2e omnibus (Task 20). This
// unit test exercises the read-path resilience that makes that e2e work.
func TestCoalescedReadAfterOwnerFailure(t *testing.T) {
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

	// "Owner failure": wipe the on-disk owner-local files (segment + coalesced
	// dirs). EC shards under shardSvc remain intact — those are the
	// reconstruct source.
	segDir := b.objectPath(bucket, key) + "_segments"
	_ = os.RemoveAll(segDir)
	coalescedDir := filepath.Dir(b.coalescedBlobPath(bucket, key, "x"))
	_ = os.RemoveAll(coalescedDir)

	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, expected, body)
}
