package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessCoalesceJobB2EndToEnd(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	bucket, key := "b", "k"
	chunks := []string{"aaaa", "bbbb", "cc"}
	for _, c := range chunks {
		_, err := b.AppendObject(ctx, bucket, key, int64(currentSize(t, b, bucket, key)), bytes.NewReader([]byte(c)))
		require.NoError(t, err)
	}

	// Snapshot prior segment count to confirm coalesce shrinks it.
	pre, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Equal(t, 3, len(pre.Segments))

	require.NoError(t, b.processCoalesceJobB2(ctx, coalesceJob{Bucket: bucket, Key: key}))

	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	if len(obj.Segments) != 0 {
		t.Fatalf("raw segments not pruned: %+v", obj.Segments)
	}
	if len(obj.Coalesced) != 1 {
		t.Fatalf("coalesced entry missing: %+v", obj.Coalesced)
	}

	// GET returns the full stitched body via coalesced blob.
	rc, _, err := b.GetObject(ctx, bucket, key)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "aaaabbbbcc" {
		t.Fatalf("body = %q, want %q", got, "aaaabbbbcc")
	}

	// Raw segment files are unlinked.
	segDir := b.objectPath(bucket, key) + "_segments"
	if entries, _ := os.ReadDir(segDir); len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("raw segment files remain: %v", names)
	}
	// Coalesced blob file exists where expected.
	cp := filepath.Join(b.objectPath(bucket, key)+"_coalesced", obj.Coalesced[0].CoalescedID)
	if _, err := os.Stat(cp); err != nil {
		t.Fatalf("coalesced file missing: %v", err)
	}
}

// currentSize is a small helper that returns obj.Size when present, 0 otherwise.
func currentSize(t *testing.T, b *DistributedBackend, bucket, key string) int64 {
	t.Helper()
	obj, err := b.HeadObject(context.Background(), bucket, key)
	if err != nil {
		return 0
	}
	return obj.Size
}
