package cluster

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestMergeSegmentsOwnerLocal(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket, key := "b", "k"

	s1, err := b.writeSegmentBlobForAppend(bucket, key, bytes.NewReader([]byte("aaaa")))
	require.NoError(t, err)
	s2, err := b.writeSegmentBlobForAppend(bucket, key, bytes.NewReader([]byte("bbbb")))
	require.NoError(t, err)
	s3, err := b.writeSegmentBlobForAppend(bucket, key, bytes.NewReader([]byte("cc")))
	require.NoError(t, err)

	coalescedID := "c1"
	out, err := b.mergeSegmentsOwnerLocal(bucket, key, coalescedID, []storage.SegmentRef{s1, s2, s3})
	require.NoError(t, err)

	if out.Size != 10 {
		t.Fatalf("Size = %d, want 10", out.Size)
	}
	h := md5.Sum([]byte("aaaabbbbcc"))
	expETag := hex.EncodeToString(h[:])
	if out.ETag != expETag {
		t.Fatalf("ETag = %s, want %s", out.ETag, expETag)
	}

	p := b.coalescedBlobPath(bucket, key, coalescedID)
	info, statErr := os.Stat(p)
	require.NoError(t, statErr)
	if info.Size() != 10 {
		t.Fatalf("on-disk size = %d, want 10", info.Size())
	}
}
