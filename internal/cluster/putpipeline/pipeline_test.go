package putpipeline

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestPipeline_Put_5MiB_RoundTrip(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()}
	p := New(Config{
		DataDirs:    dirs,
		Encryptor:   testEncryptorRaw(t),
		ECConfig:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		StripeBytes: 1 << 20,
	})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx))
	}()

	body := make([]byte, 5*1024*1024)
	for i := range body {
		body[i] = byte(i % 251)
	}
	wantSum := md5.Sum(body)
	wantETag := hex.EncodeToString(wantSum[:])

	size := int64(len(body))
	obj, err := p.Put(context.Background(), PutRequest{
		Bucket:   "external",
		Key:      "obj1",
		Body:     bytes.NewReader(body),
		SizeHint: &size,
	})
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, wantETag, obj.ETag)

	// All 4 shard files should land on disk eventually. With K-quorum
	// early ack the handler can return before parity shards finalize,
	// so poll until the file system reflects the full set.
	require.Eventually(t, func() bool {
		for i := 0; i < 4; i++ {
			driveIdx := i % len(dirs)
			path := filepath.Join(dirs[driveIdx], "external", "obj1", fmt.Sprintf("shard_%d", i))
			if _, err := os.Stat(path); err != nil {
				return false
			}
		}
		return true
	}, 3*time.Second, 25*time.Millisecond, "all 4 shards must land on disk")

	// Metadata is queued in MetadataBatcher's pending map (Phase 5.3
	// wires the real Badger commit; here db is nil → records stay
	// pending forever, which is fine for this test).
	require.Eventually(t, func() bool {
		_, ok := p.PeekPendingMetadata("external", "obj1", "")
		return ok
	}, 3*time.Second, 25*time.Millisecond, "metadata must reach the batcher's pending map")
}
