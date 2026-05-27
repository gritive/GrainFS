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
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

func TestPipeline_DEKKeeperAdapter_WriteReadParity(t *testing.T) {
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x55}, encrypt.KEKSize), bytes.Repeat([]byte{0x66}, 16))
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	cid := bytes.Repeat([]byte{0x77}, 16)
	writeEnc := storage.NewDEKKeeperAdapter(keeper, cid)
	readEnc := storage.NewDEKKeeperAdapter(keeper, cid)
	fields := cluster.ShardAADFields("bkt", "key", 2)
	var buf bytes.Buffer
	if err := eccodec.EncodeEncryptedShard(&buf, bytes.NewReader(bytes.Repeat([]byte("p"), 4096)), writeEnc, fields, eccodec.DefaultEncryptedChunkSize); err != nil {
		t.Fatalf("encode (write adapter): %v", err)
	}
	var out bytes.Buffer
	if err := eccodec.DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), readEnc, fields); err != nil {
		t.Fatalf("decode (read adapter) — clusterID coupling broke?: %v", err)
	}
	if !bytes.Equal(out.Bytes(), bytes.Repeat([]byte("p"), 4096)) {
		t.Fatal("parity mismatch")
	}
	badRead := storage.NewDEKKeeperAdapter(keeper, bytes.Repeat([]byte{0x00}, 16))
	if err := eccodec.DecodeEncryptedShard(&bytes.Buffer{}, bytes.NewReader(buf.Bytes()), badRead, fields); err == nil {
		t.Fatal("expected divergent clusterID to fail AEAD")
	}
}

func TestPipelineNew_BuildsDEKKeeperAdapterWhenKeeperSet(t *testing.T) {
	keeper, _ := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x55}, encrypt.KEKSize), bytes.Repeat([]byte{0x66}, 16))
	p := New(Config{DataDirs: []string{t.TempDir()}, DEKKeeper: keeper, ClusterID: bytes.Repeat([]byte{0x77}, 16), ECConfig: cluster.ECConfig{DataShards: 1, ParityShards: 0}})
	if _, ok := p.cpu.enc.(*storage.DEKKeeperAdapter); !ok {
		t.Fatalf("expected *storage.DEKKeeperAdapter, got %T", p.cpu.enc)
	}
}

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
