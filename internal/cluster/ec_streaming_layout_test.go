// Standalone probes for klauspost/reedsolomon layout assumptions, the
// cluster spoolECShards write path, and the cluster GetObject read path.
package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestStreamingShardLayoutInterleavesBlocks: confirm Stream Split layout.
// Use marker bytes (1 byte per position so we can verify exact mapping).
func TestStreamingShardLayoutInterleavesBlocks(t *testing.T) {
	const (
		blockSize = 1 << 20
		k         = 2
		m         = 2 // match prod 2+2
	)
	// Mark bytes: input[i] = byte(i / blockSize) so each 1MiB block has a unique marker.
	input := make([]byte, 10<<20)
	for i := range input {
		input[i] = byte(i / blockSize)
	}
	enc, err := reedsolomon.NewStream(k, m, reedsolomon.WithStreamBlockSize(blockSize))
	require.NoError(t, err)
	dataBufs := make([]*bytes.Buffer, k)
	dataWriters := make([]io.Writer, k)
	for i := range dataBufs {
		dataBufs[i] = &bytes.Buffer{}
		dataWriters[i] = dataBufs[i]
	}
	require.NoError(t, enc.Split(bytes.NewReader(input), dataWriters, int64(len(input))))

	for sIdx, b := range dataBufs {
		body := b.Bytes()
		for blk := 0; blk*blockSize < len(body); blk++ {
			start := blk * blockSize
			marker := body[start]
			t.Logf("shard %d in-shard block %d: marker=%d (i.e. this 1MiB chunk came from original block %d, bytes [%d, %d))",
				sIdx, blk, marker, marker, int(marker)*blockSize, (int(marker)+1)*blockSize)
		}
	}

	var linear bytes.Buffer
	for _, b := range dataBufs {
		linear.Write(b.Bytes())
	}
	t.Logf("Stream Split: linear-concat round-trips=%v size=%d", bytes.Equal(linear.Bytes(), input), linear.Len())
}

// TestSpoolECShardsWritesLinearLayout: verify on-disk shard layout.
func TestSpoolECShardsWritesLinearLayout(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	input := append(bytes.Repeat([]byte{'A'}, 5<<20), bytes.Repeat([]byte{'B'}, 5<<20)...)
	tmpDir := t.TempDir()
	spoolPath := tmpDir + "/spool.bin"
	require.NoError(t, os.WriteFile(spoolPath, input, 0o600))
	sp := &spooledObject{Path: spoolPath, Size: int64(len(input))}
	defer sp.Cleanup()
	out, err := spoolECShards(context.Background(), cfg, tmpDir, sp)
	require.NoError(t, err)
	defer out.Cleanup()
	for i := 0; i < cfg.DataShards; i++ {
		rc, err := out.OpenShard(i)
		require.NoError(t, err)
		header := make([]byte, shardHeaderSize)
		_, _ = io.ReadFull(rc, header)
		body, _ := io.ReadAll(rc)
		_ = rc.Close()
		t.Logf("on-disk data shard %d: size=%d A=%d B=%d first16=%q last16=%q",
			i, len(body), bytes.Count(body, []byte{'A'}), bytes.Count(body, []byte{'B'}),
			body[:16], body[len(body)-16:])
	}
}

// TestClusterPutGet_10MiB_2plus2_RoundTrip: write 10 MiB (5 A + 5 B) to a
// backend with 2+2 EC and verify byte-equal round-trip via GetObject.
// This is the closest unit-level reproduction of the e2e cluster GET
// corruption observed at warp multipart partNumber=1.
func TestClusterPutGet_10MiB_2plus2_RoundTrip(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	shardDir := t.TempDir()
	backend.shardSvc = NewShardService(shardDir, nil)

	// 4 "nodes" all pointing at self → 2+2 IsActive(4)=true, all shards local.
	const selfAddr = "self"
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr, selfAddr, selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 2, ParityShards: 2})

	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	input := append(bytes.Repeat([]byte{'A'}, 5<<20), bytes.Repeat([]byte{'B'}, 5<<20)...)
	_, err := backend.PutObject(context.Background(), "b", "k", bytes.NewReader(input), "application/octet-stream")
	require.NoError(t, err)

	rc, obj, err := backend.GetObject(context.Background(), "b", "k")
	require.NoError(t, err)
	defer rc.Close()
	require.NotNil(t, obj)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)

	t.Logf("got: size=%d obj.Size=%d A=%d B=%d", len(got), obj.Size,
		bytes.Count(got, []byte{'A'}), bytes.Count(got, []byte{'B'}))
	if !bytes.Equal(got, input) {
		first := -1
		for i := 0; i < len(got) && i < len(input); i++ {
			if got[i] != input[i] {
				first = i
				break
			}
		}
		t.Logf("first divergence at byte=%d (block=%d, offsetInBlock=%d)",
			first, first/(1<<20), first%(1<<20))
		// dump first 5 MiB block boundaries
		for blk := 0; blk*(1<<20) < len(got); blk++ {
			start := blk * (1 << 20)
			end := start + (1 << 20)
			if end > len(got) {
				end = len(got)
			}
			t.Logf("got block %d: A=%d B=%d", blk,
				bytes.Count(got[start:end], []byte{'A'}),
				bytes.Count(got[start:end], []byte{'B'}))
		}
	}
	require.Equal(t, input, got, "round-trip must match")
}

// TestEcReconstructMissingDataStreamTo_LinearLayout: prove the read fallback
// produces block-interleaved output even though Stream Split stores shards
// linearly. Encode 10 MiB into 2+2, drop data shard 1, then call
// ecReconstructMissingDataStreamTo and check the output bytes.
func TestEcReconstructMissingDataStreamTo_LinearLayout(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	input := append(bytes.Repeat([]byte{'A'}, 5<<20), bytes.Repeat([]byte{'B'}, 5<<20)...)

	tmpDir := t.TempDir()
	spoolPath := tmpDir + "/spool.bin"
	require.NoError(t, os.WriteFile(spoolPath, input, 0o600))
	sp := &spooledObject{Path: spoolPath, Size: int64(len(input))}
	defer sp.Cleanup()

	out, err := spoolECShards(context.Background(), cfg, tmpDir, sp)
	require.NoError(t, err)
	defer out.Cleanup()

	// Build bodies[] just like the real read path: open all shards, strip
	// the 8-byte header, drop data shard 1 so reconstruct must run.
	bodies := make([]io.Reader, cfg.NumShards())
	for i := 0; i < cfg.NumShards(); i++ {
		if i == 1 {
			continue
		}
		rc, err := out.OpenShard(i)
		require.NoError(t, err)
		header := make([]byte, shardHeaderSize)
		_, err = io.ReadFull(rc, header)
		require.NoError(t, err)
		bodies[i] = rc
	}

	var got bytes.Buffer
	require.NoError(t, ecReconstructMissingDataStreamTo(&got, cfg, int64(len(input)), bodies))

	if !bytes.Equal(got.Bytes(), input) {
		for blk := 0; blk*(1<<20) < got.Len(); blk++ {
			start := blk * (1 << 20)
			end := start + (1 << 20)
			if end > got.Len() {
				end = got.Len()
			}
			d := got.Bytes()[start:end]
			a := bytes.Count(d, []byte{'A'})
			b := bytes.Count(d, []byte{'B'})
			label := "MIXED"
			if a == len(d) {
				label = "A"
			} else if b == len(d) {
				label = "B"
			}
			t.Logf("reconstruct block %d (offset %d): %s (A=%d B=%d)", blk, start, label, a, b)
		}
	}
	require.Equal(t, input, got.Bytes(),
		"ecReconstructMissingDataStreamTo must reproduce original linear layout")
}

// TestClusterMultipart_10MiB_2plus2_RoundTrip: write a 10 MiB object via
// the multipart API (2 × 5 MiB parts) and verify byte-equal round-trip via
// GetObject. Reproduces the e2e cluster GET corruption.
func TestClusterMultipart_10MiB_2plus2_RoundTrip(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	shardDir := t.TempDir()
	backend.shardSvc = NewShardService(shardDir, nil)

	const selfAddr = "self"
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr, selfAddr, selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 2, ParityShards: 2})

	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	part1 := bytes.Repeat([]byte{'A'}, 5<<20)
	part2 := bytes.Repeat([]byte{'B'}, 5<<20)

	mu, err := backend.CreateMultipartUpload(ctx, "b", "k", "application/octet-stream")
	require.NoError(t, err)

	p1, err := backend.UploadPart(ctx, "b", "k", mu.UploadID, 1, bytes.NewReader(part1))
	require.NoError(t, err)
	p2, err := backend.UploadPart(ctx, "b", "k", mu.UploadID, 2, bytes.NewReader(part2))
	require.NoError(t, err)

	_, err = backend.CompleteMultipartUpload(ctx, "b", "k", mu.UploadID, []storage.Part{
		{PartNumber: 1, ETag: p1.ETag, Size: p1.Size},
		{PartNumber: 2, ETag: p2.ETag, Size: p2.Size},
	})
	require.NoError(t, err)

	rc, obj, err := backend.GetObject(ctx, "b", "k")
	require.NoError(t, err)
	defer rc.Close()
	require.NotNil(t, obj)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)

	expected := append(append([]byte{}, part1...), part2...)
	t.Logf("got: size=%d obj.Size=%d A=%d B=%d", len(got), obj.Size,
		bytes.Count(got, []byte{'A'}), bytes.Count(got, []byte{'B'}))
	if !bytes.Equal(got, expected) {
		first := -1
		for i := 0; i < len(got) && i < len(expected); i++ {
			if got[i] != expected[i] {
				first = i
				break
			}
		}
		t.Logf("first divergence at byte=%d (block=%d, offsetInBlock=%d)",
			first, first/(1<<20), first%(1<<20))
		for blk := 0; blk*(1<<20) < len(got); blk++ {
			start := blk * (1 << 20)
			end := start + (1 << 20)
			if end > len(got) {
				end = len(got)
			}
			t.Logf("got block %d: A=%d B=%d", blk,
				bytes.Count(got[start:end], []byte{'A'}),
				bytes.Count(got[start:end], []byte{'B'}))
		}
	}
	require.Equal(t, expected, got, "multipart round-trip must match")
}
