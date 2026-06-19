package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/gritive/GrainFS/internal/transport"
)

func TestShardService_LocalWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, tr, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	// Verify shards directory created
	_, err := os.Stat(filepath.Join(dir, "shards"))
	require.NoError(t, err)

	// The shard RPC handler exists (native /shard/rpc route)
	handler := svc.NativeRPCHandler()
	require.NotNil(t, handler)

	// Direct local write
	shardDir := filepath.Join(dir, "shards", "test-bucket", "test-key")
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "shard_0"), []byte("hello shard"), 0o644))

	// Read it back
	data, err := os.ReadFile(filepath.Join(shardDir, "shard_0"))
	require.NoError(t, err)
	assert.Equal(t, "hello shard", string(data))
}

func TestShardServiceAcceptsDEKKeeperWithoutStaticEncryptor(t *testing.T) {
	kek := bytes.Repeat([]byte{0x73}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x74}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)

	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID))
	require.NotNil(t, svc)
	require.NotNil(t, svc.segEnc)
	require.NotNil(t, svc.DEKKeeper())
	require.Equal(t, keeper, svc.DEKKeeper())
}

func TestEncodeEncryptedShardBufferRoundTripsViaDEK(t *testing.T) {
	kek := bytes.Repeat([]byte{0x75}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x76}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID))

	plain := bytes.Repeat([]byte("shard-bytes"), 1000)
	enc, err := svc.EncodeEncryptedShardBuffer("bucket", "key", 2, plain)
	require.NoError(t, err)
	require.True(t, eccodec.IsEncryptedShard(enc), "must be GFSENC3 ciphertext, not plaintext")
	require.False(t, bytes.Contains(enc, plain[:64]), "plaintext run must not appear in ciphertext")

	// Round-trips through the normal read decoder.
	var out bytes.Buffer
	require.NoError(t, eccodec.DecodeEncryptedShard(&out, bytes.NewReader(enc), svc.segEnc, ShardAADFields("bucket", "key", 2)))
	require.Equal(t, plain, out.Bytes())
}

// TestShardService_Encryption verifies that shards written with an encryptor
// are NOT stored as plaintext on disk, and can be decrypted on read.
func TestShardService_Encryption(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	svc := NewShardService(dir, tr, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := []byte("secret shard data")
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	// Raw on-disk bytes must differ from plaintext
	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	raw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, raw, "shard should be encrypted on disk")

	// ReadLocalShard must return the original plaintext
	got, err := svc.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestShardService_OpenLocalShard_EncryptedStreamsPlaintext(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := bytes.Repeat([]byte("secret shard data"), 8192)
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	r, err := svc.OpenLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

// TestShardService_WriteLocalSealedShardVerbatim proves the S2 seal-at-source
// receiver path: an already-sealed shard (sealed by the "source" via
// EncodeEncryptedShardBuffer, mirroring the coordinator's CPUPool) is stored
// VERBATIM — no re-encryption at the destination — and reads back to the
// original plaintext. A double-encrypt bug would make the on-disk bytes differ
// from the sealed bytes and corrupt the plaintext read; both assertions catch it.
func TestShardService_WriteLocalSealedShardVerbatim(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"),
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := bytes.Repeat([]byte("sealed-at-source shard "), 8192)
	// Source seals (mirrors CPUPool / coordinator seal-at-source).
	sealed, err := svc.EncodeEncryptedShardBuffer("bkt", "obj", 0, plaintext)
	require.NoError(t, err)
	require.True(t, eccodec.IsEncryptedShard(sealed))

	// Receiver stores the sealed bytes verbatim — no re-encode.
	require.NoError(t, svc.writeLocalSealedShard(context.Background(), "bkt", "obj", 0, sealed))

	// On-disk bytes must equal the sealed bytes exactly (verbatim, not re-sealed).
	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	raw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	require.Equal(t, sealed, raw, "sealed shard must be stored verbatim (no double-encryption)")

	// And reads back to the original plaintext through the normal decoder.
	got, err := svc.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestShardService_NativeWriteHandler_SealedShardRoundTrip drives the receiver
// branch end-to-end: a Sealed=true request + already-sealed body is stored
// verbatim by the handler and reads back to the original plaintext.
func TestShardService_NativeWriteHandler_SealedShardRoundTrip(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"),
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := bytes.Repeat([]byte("rpc sealed shard "), 8192)
	sealed, err := svc.EncodeEncryptedShardBuffer("bkt", "obj", 3, plaintext)
	require.NoError(t, err)

	// The receiver requires the completeness trailer the streaming sink appends
	// on a clean Finalize; hand-build it here since this test drives the handler
	// directly without the sink.
	body := AppendSealedShardTrailer(sealed, int64(len(sealed)))
	werr := svc.NativeWriteHandler()(transport.ShardWriteRequest{Bucket: "bkt", Key: "obj", ShardIdx: 3, Sealed: true}, bytes.NewReader(body))
	require.NoError(t, werr, "sealed-shard write must not error")

	got, err := svc.ReadLocalShard("bkt", "obj", 3)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestSplitSealedShardTrailer covers the receiver's completeness-check helper in
// isolation: a body shorter than the trailer, a declared-length mismatch (the
// signature of a mid-stream truncation), and the exact-match happy path.
func TestSplitSealedShardTrailer(t *testing.T) {
	payload := bytes.Repeat([]byte("sealed shard bytes "), 100)
	good := AppendSealedShardTrailer(payload, int64(len(payload)))

	t.Run("exact match strips and returns payload", func(t *testing.T) {
		got, err := SplitSealedShardTrailer(good)
		require.NoError(t, err)
		require.Equal(t, payload, got)
	})
	t.Run("shorter than trailer is rejected", func(t *testing.T) {
		_, err := SplitSealedShardTrailer([]byte{1, 2, 3})
		require.Error(t, err)
		require.Contains(t, err.Error(), "truncated")
	})
	t.Run("declared-length mismatch is rejected", func(t *testing.T) {
		// Drop the last payload byte (keep the trailer) — the classic truncation:
		// declared length now exceeds the bytes that precede the trailer.
		truncated := append(good[:len(payload)-1], good[len(payload):]...)
		_, err := SplitSealedShardTrailer(truncated)
		require.Error(t, err)
		require.Contains(t, err.Error(), "truncated")
	})
}

// TestShardService_NativeWriteHandler_RejectsTruncatedSealedShard proves the
// handler path: a Sealed=true body whose trailer declares MORE payload than was
// received (a truncated stream) is rejected with an error and commits NO shard
// file. This is the direct-handler counterpart to the over-transport
// TestRemoteSealedShardSink_AbortDoesNotCommit.
func TestShardService_NativeWriteHandler_RejectsTruncatedSealedShard(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"),
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := bytes.Repeat([]byte("truncated sealed shard "), 8192)
	sealed, err := svc.EncodeEncryptedShardBuffer("bkt", "obj", 5, plaintext)
	require.NoError(t, err)

	// Trailer declares the FULL length, but the body carries only half the sealed
	// bytes — exactly what a mid-stream abort leaves on the wire.
	full := AppendSealedShardTrailer(sealed, int64(len(sealed)))
	truncated := append(full[:len(sealed)/2], full[len(sealed):]...)

	werr := svc.NativeWriteHandler()(transport.ShardWriteRequest{Bucket: "bkt", Key: "obj", ShardIdx: 5, Sealed: true}, bytes.NewReader(truncated))
	require.Error(t, werr, "truncated sealed-shard write must be rejected")

	_, statErr := os.Stat(filepath.Join(dir, "shards", "bkt", "obj", "shard_5"))
	require.True(t, os.IsNotExist(statErr), "rejected sealed-shard write must commit NO shard file")
}

func TestBuildShardEnvelope_SizesBuilderForSmallShardPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64<<10)

	envb := buildShardEnvelope("WriteShard", "bkt", "obj/v1", 1, payload, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()

	require.LessOrEqual(t, cap(envb.Bytes), 80<<10)
}

func TestShardService_ReadLocalShardAt_EncryptedShard(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 192*1024)
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	offset := int64(eccodec.DefaultEncryptedChunkSize + 12345)
	buf := make([]byte, 4096)
	n, err := svc.ReadLocalShardAt("bkt", "obj", 0, offset, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, plaintext[offset:offset+int64(len(buf))], buf)
}

func TestShardService_ReadLocalShard_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	_, err := svc.ReadLocalShard("bkt", "no-such-obj", 0)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestShardService_ReadLocalShard_DecryptError(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	// Write garbage bytes that look like valid data but aren't valid ciphertext
	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
	require.NoError(t, os.WriteFile(rawPath, []byte("not-valid-ciphertext"), 0o644))

	_, err := svc.ReadLocalShard("bkt", "obj", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext rejected")
	assert.True(t, eccodec.IsCorruption(err), "non-encrypted bytes must classify as corruption: %v", err)
}

// TestShardService_ReadLocalShard_LegacyShardRejectedAsCorrupt confirms that
// GFSCRC1-encoded shards (legacy format no longer supported) and unrecognized
// raw bytes are rejected as corruption so the placement monitor quarantines
// instead of skipping.
func TestShardService_ReadLocalShard_LegacyShardRejectedAsCorrupt(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	const bucket = "bkt"

	t.Run("structural missing magic", func(t *testing.T) {
		// Bytes with no outer CRC envelope and no encrypted-blob magic header:
		// the stored shard is not in any recognized format → rejected as corrupt.
		raw := bytes.Repeat([]byte("X"), 64)

		rawPath := filepath.Join(dir, "shards", bucket, "structural", "shard_0")
		require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
		require.NoError(t, os.WriteFile(rawPath, raw, 0o644))

		_, err := svc.ReadLocalShard(bucket, "structural", 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "plaintext rejected")
		assert.True(t, eccodec.IsCorruption(err), "unrecognized format must classify as corruption: %v", err)
	})

	t.Run("plain GFSCRC1 fail-loud", func(t *testing.T) {
		// A genuinely unencrypted GFSCRC1 shard (valid CRC framing over plain
		// bytes, no inner encrypted-blob magic) is the format we intentionally
		// removed: it must fail loud as corruption, not be returned as plaintext.
		raw := eccodec.EncodeShard(bytes.Repeat([]byte("plain shard bytes "), 8))

		rawPath := filepath.Join(dir, "shards", bucket, "plaincrc", "shard_0")
		require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
		require.NoError(t, os.WriteFile(rawPath, raw, 0o644))

		_, err := svc.ReadLocalShard(bucket, "plaincrc", 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "plaintext rejected")
		assert.True(t, eccodec.IsCorruption(err), "plain GFSCRC1 must classify as corruption: %v", err)
	})
}

func TestShardService_ResolvePeerAddress(t *testing.T) {
	dir := t.TempDir()
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-a", "10.0.0.1:7001", 0)))
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithNodeAddressBook(f), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	addr, err := svc.resolvePeerAddress("node-a")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:7001", addr)

	_, err = svc.resolvePeerAddress("node-missing")
	require.ErrorContains(t, err, `node "node-missing" not found in address book`)
}

// TestShardService_RPCEncryptedWriteRead verifies that encryption works end-to-end
// over the QUIC RPC path: write via handleWrite → WriteLocalShard (encrypt) and
// read back via handleRead → ReadLocalShard (decrypt).
func TestShardService_RPCEncryptedWriteRead(t *testing.T) {
	ctx := context.Background()
	tracePath := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", tracePath)
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	keeper, clusterID := testDEKKeeper(t)

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())

	plaintext := []byte("encrypted rpc shard")
	require.NoError(t, svc1.WriteShard(ctx, tr2.LocalAddr(), "bkt", "key", 0, plaintext))

	// On-disk bytes on node2 must NOT be plaintext
	rawPath := filepath.Join(dir2, "shards", "bkt", "key", "shard_0")
	raw, readErr := os.ReadFile(rawPath)
	require.NoError(t, readErr)
	assert.NotEqual(t, plaintext, raw, "remote shard should be encrypted on disk")

	// Read back via RPC must return decrypted plaintext
	got, readErr := svc1.ReadShard(ctx, tr2.LocalAddr(), "bkt", "key", 0)
	require.NoError(t, readErr)
	assert.Equal(t, plaintext, got)

	events := readShardServiceTraceEvents(t, tracePath)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncOpen)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncWrite)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncSync)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncClose)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncRename)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalDirSync)
}

func TestShardService_ReadShardStream_EncryptedStreamsPlaintext(t *testing.T) {
	ctx := context.Background()

	keeper, clusterID := testDEKKeeper(t)

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	tr2.RegisterShardWriteHandler(svc2.NativeWriteHandler()) // WriteShardStream dials the native route (Phase 8 N6)
	tr2.RegisterShardReadHandler(svc2.NativeReadHandler())   // ReadShardStream dials the native route (Phase 8 N7-1)
	tr2.RegisterShardWriteHandler(svc2.NativeWriteHandler())
	tr2.RegisterShardReadHandler(svc2.NativeReadHandler())

	plaintext := bytes.Repeat([]byte("encrypted rpc shard"), 128*1024)
	require.NoError(t, svc1.WriteShardStream(ctx, tr2.LocalAddr(), "bkt", "key", 0, bytes.NewReader(plaintext)))

	rawPath := filepath.Join(dir2, "shards", "bkt", "key", "shard_0")
	raw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(plaintext[:64]))

	r, err := svc1.ReadShardStream(ctx, tr2.LocalAddr(), "bkt", "key", 0)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestShardService_ReadShardRange_RejectsMediumSingleFrame(t *testing.T) {
	ctx := context.Background()

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	dir1, dir2 := t.TempDir(), t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())

	plaintext := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), 4096)
	require.NoError(t, svc1.WriteShard(ctx, tr2.LocalAddr(), "bkt", "key", 0, plaintext))

	_, err := svc1.ReadShardRange(ctx, tr2.LocalAddr(), "bkt", "key", 0, 0, 64*1024+1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

func TestShardService_RPCWriteReadDelete(t *testing.T) {
	ctx := context.Background()

	// Set up two QUIC transports to simulate two nodes
	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	// Connect them

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	keeper, clusterID := testDEKKeeper(t)
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	// Set tr2's stream handler to svc2's handler (simulating node2's shard server)
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())

	// Node1 writes a shard to Node2
	err := svc1.WriteShard(ctx, tr2.LocalAddr(), "mybucket", "mykey", 0, []byte("shard-data-0"))
	require.NoError(t, err)

	// Verify shard landed on Node2's disk
	shardPath := filepath.Join(dir2, "shards", "mybucket", "mykey", "shard_0")
	_, err = os.Stat(shardPath)
	require.NoError(t, err)
	decoded, err := svc2.ReadLocalShard("mybucket", "mykey", 0)
	require.NoError(t, err)
	assert.Equal(t, "shard-data-0", string(decoded))

	// Node1 reads the shard back from Node2
	got, err := svc1.ReadShard(ctx, tr2.LocalAddr(), "mybucket", "mykey", 0)
	require.NoError(t, err)
	assert.Equal(t, "shard-data-0", string(got))

	// Node1 deletes the shard on Node2
	err = svc1.DeleteShards(ctx, tr2.LocalAddr(), "mybucket", "mykey")
	require.NoError(t, err)

	// Verify shard is gone on Node2
	_, err = os.ReadFile(shardPath)
	assert.True(t, os.IsNotExist(err))
}

func readShardServiceTraceEvents(t *testing.T, path string) []PutTraceEvent {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var out []PutTraceEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev PutTraceEvent
		require.NoError(t, json.Unmarshal(sc.Bytes(), &ev))
		out = append(out, ev)
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, out)
	return out
}

func requireShardServiceTraceStage(t *testing.T, events []PutTraceEvent, stage PutTraceStage) {
	t.Helper()
	for _, ev := range events {
		if ev.Stage == stage {
			return
		}
	}
	require.Failf(t, "missing trace stage", "stage %s not found in %#v", stage, events)
}

// TestWriteLocalShard_Atomic verifies that WriteLocalShard is crash-safe:
//  1. Successful writes leave no .tmp garbage.
//  2. Overwriting an existing shard produces correct final content.
//  3. The original shard is not modified when the write fails (parent dir
//     is made non-writable to force an error before rename).
func TestWriteLocalShard_Atomic(t *testing.T) {
	dir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	data := []byte("atomic-shard-payload")
	require.NoError(t, svc.WriteLocalShard("bkt", "key/v1", 0, data))

	shardPath := filepath.Join(dir, "shards", "bkt", "key/v1", "shard_0")
	tmpPath := shardPath + ".tmp"

	// Final shard must exist with correct content.
	decoded, err := svc.ReadLocalShard("bkt", "key/v1", 0)
	require.NoError(t, err)
	assert.Equal(t, data, decoded)

	// .tmp file must NOT remain after a successful write.
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), ".tmp file must not remain after successful WriteLocalShard")
}

// TestWriteLocalShard_OverwritePreservesOriginalOnError verifies that when
// WriteLocalShard fails mid-flight (simulated by making the shard dir
// non-writable so tmp creation is blocked), the existing shard is intact.
// With os.WriteFile (non-atomic), the file would be truncated before the
// error, leaving a torn shard. With tmp→rename, the original is never touched.
func TestWriteLocalShard_OverwritePreservesOriginalOnError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	dir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	original := []byte("original-safe-content")
	require.NoError(t, svc.WriteLocalShard("bkt", "key", 0, original))

	shardPath := filepath.Join(dir, "shards", "bkt", "key", "shard_0")

	// Make the shard directory non-writable so the tmp file cannot be created.
	shardDir := filepath.Dir(shardPath)
	require.NoError(t, os.Chmod(shardDir, 0o555))
	defer os.Chmod(shardDir, 0o755)

	// Write should fail because the directory is read-only.
	err := svc.WriteLocalShard("bkt", "key", 0, []byte("replacement"))
	require.Error(t, err, "write to read-only dir must fail")

	// Original shard must be intact — not truncated or corrupted.
	decoded, err := svc.ReadLocalShard("bkt", "key", 0)
	require.NoError(t, err)
	assert.Equal(t, original, decoded, "original shard must survive a failed overwrite")
}

func TestWriteReadLocalShard_Encrypted_AAD(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	data := []byte("secret shard payload")
	require.NoError(t, svc.WriteLocalShard("mybucket", "obj/v1", 2, data))

	// Round-trip must recover plaintext.
	got, err := svc.ReadLocalShard("mybucket", "obj/v1", 2)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	// Raw on-disk bytes must be opaque (not equal to plaintext).
	shardPath := filepath.Join(dir, "shards", "mybucket", "obj/v1", "shard_2")
	raw, _ := os.ReadFile(shardPath)
	assert.NotEqual(t, data, raw, "shard on disk must be encrypted")
	assert.True(t, eccodec.IsEncryptedShard(raw), "shard must use chunked encrypted envelope")
}

func TestWriteLocalShardStream_EncryptedUsesChunkedEnvelope(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	data := bytes.Repeat([]byte("stream-secret-"), 8192)
	require.NoError(t, svc.WriteLocalShardStream("b", "k", 1, bytes.NewReader(data)))

	shardPath := filepath.Join(dir, "shards", "b", "k", "shard_1")
	raw, err := os.ReadFile(shardPath)
	require.NoError(t, err)
	require.True(t, eccodec.IsEncryptedShard(raw), "streamed encrypted shard must use chunked envelope")

	got, err := svc.ReadLocalShard("b", "k", 1)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestWriteLocalShard_AAD_LocationBinding(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	data := []byte("payload")
	require.NoError(t, svc.WriteLocalShard("b", "k", 0, data))

	// Simulate attacker copying shard_0 to shard_1 position.
	src := filepath.Join(dir, "shards", "b", "k", "shard_0")
	raw, _ := os.ReadFile(src)
	dst := filepath.Join(dir, "shards", "b", "k", "shard_1")
	require.NoError(t, os.WriteFile(dst, raw, 0o600))

	// Reading shard_1 must fail because AAD doesn't match.
	_, err := svc.ReadLocalShard("b", "k", 1)
	require.Error(t, err, "shard moved to wrong position must fail decryption")
}

func TestWithShardDEKKeeper_SetsSegEncAndClusterID(t *testing.T) {
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x22}, encrypt.KEKSize), bytes.Repeat([]byte{0x33}, 16))
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	cid := bytes.Repeat([]byte{0x44}, 16)
	s := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, cid))
	if !bytes.Equal(s.clusterID[:], cid) {
		t.Fatalf("clusterID not threaded: %x", s.clusterID)
	}
	ct, gen, err := s.segEnc.Seal(encrypt.DomainShard, []encrypt.AADField{encrypt.FieldString("x")}, []byte("hi"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	pt, err := s.segEnc.Open(encrypt.DomainShard, []encrypt.AADField{encrypt.FieldString("x")}, gen, ct)
	if err != nil || string(pt) != "hi" {
		t.Fatalf("open: pt=%q err=%v", pt, err)
	}
}

func TestWithShardDEKKeeper_NilOrBadClusterIDIsNoOp(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	s := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), WithShardDEKKeeper(nil, nil))
	if s.segEnc == nil {
		t.Fatal("nil keeper must leave the valid DEK-keeper segEnc intact")
	}
}

func TestShardService_NativeWriteHandler_PlainAndSealed(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	tr2.RegisterShardWriteHandler(svc2.NativeWriteHandler())
	tr2.RegisterShardReadHandler(svc2.NativeReadHandler()) // ReadShardStream dials the native route (Phase 8 N7-1)
	// Tunnel registrations mirror boot wiring: the write tunnel handler stays
	// registered until N8 (and, pre-Task-3, WriteShardStream still dials it —
	// which is exactly what the InboundNativeShardWrites assertion below
	// discriminates). Read-back uses the streaming read (HandleRead), the same
	// shape TestShardService_ReadShardStream_EncryptedStreamsPlaintext uses.
	tr2.RegisterShardWriteHandler(svc2.NativeWriteHandler())
	tr2.RegisterShardReadHandler(svc2.NativeReadHandler())

	// Plain path (WriteShard semantics): destination seals; read-back decrypts.
	plaintext := bytes.Repeat([]byte("native shard write"), 64*1024)
	require.NoError(t, svc1.WriteShardStream(ctx, tr2.LocalAddr(), "bkt", "key", 0, bytes.NewReader(plaintext)))
	r, err := svc1.ReadShardStream(ctx, tr2.LocalAddr(), "bkt", "key", 0)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, plaintext, got)
	require.Positive(t, tr2.InboundNativeShardWrites(), "write must dispatch through the native route, not the tunnel")

	// Sealed path: a truncated body (trailer mismatch) must be rejected.
	bodyNoTrailer := []byte("sealed-bytes-without-trailer")
	err = tr1.ShardWrite(ctx, tr2.LocalAddr(),
		transport.ShardWriteRequest{Bucket: "bkt", Key: "key2", ShardIdx: 0, Sealed: true},
		bytes.NewReader(bodyNoTrailer))
	require.Error(t, err, "missing completeness trailer must be rejected")
	require.Contains(t, err.Error(), "truncated")

	// Sealed path, complete: payload + correct trailer commits. The body must be
	// produced by the same sealer (EncodeEncryptedShardBuffer) with the SAME
	// identity the write targets — the AAD binds bucket/shardKey/shardIdx.
	sealed, err := svc2.EncodeEncryptedShardBuffer("bkt", "key3", 0, []byte("sealed shard plaintext"))
	require.NoError(t, err)
	withTrailer := AppendSealedShardTrailer(append([]byte(nil), sealed...), int64(len(sealed)))
	require.NoError(t, tr1.ShardWrite(ctx, tr2.LocalAddr(),
		transport.ShardWriteRequest{Bucket: "bkt", Key: "key3", ShardIdx: 0, Sealed: true},
		bytes.NewReader(withTrailer)))
	gotSealed, err := svc2.ReadLocalShard("bkt", "key3", 0)
	require.NoError(t, err)
	require.Equal(t, []byte("sealed shard plaintext"), gotSealed)
}
