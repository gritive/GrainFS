package cluster

import (
	"bytes"
	"context"
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
	tr := transport.NewQUICTransport()
	svc := NewShardService(dir, tr)

	// Verify shards directory created
	_, err := os.Stat(filepath.Join(dir, "shards"))
	require.NoError(t, err)

	// Write a shard locally via handleRPC
	handler := svc.HandleRPC()
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

// TestShardService_Encryption verifies that shards written with an encryptor
// are NOT stored as plaintext on disk, and can be decrypted on read.
func TestShardService_Encryption(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	tr := transport.NewQUICTransport()
	svc := NewShardService(dir, tr, WithEncryptor(enc))

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

// TestShardService_NoEncryption verifies plaintext storage when no encryptor is set.
func TestShardService_NoEncryption(t *testing.T) {
	dir := t.TempDir()
	tr := transport.NewQUICTransport()
	svc := NewShardService(dir, tr)

	plaintext := []byte("plain shard data")
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	raw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	assert.True(t, eccodec.IsEncodedShard(raw), "new shards should carry CRC envelope")
	decoded, err := eccodec.DecodeShard(raw)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded, "without encryptor, CRC payload should be plaintext")
}

func TestShardService_ReadLocalShard_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.NewQUICTransport())

	_, err := svc.ReadLocalShard("bkt", "no-such-obj", 0)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestShardService_ReadLocalShard_DecryptError(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.NewQUICTransport(), WithEncryptor(enc))

	// Write garbage bytes that look like valid data but aren't valid ciphertext
	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
	require.NoError(t, os.WriteFile(rawPath, []byte("not-valid-ciphertext"), 0o644))

	_, err = svc.ReadLocalShard("bkt", "obj", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt shard")
}

func TestShardService_WithEncryptorNil(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.NewQUICTransport(), WithEncryptor(nil))

	plaintext := []byte("plain data with nil encryptor")
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	got, err := svc.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestShardService_ResolvePeerAddress(t *testing.T) {
	dir := t.TempDir()
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-a", "10.0.0.1:7001", 0)))
	svc := NewShardService(dir, transport.NewQUICTransport(), WithNodeAddressBook(f))

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

	key := bytes.Repeat([]byte("e"), 32)
	enc1, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	enc2, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	tr1 := transport.NewQUICTransport()
	tr2 := transport.NewQUICTransport()
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithEncryptor(enc1))
	svc2 := NewShardService(dir2, tr2, WithEncryptor(enc2))
	tr2.SetStreamHandler(svc2.HandleRPC())

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
}

func TestShardService_RPCWriteReadDelete(t *testing.T) {
	ctx := context.Background()

	// Set up two QUIC transports to simulate two nodes
	tr1 := transport.NewQUICTransport()
	tr2 := transport.NewQUICTransport()
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	// Connect them
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	svc1 := NewShardService(dir1, tr1)
	svc2 := NewShardService(dir2, tr2)

	// Set tr2's stream handler to svc2's handler (simulating node2's shard server)
	tr2.SetStreamHandler(svc2.HandleRPC())

	// Node1 writes a shard to Node2
	err := svc1.WriteShard(ctx, tr2.LocalAddr(), "mybucket", "mykey", 0, []byte("shard-data-0"))
	require.NoError(t, err)

	// Verify shard landed on Node2's disk
	shardPath := filepath.Join(dir2, "shards", "mybucket", "mykey", "shard_0")
	data, err := os.ReadFile(shardPath)
	require.NoError(t, err)
	decoded, err := eccodec.DecodeShard(data)
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

// TestWriteLocalShard_Atomic verifies that WriteLocalShard is crash-safe:
//  1. Successful writes leave no .tmp garbage.
//  2. Overwriting an existing shard produces correct final content.
//  3. The original shard is not modified when the write fails (parent dir
//     is made non-writable to force an error before rename).
func TestWriteLocalShard_Atomic(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.NewQUICTransport())

	data := []byte("atomic-shard-payload")
	require.NoError(t, svc.WriteLocalShard("bkt", "key/v1", 0, data))

	shardPath := filepath.Join(dir, "shards", "bkt", "key/v1", "shard_0")
	tmpPath := shardPath + ".tmp"

	// Final shard must exist with correct content.
	got, err := os.ReadFile(shardPath)
	require.NoError(t, err)
	decoded, err := eccodec.DecodeShard(got)
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
	svc := NewShardService(dir, transport.NewQUICTransport())

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
	got, readErr := os.ReadFile(shardPath)
	require.NoError(t, readErr)
	decoded, err := eccodec.DecodeShard(got)
	require.NoError(t, err)
	assert.Equal(t, original, decoded, "original shard must survive a failed overwrite")
}

func TestWriteReadLocalShard_Encrypted_AAD(t *testing.T) {
	key := make([]byte, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.NewQUICTransport(), WithEncryptor(enc))

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
	assert.True(t, eccodec.IsEncodedShard(raw), "shard must have CRC envelope")
	encodedPayload, err := eccodec.DecodeShard(raw)
	require.NoError(t, err)
	assert.True(t, encrypt.IsEncryptedBlob(encodedPayload), "CRC payload must be encrypted")
}

func TestReadLocalShard_DowngradeDetection(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := encrypt.NewEncryptor(key)

	dir := t.TempDir()
	svcEncrypted := NewShardService(dir, transport.NewQUICTransport(), WithEncryptor(enc))
	svcPlain := NewShardService(dir, transport.NewQUICTransport())

	// Write with encryption.
	require.NoError(t, svcEncrypted.WriteLocalShard("b", "k", 0, []byte("secret")))

	// Reading without encryption must fail with a clear error.
	_, err := svcPlain.ReadLocalShard("b", "k", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encrypted", "error must mention encryption")
}

func TestWriteLocalShard_AAD_LocationBinding(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := encrypt.NewEncryptor(key)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.NewQUICTransport(), WithEncryptor(enc))

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
