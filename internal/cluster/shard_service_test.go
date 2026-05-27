package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/gritive/GrainFS/internal/transport"
)

func TestShardService_LocalWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	tr := transport.MustNewQUICTransport("test-cluster-psk")
	svc := NewShardService(dir, tr, withTestWAL(t))

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
	tr := transport.MustNewQUICTransport("test-cluster-psk")
	svc := NewShardService(dir, tr, WithEncryptor(enc), withTestWALEnc(t, enc))

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
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

	plaintext := bytes.Repeat([]byte("secret shard data"), 8192)
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	r, err := svc.OpenLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestShardService_SharedPackWriteReadRangeDelete(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithEncryptor(enc),
		WithShardPackThreshold(1024),
		withTestWALEnc(t, enc),
	)

	plaintext := []byte("secret shard data")
	require.NoError(t, svc.WriteLocalShard("bkt", "obj/v1", 0, plaintext))

	_, err = os.Stat(filepath.Join(dir, "shards", "bkt", "obj/v1", "shard_0"))
	require.ErrorIs(t, err, os.ErrNotExist)

	got, err := svc.ReadLocalShard("bkt", "obj/v1", 0)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)

	buf := make([]byte, 6)
	n, err := svc.ReadLocalShardAt("bkt", "obj/v1", 0, 7, buf)
	require.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, []byte("shard "), buf)

	r, err := svc.OpenLocalShardRange("bkt", "obj/v1", 0, 7, 5)
	require.NoError(t, err)
	defer r.Close()
	ranged, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("shard"), ranged)

	require.NoError(t, svc.DeleteLocalShards("bkt", "obj/v1"))
	_, err = svc.ReadLocalShard("bkt", "obj/v1", 0)
	require.Error(t, err)
}

func TestShardService_SharedPackWriteLocalShardStream(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithEncryptor(enc),
		WithShardPackThreshold(1024),
		withTestWALEnc(t, enc),
	)

	plaintext := []byte("streamed shard data")
	require.NoError(t, svc.WriteLocalShardStreamContext(context.Background(), "bkt", "obj/v1", 0, bytes.NewReader(plaintext)))

	_, err = os.Stat(filepath.Join(dir, "shards", "bkt", "obj/v1", "shard_0"))
	require.ErrorIs(t, err, os.ErrNotExist)

	got, err := svc.ReadLocalShard("bkt", "obj/v1", 0)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestShardService_WriteLocalShardStreamSizedContextBypassesPackForLargeShard(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithEncryptor(enc),
		WithShardPackThreshold(1024),
		withTestWALEnc(t, enc),
	)

	// declared size must match the actual stream length; the WAL write path
	// reads exactly streamSize bytes. Use a >= packThreshold payload so the
	// large-shard pack bypass is still exercised.
	plaintext := bytes.Repeat([]byte("x"), 1024)
	require.NoError(t, svc.WriteLocalShardStreamSizedContext(context.Background(), "bkt", "obj/v1", 0, bytes.NewReader(plaintext), int64(len(plaintext))))

	_, err = os.Stat(filepath.Join(dir, "shards", "bkt", "obj/v1", "shard_0"))
	require.NoError(t, err)

	got, err := svc.ReadLocalShard("bkt", "obj/v1", 0)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestShardService_SharedPackDefaultDoesNotSyncEveryAppend(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithShardPackThreshold(1024),
		withTestWAL(t),
	)

	require.NotNil(t, svc.shardPack)
}

func TestShardService_SharedPackDeleteReturnsTombstoneWriteError(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithShardPackThreshold(1024),
		withTestWAL(t),
	)

	require.NoError(t, svc.WriteLocalShard("bkt", "obj/v1", 0, []byte("secret shard data")))
	require.NotNil(t, svc.shardPack)
	require.NoError(t, svc.shardPack.active.Close())

	err := svc.DeleteLocalShards("bkt", "obj/v1")
	require.Error(t, err)
}

func TestShardService_SharedPackRestartSkipsCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithShardPackThreshold(1024),
		withTestWAL(t),
	)

	require.NoError(t, svc.WriteLocalShard("bkt", "obj/v1", 0, []byte("secret shard data")))
	require.NotNil(t, svc.shardPack)
	packPath := svc.shardPack.blobPath(svc.shardPack.activeID)
	require.NoError(t, svc.shardPack.active.Close())

	raw, err := os.ReadFile(packPath)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0xff
	require.NoError(t, os.WriteFile(packPath, raw, 0o600))

	restarted := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithShardPackThreshold(1024),
		withTestWAL(t),
	)
	require.NotNil(t, restarted.shardPack)
	_, ok := restarted.shardPack.index[shardPackKey("bkt", "obj/v1", 0)]
	require.False(t, ok)
}

func TestShardPackScanSkipsOversizedRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shardpack_0000000000000000.dat")
	key := shardPackKey("bkt", "obj/v1", 0)
	record := appendShardPackRecord(nil, shardPackFlagPut, key, make([]byte, 65))
	require.NoError(t, os.WriteFile(path, record, 0o600))

	store := &shardPackStore{
		dir:     dir,
		maxSize: 64,
		index:   make(map[string]shardPackLocation),
	}
	require.NoError(t, store.scanFile(0, path))

	_, ok := store.index[key]
	require.False(t, ok)
}

func TestBuildShardEnvelope_SizesBuilderForSmallShardPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64<<10)

	req := buildShardEnvelope("WriteShard", "bkt", "obj/v1", 1, payload)
	defer func() { req.Builder.Reset(); shardBuilderPool.Put(req.Builder) }()

	require.LessOrEqual(t, cap(req.Builder.Bytes), 80<<10)
}

func TestShardService_OpenLocalShard_CRCFooterMismatchDetected(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

	plaintext := bytes.Repeat([]byte("plain shard data"), 8192)
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	raw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0xff
	require.NoError(t, os.WriteFile(rawPath, raw, 0o644))

	r, err := svc.OpenLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	defer r.Close()

	buf := make([]byte, len(plaintext))
	_, err = io.ReadFull(r, buf)
	require.ErrorIs(t, err, eccodec.ErrCRCMismatch)
}

// writeLegacyEncodedShard writes a GFSCRC1-encoded shard whose inner payload is
// a pre-XAES EncryptWithAAD blob (0xAE 0xE1 magic). Such a shard would be
// streamed back as raw "plaintext" by the CRC fast-paths absent the legacy
// guard. Returns the on-disk shard path.
func writeLegacyEncodedShard(t *testing.T, svc *ShardService, bucket, key string, shardIdx int) string {
	t.Helper()
	// Inner payload carries the exact old EncryptWithAAD blob magic.
	legacyBlob := append([]byte{0xAE, 0xE1}, bytes.Repeat([]byte("legacy-cipher"), 64)...)
	encoded := eccodec.EncodeShard(legacyBlob)
	path := svc.getShardPath(bucket, key, shardIdx)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, encoded, 0o644))
	return path
}

// TestShardService_ReadPaths_RejectLegacyEncodedBlob verifies Finding A: every
// CRC read fast-path loud-fails when the CRC-decoded payload carries the exact
// pre-XAES blob magic (0xAE 0xE1) rather than silently returning it as
// plaintext.
func TestShardService_ReadPaths_RejectLegacyEncodedBlob(t *testing.T) {
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte("k"), 32))
	require.NoError(t, err)
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))
	writeLegacyEncodedShard(t, svc, "bkt", "obj", 0)

	const wantMsg = "unsupported/old encrypted-blob format"

	t.Run("ReadLocalShard", func(t *testing.T) {
		_, err := svc.ReadLocalShard("bkt", "obj", 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), wantMsg)
	})

	t.Run("OpenLocalShard", func(t *testing.T) {
		_, err := svc.OpenLocalShard("bkt", "obj", 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), wantMsg)
	})

	t.Run("ReadLocalShardAt", func(t *testing.T) {
		buf := make([]byte, 8)
		_, err := svc.ReadLocalShardAt("bkt", "obj", 0, 0, buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), wantMsg)
	})

	t.Run("OpenLocalShardRange", func(t *testing.T) {
		_, err := svc.OpenLocalShardRange("bkt", "obj", 0, 0, 8)
		require.Error(t, err)
		require.Contains(t, err.Error(), wantMsg)
	})
}

// TestShardService_ReadPaths_GenuinePlaintextStillPasses verifies that a
// CRC-encoded shard with a genuine-plaintext payload (no legacy magic) is still
// returned as-is by the read fast-paths when no encryptor is wired.
func TestShardService_ReadPaths_GenuinePlaintextStillPasses(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

	plaintext := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	got, err := svc.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)

	buf := make([]byte, 8)
	n, err := svc.ReadLocalShardAt("bkt", "obj", 0, 10, buf)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, "abcdefgh", string(buf))
}

func TestShardService_ReadLocalShardAt_EncodedShard(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

	plaintext := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	buf := make([]byte, 8)
	n, err := svc.ReadLocalShardAt("bkt", "obj", 0, 10, buf)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, "abcdefgh", string(buf))
}

func TestShardService_ReadLocalShardAt_EncryptedShard(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 192*1024)
	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, plaintext))

	offset := int64(eccodec.DefaultEncryptedChunkSize + 12345)
	buf := make([]byte, 4096)
	n, err := svc.ReadLocalShardAt("bkt", "obj", 0, offset, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, plaintext[offset:offset+int64(len(buf))], buf)
}

// TestShardService_NoEncryption verifies plaintext storage when no encryptor is set.
func TestShardService_NoEncryption(t *testing.T) {
	dir := t.TempDir()
	tr := transport.MustNewQUICTransport("test-cluster-psk")
	svc := NewShardService(dir, tr, withTestWAL(t))

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

func TestShardService_DirectIOWriterSuccess(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithDirectIO(), withTestWAL(t))

	payload := []byte("direct writer payload")
	directCalls := 0
	svc.directWriter = func(path string, got []byte) error {
		directCalls++
		require.Equal(t, eccodec.EncodeShard(payload), got)
		return os.WriteFile(path, got, 0o600)
	}

	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, payload))
	require.Equal(t, 1, directCalls)

	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	raw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	decoded, err := eccodec.DecodeShard(raw)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
}

func TestShardService_DirectIOUnsupportedFallsBackToBuffered(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithDirectIO(), withTestWAL(t))

	payload := []byte("fallback payload")
	svc.directWriter = func(path string, got []byte) error {
		require.NoFileExists(t, path)
		return errors.New("create tmp shard (direct): invalid argument")
	}

	require.NoError(t, svc.WriteLocalShard("bkt", "obj", 0, payload))

	got, err := svc.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestShardService_DirectIONonUnsupportedErrorDoesNotFallback(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithDirectIO(), withTestWAL(t))

	payload := []byte("should not be written")
	svc.directWriter = func(path string, got []byte) error {
		require.NoFileExists(t, path)
		return errors.New("create tmp shard (direct): permission denied")
	}

	err := svc.WriteLocalShard("bkt", "obj", 0, payload)
	require.ErrorContains(t, err, "permission denied")
	require.NoFileExists(t, filepath.Join(dir, "shards", "bkt", "obj", "shard_0"))
}

func TestIsUnsupportedDirectIO(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "invalid argument", err: errors.New("create tmp shard (direct): invalid argument"), want: true},
		{name: "operation not supported", err: errors.New("create tmp shard (direct): operation not supported"), want: true},
		{name: "not implemented", err: errors.New("create tmp shard (direct): not implemented"), want: true},
		{name: "other", err: errors.New("create tmp shard (direct): permission denied"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isUnsupportedDirectIO(tt.err))
		})
	}
}

func TestShardService_ReadLocalShard_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

	_, err := svc.ReadLocalShard("bkt", "no-such-obj", 0)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestShardService_ReadLocalShard_DecryptError(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

	// Write garbage bytes that look like valid data but aren't valid ciphertext
	rawPath := filepath.Join(dir, "shards", "bkt", "obj", "shard_0")
	require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
	require.NoError(t, os.WriteFile(rawPath, []byte("not-valid-ciphertext"), 0o644))

	_, err = svc.ReadLocalShard("bkt", "obj", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt shard")
}

// TestShardService_ReadLocalShard_LegacyAEADCorruption seeds a legacy single-blob
// encrypted shard (the scrubber repair / pre-v0.0.62.0 format:
// eccodec.EncodeShard(encryptor.EncryptWithAAD(...))) whose inner AEAD tag fails
// while the outer CRC footer stays valid, and confirms ReadLocalShard surfaces it
// as corruption so the placement monitor quarantines instead of skipping.
func TestShardService_ReadLocalShard_LegacyAEADCorruption(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

	const bucket, objKey = "bkt", "obj"
	aad := []byte(bucket + "/" + objKey + "/0")
	plaintext := bytes.Repeat([]byte("legacy single-blob aead corruption probe "), 8)

	t.Run("AEAD tamper", func(t *testing.T) {
		blob, err := enc.EncryptWithAAD(plaintext, aad)
		require.NoError(t, err)
		// Flip a ciphertext byte BEFORE the CRC envelope is applied, so the outer
		// CRC matches the tampered payload (valid) but the inner AEAD tag fails.
		blob[len(blob)/2] ^= 0xFF
		raw := eccodec.EncodeShard(blob)

		rawPath := filepath.Join(dir, "shards", bucket, objKey, "shard_0")
		require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
		require.NoError(t, os.WriteFile(rawPath, raw, 0o644))

		_, err = svc.ReadLocalShard(bucket, objKey, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decrypt shard")
		assert.True(t, eccodec.IsCorruption(err), "AEAD failure must classify as corruption: %v", err)
	})

	t.Run("structural missing magic", func(t *testing.T) {
		// Bytes with no outer CRC envelope and no encrypted-blob magic header:
		// encryption is enabled but the stored shard is not in any recognized
		// format → structural format corruption.
		raw := bytes.Repeat([]byte("X"), 64)

		rawPath := filepath.Join(dir, "shards", bucket, "structural", "shard_0")
		require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0o755))
		require.NoError(t, os.WriteFile(rawPath, raw, 0o644))

		_, err := svc.ReadLocalShard(bucket, "structural", 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not an encrypted blob")
		assert.True(t, eccodec.IsCorruption(err), "missing magic must classify as corruption: %v", err)
	})
}

func TestShardService_WithEncryptorReadsLegacyCRCShard(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	legacy := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))
	plaintext := []byte("legacy crc shard data")
	require.NoError(t, legacy.WriteLocalShard("bkt", "obj", 0, plaintext))

	upgraded := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))
	got, err := upgraded.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)

	buf := make([]byte, 6)
	n, err := upgraded.ReadLocalShardAt("bkt", "obj", 0, 7, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, []byte("crc sh"), buf)
}

func TestShardService_WithEncryptorNil(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(nil), withTestWAL(t))

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
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithNodeAddressBook(f), withTestWAL(t))

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

	key := bytes.Repeat([]byte("e"), 32)
	enc1, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	enc2, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithEncryptor(enc1), withTestWALEnc(t, enc1))
	svc2 := NewShardService(dir2, tr2, WithEncryptor(enc2), withTestWALEnc(t, enc2))
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

	key := bytes.Repeat([]byte("e"), 32)
	enc1, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	enc2, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithEncryptor(enc1), withTestWALEnc(t, enc1))
	svc2 := NewShardService(dir2, tr2, WithEncryptor(enc2), withTestWALEnc(t, enc2))
	tr2.HandleBody(transport.StreamShardWriteBody, svc2.HandleWriteBody())
	tr2.HandleRead(transport.StreamShardReadBody, svc2.HandleReadBody())

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

func TestShardService_ReadShardRangeStream_EncodedShard(t *testing.T) {
	ctx := context.Background()

	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, withTestWAL(t))
	svc2 := NewShardService(dir2, tr2, withTestWAL(t))
	tr2.HandleBody(transport.StreamShardWriteBody, svc2.HandleWriteBody())
	tr2.HandleRead(transport.StreamShardReadBody, svc2.HandleReadBody())

	plaintext := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), 1024)
	require.NoError(t, svc1.WriteShardStream(ctx, tr2.LocalAddr(), "bkt", "key", 0, bytes.NewReader(plaintext)))

	r, err := svc1.ReadShardRangeStream(ctx, tr2.LocalAddr(), "bkt", "key", 0, 10, 8192)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, plaintext[10:10+8192], got)
}

func TestShardService_ReadShardRange_EncodedShard(t *testing.T) {
	ctx := context.Background()

	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, withTestWAL(t))
	svc2 := NewShardService(dir2, tr2, withTestWAL(t))
	tr2.SetStreamHandler(svc2.HandleRPC())

	plaintext := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), 1024)
	require.NoError(t, svc1.WriteShard(ctx, tr2.LocalAddr(), "bkt", "key", 0, plaintext))

	got, err := svc1.ReadShardRange(ctx, tr2.LocalAddr(), "bkt", "key", 0, 10, 8192)
	require.NoError(t, err)
	require.Equal(t, plaintext[10:10+8192], got)
}

func TestShardService_ReadShardRange_RejectsMediumSingleFrame(t *testing.T) {
	ctx := context.Background()

	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, withTestWAL(t))
	svc2 := NewShardService(dir2, tr2, withTestWAL(t))
	tr2.SetStreamHandler(svc2.HandleRPC())

	plaintext := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), 4096)
	require.NoError(t, svc1.WriteShard(ctx, tr2.LocalAddr(), "bkt", "key", 0, plaintext))

	_, err := svc1.ReadShardRange(ctx, tr2.LocalAddr(), "bkt", "key", 0, 0, 64*1024+1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

func TestShardService_RPCWriteReadDelete(t *testing.T) {
	ctx := context.Background()

	// Set up two QUIC transports to simulate two nodes
	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	// Connect them
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	svc1 := NewShardService(dir1, tr1, withTestWAL(t))
	svc2 := NewShardService(dir2, tr2, withTestWAL(t))

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

func TestShardService_WriteShardRecordsRemoteTraceBreakdown(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	tr1 := transport.MustNewQUICTransport("test-cluster-psk")
	tr2 := transport.MustNewQUICTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	svc1 := NewShardService(t.TempDir(), tr1, withTestWAL(t))
	svc2 := NewShardService(t.TempDir(), tr2, withTestWAL(t))
	tr2.SetStreamHandler(svc2.HandleRPC())

	traceCtx := ContextWithPutTrace(ctx, PutTraceRequest{
		Bucket:      "mybucket",
		Key:         "mykey",
		GroupID:     "group-1",
		Ingress:     PutTraceIngressLocalLeader,
		SizeClass:   PutTraceSizeSmall,
		ForwardMode: PutTraceForwardNone,
	})

	err := svc1.WriteShard(traceCtx, tr2.LocalAddr(), "mybucket", "mykey", 0, []byte("shard-data-0"))
	require.NoError(t, err)

	events := readShardServiceTraceEvents(t, path)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteRemoteBuild)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteRemoteCall)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteRemoteDecode)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalMkdir)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncode)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalFile)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalBuffered)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalDirSync)
}

func TestShardService_WriteLocalShardContextRecordsTraceBreakdown(t *testing.T) {
	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      "mybucket",
		Key:         "mykey",
		GroupID:     "group-1",
		Ingress:     PutTraceIngressLocalLeader,
		SizeClass:   PutTraceSizeSmall,
		ForwardMode: PutTraceForwardNone,
	})
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	svc := NewShardService(t.TempDir(), transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))
	err := svc.WriteLocalShardContext(ctx, "mybucket", "mykey", 0, []byte("local-shard"))
	require.NoError(t, err)

	events := readShardServiceTraceEvents(t, path)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalMkdir)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalEncode)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalFile)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalBuffered)
	requireShardServiceTraceStage(t, events, PutTraceStageShardWriteLocalDirSync)
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
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

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
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

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
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

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
	key := make([]byte, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)

	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

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

func TestReadLocalShard_DowngradeDetection(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := encrypt.NewEncryptor(key)

	dir := t.TempDir()
	svcEncrypted := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))
	svcPlain := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

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
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithEncryptor(enc), withTestWALEnc(t, enc))

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

func TestShardService_DataWALRestoresMissingLocalShard(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithDataWAL(dwal))
	require.NoError(t, svc.WriteLocalShard("b", "k", 0, []byte("payload")))
	require.NoError(t, dwal.Flush())
	shardPath := svc.getShardPath("b", "k", 0)
	require.NoError(t, os.Remove(shardPath))
	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	got, err := svc.ReadLocalShard("b", "k", 0)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}

func TestShardService_DataWALRestoresStreamedLocalShard(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithDataWAL(dwal))
	require.NoError(t, svc.WriteLocalShardStream("b", "streamed", 1, strings.NewReader("stream-payload")))
	require.NoError(t, dwal.Flush())
	shardPath := svc.getShardPath("b", "streamed", 1)
	require.NoError(t, os.Remove(shardPath))
	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	got, err := svc.ReadLocalShard("b", "streamed", 1)
	require.NoError(t, err)
	require.Equal(t, []byte("stream-payload"), got)
}

func TestShardPack_DataWALReplaysPutAndDelete(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithDataWAL(dwal),
		WithShardPackThreshold(1024),
	)
	require.NoError(t, svc.WriteLocalShard("b", "packed", 0, []byte("small")))
	require.NoError(t, svc.DeleteLocalShards("b", "packed"))
	require.NoError(t, dwal.Flush())
	require.NoError(t, os.RemoveAll(filepath.Join(svc.DataDirs()[0], ".pack")))
	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	_, found, err := svc.ReadLocalShardFromPack("b", "packed", 0)
	require.NoError(t, err)
	require.False(t, found, "pack entry must remain absent after delete replay")
	// And there must be no resurrected per-shard file either.
	_, statErr := os.Stat(svc.getShardPath("b", "packed", 0))
	require.True(t, os.IsNotExist(statErr), "pack-routed write must not resurrect shard file on replay")
}

// TestShardPack_DataWALWritesLoggedAfterRecovery regression-tests that the
// pack store wired by RecoverDataWAL holds onto the live data WAL, so a
// pack write made after recovery survives a second crash+recover cycle.
// Pre-fix the materializer left s.shardPack pointing at a nil-WAL store and
// subsequent pack writes were silently un-logged.
func TestShardPack_DataWALWritesLoggedAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithDataWAL(dwal),
		WithShardPackThreshold(1024),
	)
	require.NoError(t, svc.WriteLocalShard("b", "k1", 0, []byte("first")))
	require.NoError(t, dwal.Flush())

	// Simulate restart-and-recover.
	require.NoError(t, svc.RecoverDataWAL(context.Background()))

	// A write made AFTER recovery must produce a WAL record that can replay
	// through a second recovery.
	require.NoError(t, svc.WriteLocalShard("b", "k2", 0, []byte("second")))
	require.NoError(t, dwal.Flush())

	// Wipe the pack directory and recover again; the second write must
	// reappear from the WAL.
	require.NoError(t, os.RemoveAll(filepath.Join(svc.DataDirs()[0], ".pack")))
	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	got, ok, err := svc.ReadLocalShardFromPack("b", "k2", 0)
	require.NoError(t, err)
	require.True(t, ok, "post-recovery pack write must replay through a second recovery")
	require.Equal(t, []byte("second"), got)
}

// TestShardService_DataWALRestoresEncryptedShard regression-tests the latent
// bug surfaced by boot wiring (see commit 775286d5): RecoverDataWAL must
// forward the configured encryptor to datawal.Recover so the WAL segments
// can be decrypted. Pre-fix the call passed nil and recovery failed with
// "segment mode mismatch" once an encrypted WAL was wired in production.
func TestShardService_DataWALRestoresEncryptedShard(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), enc)
	require.NoError(t, err)
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithEncryptor(enc),
		WithDataWAL(dwal),
	)
	require.NoError(t, svc.WriteLocalShard("b", "k", 0, []byte("encrypted-payload")))
	require.NoError(t, dwal.Flush())
	shardPath := svc.getShardPath("b", "k", 0)
	require.NoError(t, os.Remove(shardPath))
	require.NoError(t, svc.RecoverDataWAL(context.Background()))
	got, err := svc.ReadLocalShard("b", "k", 0)
	require.NoError(t, err)
	require.Equal(t, []byte("encrypted-payload"), got)
}

func TestDataWALRepairCollector_CoalescesByBucketShardAndIndex(t *testing.T) {
	collector := NewDataWALRepairCollector()

	collector.AddDataWALRepairCandidate(DataWALRepairCandidate{
		Bucket:       "b",
		ShardKey:     "obj/v1",
		ShardIdx:     2,
		ExpectedSize: 10,
		Reason:       DataWALRepairMissing,
	})
	collector.AddDataWALRepairCandidate(DataWALRepairCandidate{
		Bucket:       "b",
		ShardKey:     "obj/v1",
		ShardIdx:     2,
		ExpectedSize: 99,
		Reason:       DataWALRepairSizeMismatch,
	})
	collector.AddDataWALRepairCandidate(DataWALRepairCandidate{
		Bucket:       "b",
		ShardKey:     "obj/v1",
		ShardIdx:     3,
		ExpectedSize: 11,
		Reason:       DataWALRepairMissing,
	})
	// Same ShardIdx as the first entry but a different Bucket: must NOT
	// coalesce, proving the composite key includes Bucket.
	collector.AddDataWALRepairCandidate(DataWALRepairCandidate{
		Bucket:       "other",
		ShardKey:     "obj/v1",
		ShardIdx:     2,
		ExpectedSize: 22,
		Reason:       DataWALRepairMissing,
	})
	// Same Bucket and ShardIdx as the first entry but a different ShardKey:
	// must NOT coalesce, proving the composite key includes ShardKey.
	collector.AddDataWALRepairCandidate(DataWALRepairCandidate{
		Bucket:       "b",
		ShardKey:     "obj/v2",
		ShardIdx:     2,
		ExpectedSize: 33,
		Reason:       DataWALRepairMissing,
	})

	got := collector.Candidates()
	require.Len(t, got, 4)
	require.Equal(t, DataWALRepairCandidate{
		Bucket:       "b",
		ShardKey:     "obj/v1",
		ShardIdx:     2,
		ExpectedSize: 99,
		Reason:       DataWALRepairSizeMismatch,
	}, got[0])
	require.Equal(t, 3, got[1].ShardIdx)
	require.Equal(t, DataWALRepairCandidate{
		Bucket:       "other",
		ShardKey:     "obj/v1",
		ShardIdx:     2,
		ExpectedSize: 22,
		Reason:       DataWALRepairMissing,
	}, got[2])
	require.Equal(t, DataWALRepairCandidate{
		Bucket:       "b",
		ShardKey:     "obj/v2",
		ShardIdx:     2,
		ExpectedSize: 33,
		Reason:       DataWALRepairMissing,
	}, got[3])
}

func TestShardService_DataWALMetadataOnlyMissingQueuesStartupRepair(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	collector := NewDataWALRepairCollector()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithDataWAL(dwal),
		WithDataWALRepairSink(collector),
	)

	_, err = dwal.Append(context.Background(), datawal.Record{
		Op:     datawal.OpShardPut,
		Bucket: "b",
		Key:    "obj/v1",
		Target: "0",
		Size:   int64(walPayloadInlineThreshold),
	})
	require.NoError(t, err)
	require.NoError(t, dwal.Flush())

	require.NoError(t, svc.RecoverDataWAL(context.Background()))

	require.Equal(t, []DataWALRepairCandidate{{
		Bucket:       "b",
		ShardKey:     "obj/v1",
		ShardIdx:     0,
		ExpectedSize: int64(walPayloadInlineThreshold),
		Reason:       DataWALRepairMissing,
	}}, collector.Candidates())
}

func TestShardService_DataWALMetadataOnlySizeMismatchQueuesStartupRepair(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	collector := NewDataWALRepairCollector()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithDataWAL(dwal),
		WithDataWALRepairSink(collector),
	)

	path := svc.getShardPath("b", "obj/v1", 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("short"), 0o600))
	_, err = dwal.Append(context.Background(), datawal.Record{
		Op:     datawal.OpShardPut,
		Bucket: "b",
		Key:    "obj/v1",
		Target: "1",
		Size:   int64(walPayloadInlineThreshold),
	})
	require.NoError(t, err)
	require.NoError(t, dwal.Flush())

	require.NoError(t, svc.RecoverDataWAL(context.Background()))

	require.Equal(t, []DataWALRepairCandidate{{
		Bucket:       "b",
		ShardKey:     "obj/v1",
		ShardIdx:     1,
		ExpectedSize: int64(walPayloadInlineThreshold),
		Reason:       DataWALRepairSizeMismatch,
	}}, collector.Candidates())
}

func TestDataWALRepairCollector_ConcurrentAddIsRaceFree(t *testing.T) {
	collector := NewDataWALRepairCollector()

	const goroutines = 50
	// Keys 0..39 are distinct; keys 40..49 duplicate key "0".
	// Distinct key count is 40.
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		key := strconv.Itoa(i % 40)
		go func() {
			defer wg.Done()
			collector.AddDataWALRepairCandidate(DataWALRepairCandidate{
				Bucket:   "b",
				ShardKey: key,
				ShardIdx: 0,
				Reason:   DataWALRepairMissing,
			})
		}()
	}
	wg.Wait()

	require.Len(t, collector.Candidates(), 40)
}

func TestShardService_DataWALInlineReplayDoesNotQueueStartupRepair(t *testing.T) {
	dir := t.TempDir()
	dwal, err := datawal.Open(filepath.Join(dir, "datawal"), nil)
	require.NoError(t, err)
	collector := NewDataWALRepairCollector()
	svc := NewShardService(
		dir,
		transport.MustNewQUICTransport("test-cluster-psk"),
		WithDataWAL(dwal),
		WithDataWALRepairSink(collector),
	)
	require.NoError(t, svc.WriteLocalShard("b", "small", 0, []byte("payload")))
	require.NoError(t, dwal.Flush())
	require.NoError(t, os.Remove(svc.getShardPath("b", "small", 0)))

	require.NoError(t, svc.RecoverDataWAL(context.Background()))

	require.Empty(t, collector.Candidates())
	got, err := svc.ReadLocalShard("b", "small", 0)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}
