package packblob

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobStore_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024) // 256MB max
	require.NoError(t, err)
	defer bs.Close()

	data := []byte("hello packed blob")
	loc, err := bs.Append("bucket/key1", data)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), loc.Offset) // first entry starts at offset 0
	assert.Equal(t, uint32(len(data)), loc.Length)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBlobStore_MultipleAppends(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	entries := []string{"first", "second", "third"}
	var locs []BlobLocation
	for i, e := range entries {
		loc, err := bs.Append("bucket/key"+string(rune('0'+i)), []byte(e))
		require.NoError(t, err)
		locs = append(locs, loc)
	}

	for i, loc := range locs {
		got, err := bs.Read(loc)
		require.NoError(t, err)
		assert.Equal(t, entries[i], string(got))
	}
}

func TestBlobStore_CRCValidation(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)

	loc, err := bs.Append("bucket/key", []byte("valid data"))
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Equal(t, "valid data", string(got))

	bs.Close()
}

func TestBlobStore_RotatesOnMaxSize(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 100) // tiny max for rotation testing
	require.NoError(t, err)
	defer bs.Close()

	data := bytes.Repeat([]byte("X"), 50)
	loc1, err := bs.Append("bucket/key1", data)
	require.NoError(t, err)

	loc2, err := bs.Append("bucket/key2", data)
	require.NoError(t, err)

	// Should be in different blob files
	assert.NotEqual(t, loc1.BlobID, loc2.BlobID)

	// Both readable
	got1, err := bs.Read(loc1)
	require.NoError(t, err)
	assert.Equal(t, data, got1)

	got2, err := bs.Read(loc2)
	require.NoError(t, err)
	assert.Equal(t, data, got2)
}

func TestBlobStore_EmptyData(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	loc, err := bs.Append("bucket/empty", []byte{})
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestEncryptedBlobStoreHidesPayload(t *testing.T) {
	enc := newPackblobTestEncryptor(t)

	dir := t.TempDir()
	bs, err := NewEncryptedBlobStore(dir, 256*1024*1024, enc)
	require.NoError(t, err)
	defer bs.Close()

	plaintext := []byte("packed-sensitive-payload")
	loc, err := bs.Append("bucket/key", plaintext)
	require.NoError(t, err)

	raw, err := os.ReadFile(bs.blobPath(loc.BlobID))
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(plaintext))

	got, err := bs.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedBlobStoreCompressionRoundTrip(t *testing.T) {
	enc := newPackblobTestEncryptor(t)

	bs, err := NewEncryptedBlobStore(t.TempDir(), 256*1024*1024, enc)
	require.NoError(t, err)
	defer bs.Close()
	bs.EnableCompression()

	plaintext := bytes.Repeat([]byte("compressible-packed-sensitive-payload-"), 1024)
	loc, err := bs.Append("bucket/key", plaintext)
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedBlobStoreRejectsKeyRemap(t *testing.T) {
	enc := newPackblobTestEncryptor(t)

	dir := t.TempDir()
	bs, err := NewEncryptedBlobStore(dir, 256*1024*1024, enc)
	require.NoError(t, err)
	defer bs.Close()

	loc, err := bs.Append("bucket/key", []byte("packed-sensitive-payload"))
	require.NoError(t, err)

	path := bs.blobPath(loc.BlobID)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	keyLen := binary.BigEndian.Uint32(raw[loc.Offset:])
	keyStart := int(loc.Offset) + 4
	keyEnd := keyStart + int(keyLen)
	require.Equal(t, "bucket/key", string(raw[keyStart:keyEnd]))
	copy(raw[keyStart:keyEnd], []byte("bucket/kex"))

	flagsOff := keyEnd
	dataLenOff := flagsOff + 1
	dataLen := binary.BigEndian.Uint32(raw[dataLenOff:])
	payloadStart := dataLenOff + 4
	payloadEnd := payloadStart + int(dataLen)
	crcOff := payloadEnd

	h := crc32.NewIEEE()
	h.Write(raw[keyStart:keyEnd])
	h.Write(raw[payloadStart:payloadEnd])
	binary.BigEndian.PutUint32(raw[crcOff:], h.Sum32())
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = bs.Read(loc)
	require.Error(t, err)
}

func TestEncryptedBlobStoreRejectsEncryptedFlagDowngrade(t *testing.T) {
	enc := newPackblobTestEncryptor(t)

	dir := t.TempDir()
	bs, err := NewEncryptedBlobStore(dir, 256*1024*1024, enc)
	require.NoError(t, err)
	defer bs.Close()

	loc, err := bs.Append("bucket/key", []byte("packed-sensitive-payload"))
	require.NoError(t, err)

	path := bs.blobPath(loc.BlobID)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	keyLen := binary.BigEndian.Uint32(raw[loc.Offset:])
	keyStart := int(loc.Offset) + 4
	keyEnd := keyStart + int(keyLen)
	flagsOff := keyEnd
	require.Equal(t, flagEncrypted, raw[flagsOff]&flagEncrypted)
	raw[flagsOff] &^= flagEncrypted

	dataLenOff := flagsOff + 1
	dataLen := binary.BigEndian.Uint32(raw[dataLenOff:])
	payloadStart := dataLenOff + 4
	payloadEnd := payloadStart + int(dataLen)
	crcOff := payloadEnd

	h := crc32.NewIEEE()
	h.Write(raw[keyStart:keyEnd])
	h.Write(raw[payloadStart:payloadEnd])
	binary.BigEndian.PutUint32(raw[crcOff:], h.Sum32())
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = bs.Read(loc)
	require.Error(t, err)
}

func TestEncryptedBlobStoreRejectsCompressedEncryptedFlagDowngrade(t *testing.T) {
	enc := newPackblobTestEncryptor(t)

	dir := t.TempDir()
	bs, err := NewEncryptedBlobStore(dir, 256*1024*1024, enc)
	require.NoError(t, err)
	defer bs.Close()
	bs.EnableCompression()

	loc, err := bs.Append("bucket/key", bytes.Repeat([]byte("packed-sensitive-payload-"), 1024))
	require.NoError(t, err)

	path := bs.blobPath(loc.BlobID)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	keyLen := binary.BigEndian.Uint32(raw[loc.Offset:])
	keyStart := int(loc.Offset) + 4
	keyEnd := keyStart + int(keyLen)
	flagsOff := keyEnd
	require.Equal(t, flagCompressed|flagEncrypted, raw[flagsOff])
	raw[flagsOff] = 0

	dataLenOff := flagsOff + 1
	dataLen := binary.BigEndian.Uint32(raw[dataLenOff:])
	payloadStart := dataLenOff + 4
	payloadEnd := payloadStart + int(dataLen)
	crcOff := payloadEnd
	binary.BigEndian.PutUint32(raw[crcOff:], blobEntryCRC(raw[keyStart:keyEnd], raw[flagsOff], raw[payloadStart:payloadEnd]))
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = bs.Read(loc)
	require.Error(t, err)
}

func TestEncryptedBlobStoreReadsLegacyPlaintextEntry(t *testing.T) {
	dir := t.TempDir()
	legacy, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	plaintext := []byte("legacy packed payload")
	loc, err := legacy.Append("bucket/key", plaintext)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	encrypted, err := NewEncryptedBlobStore(dir, 256*1024*1024, newPackblobTestEncryptor(t))
	require.NoError(t, err)
	defer encrypted.Close()

	got, err := encrypted.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedBlobStoreReadsLegacyPlaintextEntryWithEncryptedMagicPrefix(t *testing.T) {
	dir := t.TempDir()
	legacy, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	plaintext := []byte{0xAE, 0xE2, 0x01, 'l', 'e', 'g', 'a', 'c', 'y'}
	loc, err := legacy.Append("bucket/key", plaintext)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	encrypted, err := NewEncryptedBlobStore(dir, 256*1024*1024, newPackblobTestEncryptor(t))
	require.NoError(t, err)
	defer encrypted.Close()

	got, err := encrypted.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func newPackblobTestEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0x66}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}
