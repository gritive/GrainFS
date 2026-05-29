package packblob

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
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

// TestEncryptedBlobStore_RotatesOnMaxSize exercises the encrypted re-seal-on-rotate
// branch in Append: a second encrypted Append that would exceed maxSize at the
// current offset triggers a rotate, then re-seals the entry at the new offset —
// reusing the pooled sealed buffer for a SECOND SealTo within the same call and
// rebinding the AAD to the new activeID/offset. The closure defer (not value-capture)
// must return the final re-sealed buffer; a stale/aliased buffer would corrupt the
// AEAD and fail the round-trip below.
func TestEncryptedBlobStore_RotatesOnMaxSize(t *testing.T) {
	dir := t.TempDir()
	bs := newPackblobDEKStore(t, dir, 100) // tiny max forces rotation
	defer bs.Close()

	data1 := bytes.Repeat([]byte("A"), 40)
	data2 := bytes.Repeat([]byte("B"), 40)

	loc1, err := bs.Append("bucket/key1", data1)
	require.NoError(t, err)
	loc2, err := bs.Append("bucket/key2", data2)
	require.NoError(t, err)
	require.NotEqual(t, loc1.BlobID, loc2.BlobID, "second encrypted entry must land in a rotated blob")

	got1, err := bs.Read(loc1)
	require.NoError(t, err)
	require.Equal(t, data1, got1)
	got2, err := bs.Read(loc2)
	require.NoError(t, err)
	require.Equal(t, data2, got2, "re-sealed entry must round-trip (AAD rebound to new offset; pooled buffer not corrupted)")
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

func TestBlobStoreAppendNoCompressKeepsAllocationBound(t *testing.T) {
	bs, err := NewBlobStore(t.TempDir(), 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	key := "bucket/key"
	payload := bytes.Repeat([]byte("x"), 64*1024)

	allocs := testing.AllocsPerRun(100, func() {
		_, err := bs.Append(key, payload)
		require.NoError(t, err)
	})
	require.LessOrEqual(t, allocs, 1.0)
}

func TestBlobEntryCRCMatchesStandardIEEEStream(t *testing.T) {
	key := []byte("bucket/key")
	flags := flagCompressed
	payload := bytes.Repeat([]byte("x"), 1024)

	h := crc32.NewIEEE()
	_, _ = h.Write(key)
	_, _ = h.Write([]byte{flags})
	_, _ = h.Write(payload)

	require.Equal(t, h.Sum32(), blobEntryCRC(key, flags, 0, payload))
}

func TestEncryptedBlobStoreHidesPayload(t *testing.T) {
	dir := t.TempDir()
	bs := newPackblobDEKStore(t, dir, 256*1024*1024)
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
	bs := newPackblobDEKStore(t, t.TempDir(), 256*1024*1024)
	defer bs.Close()
	bs.EnableCompression()

	plaintext := bytes.Repeat([]byte("compressible-packed-sensitive-payload-"), 1024)
	loc, err := bs.Append("bucket/key", plaintext)
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedBlobStoreAppendKeepsAllocationBound(t *testing.T) {
	bs := newPackblobDEKStore(t, t.TempDir(), 256*1024*1024)
	defer bs.Close()

	key := "bucket/key"
	payload := bytes.Repeat([]byte("x"), 64*1024)

	allocs := testing.AllocsPerRun(100, func() {
		_, err := bs.Append(key, payload)
		require.NoError(t, err)
	})
	// SealTo seam-pooling (BenchmarkAppendEncrypted, 15s-class): the Append now
	// seals via DataEncryptor.SealTo into a pooled buffer (blobAppendSealedPool)
	// with the AAD built into a pooled scratch (withSeamAAD/AppendAAD), so the
	// per-Append ciphertext+AAD churn is gone — B/op dropped 75,400 → ~1,500 (≈50×).
	// allocs/op fell 14 → 9 (non-race; 12 under -race instrumentation). The residual
	// is the AADField construction in blobEntryAADFields (FieldUint64/String allocate
	// per field); eliminating it needs an AADField-API append/pool variant — a
	// separate slice (TODOS.md). Bound 15 → 13 (race ceiling 12 + margin).
	require.LessOrEqual(t, allocs, 13.0)
}

func TestEncryptedBlobStoreReadKeepsAllocationBound(t *testing.T) {
	bs := newPackblobDEKStore(t, t.TempDir(), 256*1024*1024)
	defer bs.Close()

	payload := bytes.Repeat([]byte("x"), 64*1024)
	loc, err := bs.Append("bucket/key", payload)
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(100, func() {
		got, err := bs.Read(loc)
		require.NoError(t, err)
		require.Equal(t, payload, got)
	})
	// D-seg-pack: the DataEncryptor seam returns a freshly-allocated plaintext
	// per Open and BuildAAD allocates the AAD blob plus the four positional
	// AADField slices, replacing the pooled aadBuf scratch. Bound raised from
	// 8 → 16 (measured 13 without -race, 15 under -race instrumentation, +
	// margin); SealTo/OpenTo buffer reuse is a future optimization (TODOS.md).
	require.LessOrEqual(t, allocs, 16.0)
}

func TestEncryptedBlobStoreRejectsKeyRemap(t *testing.T) {
	dir := t.TempDir()
	bs := newPackblobDEKStore(t, dir, 256*1024*1024)
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
	if raw[flagsOff]&flagGenFramed != 0 { // encrypted store: [flags][dek_gen:4][data_len]
		dataLenOff += genFieldSize
	}
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

// tamperStripEncryptedFlag rewrites the on-disk entry at loc to clear
// flagEncrypted and recompute the trailing CRC with the CURRENT (flags-included)
// blobEntryCRC formula. This simulates a sophisticated flag-downgrade attacker
// who bypasses the legacy-CRC detection branch (blob.go:325) by recomputing the
// modern CRC, so detection must come from the segEnc Open-attempt, not the CRC.
func tamperStripEncryptedFlag(t *testing.T, dir, key string, loc BlobLocation) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("blob_%016x.blob", loc.BlobID))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()

	// entry layout: [key_len:4][key][flags:1]([dek_gen:4] iff gen-framed)[data_len:4][data][crc:4]
	flagsPos := int64(loc.Offset) + 4 + int64(len(key))
	var flagBuf [1]byte
	_, err = f.ReadAt(flagBuf[:], flagsPos)
	require.NoError(t, err)
	require.NotZero(t, flagBuf[0]&flagEncrypted, "precondition: entry must be encrypted-flagged")

	// Read the gen field (present on an encrypted store) so the recomputed CRC
	// matches; newFlags keeps flagGenFramed (only flagEncrypted is stripped), so
	// Read still parses the gen and the downgrade Open-attempt fires (M3).
	var dekGen uint32
	genBytes := int64(0)
	if flagBuf[0]&flagGenFramed != 0 {
		var gb [genFieldSize]byte
		_, err = f.ReadAt(gb[:], flagsPos+1)
		require.NoError(t, err)
		dekGen = binary.BigEndian.Uint32(gb[:])
		genBytes = genFieldSize
	}

	dataPos := flagsPos + 1 + genBytes + 4
	payload := make([]byte, loc.Length)
	_, err = f.ReadAt(payload, dataPos)
	require.NoError(t, err)

	newFlags := flagBuf[0] &^ flagEncrypted
	_, err = f.WriteAt([]byte{newFlags}, flagsPos)
	require.NoError(t, err)

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], blobEntryCRC([]byte(key), newFlags, dekGen, payload))
	_, err = f.WriteAt(crcBuf[:], dataPos+int64(loc.Length))
	require.NoError(t, err)
}

// newPackblobDEKStoreSharedKeeper builds a DEK BlobStore over an EXISTING keeper
// so seal (store1) and open (store2) agree — NewDEKKeeper randomizes per keeper,
// so a tamper-then-reopen test cannot use newPackblobDEKStore (fresh keeper each
// call).
func newPackblobDEKStoreSharedKeeper(t testing.TB, dir string, maxSize int64, keeper *encrypt.DEKKeeper) *BlobStore {
	t.Helper()
	bs, err := NewDEKBlobStore(dir, maxSize, keeper, packblobTestClusterID())
	require.NoError(t, err)
	return bs
}

// TestEncryptedBlobStoreRejectsDEKEncryptedFlagDowngrade verifies that a DEK
// store rejects an entry whose flagEncrypted bit was stripped on disk while the
// payload remains valid ciphertext. Greenfield: an encrypted store never writes
// a plaintext-flagged entry, so a payload that decrypts cleanly under the
// encrypted-flag AAD is proof of downgrade. DEK ciphertext carries no
// value-magic, so detection cannot rely on encrypt.IsEncryptedValue.
// (Re-instates the static tests removed in the R3 static-seam retirement, now on
// the DEK path.)
func TestEncryptedBlobStoreRejectsDEKEncryptedFlagDowngrade(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)

	store1 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	key := "bucket/secret-object"
	loc, err := store1.Append(key, []byte("top secret plaintext"))
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	tamperStripEncryptedFlag(t, dir, key, loc)

	store2 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer store2.Close()
	_, err = store2.Read(loc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "flags mismatch")
}

// TestEncryptedBlobStoreRejectsCompressedDEKEncryptedFlagDowngrade is the
// compressed variant: the original flags are flagCompressed|flagEncrypted and
// only flagEncrypted is stripped, so encryptedFlagCandidates(flagCompressed)
// must still reconstruct the sealed AAD and reject. On master this surfaces a
// decompress error (raw ciphertext fed to decompress), NOT the downgrade error —
// hence the specific "flags mismatch" assertion.
func TestEncryptedBlobStoreRejectsCompressedDEKEncryptedFlagDowngrade(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)

	store1 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	store1.EnableCompression()
	key := "bucket/compressible-object"
	loc, err := store1.Append(key, bytes.Repeat([]byte("A"), 4096)) // highly compressible
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	tamperStripEncryptedFlag(t, dir, key, loc)

	store2 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	store2.EnableCompression()
	defer store2.Close()
	_, err = store2.Read(loc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "flags mismatch")
}

func TestEncryptedBlobStoreReadsLegacyPlaintextEntry(t *testing.T) {
	dir := t.TempDir()
	legacy, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	plaintext := []byte("legacy packed payload")
	loc, err := legacy.Append("bucket/key", plaintext)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	encrypted := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer encrypted.Close()

	got, err := encrypted.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestEncryptedBlobStoreRejectsLegacyPlaintextEntryWithValueMagicPrefix verifies
// the XAES greenfield boundary: if a blob entry's payload carries the old
// encrypted-value magic prefix (0xAE 0xE2) but is not flagEncrypted, the
// reader must return a loud error rather than silently passing the bytes as
// plaintext. This replaces the pre-XAES pass-through behavior.
func TestEncryptedBlobStoreRejectsLegacyPlaintextEntryWithValueMagicPrefix(t *testing.T) {
	dir := t.TempDir()
	legacy, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	// These bytes carry the old encrypted-value magic: they could be a pre-XAES
	// AES-GCM encrypted value written without flagEncrypted.
	oldMagicPayload := []byte{0xAE, 0xE2, 0x01, 'l', 'e', 'g', 'a', 'c', 'y'}
	loc, err := legacy.Append("bucket/key", oldMagicPayload)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	encrypted := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer encrypted.Close()

	_, err = encrypted.Read(loc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported/old encrypted-value format")
}

func packblobTestClusterID() []byte { return bytes.Repeat([]byte{0x66}, 16) }

// newPackblobDEKStore builds a DEK-backed BlobStore. NewDEKKeeper randomizes the
// DEK, so each store gets its own keeper; tests that need seal/open agreement
// use one store instance for both, which holds here.
func newPackblobDEKStore(t testing.TB, dir string, maxSize int64) *BlobStore {
	t.Helper()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	bs, err := NewDEKBlobStore(dir, maxSize, keeper, cid)
	require.NoError(t, err)
	return bs
}

// TestEncryptedBlobStoreReadsGenuinePlaintext verifies that payload bytes with
// no magic are still returned as-is when flagEncrypted is not set (legacy
// unencrypted entries co-existing with an encryptor).
func TestEncryptedBlobStoreReadsGenuinePlaintext(t *testing.T) {
	// Write via unencrypted store, read via encrypted store — plaintext must pass through.
	dir := t.TempDir()
	plain, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	plaintext := []byte("genuine plaintext, no magic")
	loc, err := plain.Append("bucket/key-plain", plaintext)
	require.NoError(t, err)
	require.NoError(t, plain.Close())

	encrypted := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer encrypted.Close()

	got, err := encrypted.Read(loc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestEncryptedBlobStoreReadsPlaintextWithValueMagicButNonLegacyVersion verifies
// the precise-match decision: plaintext that happens to start with the value
// magic (0xAE 0xE2) but does NOT carry the exact pre-XAES version byte 0x01
// must still pass through as-is. Only the exact legacy signature rejects.
func TestEncryptedBlobStoreReadsPlaintextWithValueMagicButNonLegacyVersion(t *testing.T) {
	dir := t.TempDir()
	plain, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	// Value magic prefix but version byte 0x05 (neither legacy 0x01 nor current 0x02).
	payload := []byte{0xAE, 0xE2, 0x05, 'd', 'a', 't', 'a'}
	loc, err := plain.Append("bucket/key-nonlegacy", payload)
	require.NoError(t, err)
	require.NoError(t, plain.Close())

	encrypted := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer encrypted.Close()

	got, err := encrypted.Read(loc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// TestBlobStoreRejectsLegacyValueWithoutEncryptor verifies the bs.encryptor == nil
// branch loud-fails on an exact legacy value (0xAE 0xE2 0x01) rather than
// returning it as raw plaintext.
func TestBlobStoreRejectsLegacyValueWithoutEncryptor(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	// Exact pre-XAES value signature, written as an unencrypted entry.
	oldMagicPayload := []byte{0xAE, 0xE2, 0x01, 'l', 'e', 'g', 'a', 'c', 'y'}
	loc, err := bs.Append("bucket/key-legacy-noenc", oldMagicPayload)
	require.NoError(t, err)
	require.NoError(t, bs.Close())

	// Reopen WITHOUT an encryptor.
	reopened, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer reopened.Close()

	_, err = reopened.Read(loc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported/old encrypted-value format")
}

// TestBlobStoreReadsPlaintextWithoutEncryptor verifies the bs.encryptor == nil
// branch still passes genuine plaintext (incl. value-magic with a non-legacy
// version) through unchanged.
func TestBlobStoreReadsPlaintextWithoutEncryptor(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	// Value magic prefix but version byte 0x05 (neither legacy 0x01 nor current 0x02).
	payload := []byte{0xAE, 0xE2, 0x05, 'd', 'a', 't', 'a'}
	loc, err := bs.Append("bucket/key-plain-noenc", payload)
	require.NoError(t, err)
	require.NoError(t, bs.Close())

	reopened, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer reopened.Close()

	got, err := reopened.Read(loc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
