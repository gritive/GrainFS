package packblob

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// frameFlagsAt returns the flags byte of the entry whose frame begins at the
// given entry offset (offset points at the key_len field).
func frameFlagsAt(t *testing.T, raw []byte, entryOffset uint64) byte {
	t.Helper()
	keyLen := binary.BigEndian.Uint32(raw[entryOffset:])
	flagsOff := int(entryOffset) + 4 + int(keyLen)
	return raw[flagsOff]
}

// TestPackblobGenFramed_Gen0RoundTrip: a DEK store at active gen 0 writes a
// gen-framed encrypted entry (flagGenFramed set, gen 0) and reads it back.
func TestPackblobGenFramed_Gen0RoundTrip(t *testing.T) {
	dir := t.TempDir()
	bs := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer bs.Close()

	data := []byte("hello gen-framed blob")
	loc, err := bs.Append("bucket/key1", data)
	require.NoError(t, err)

	raw, err := os.ReadFile(bs.blobPath(loc.BlobID))
	require.NoError(t, err)
	flags := frameFlagsAt(t, raw, loc.Offset)
	assert.NotZero(t, flags&flagEncrypted, "encrypted store must set flagEncrypted")
	assert.NotZero(t, flags&flagGenFramed, "encrypted store must set flagGenFramed")

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestPackblobGenFramed_MixedGenInOneFile is the core test: an entry sealed at
// gen 0 and an entry sealed at gen 1 coexist in one blob file; each Read opens
// under the correct per-entry gen. Proves per-entry framing + mixed gens.
func TestPackblobGenFramed_MixedGenInOneFile(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	bs := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs.Close()

	require.Equal(t, uint32(0), keeper.ActiveDEKGeneration())
	dataA := []byte("entry A at gen 0")
	locA, err := bs.Append("bucket/A", dataA)
	require.NoError(t, err)

	require.NoError(t, keeper.Rotate())
	require.Equal(t, uint32(1), keeper.ActiveDEKGeneration())
	dataB := []byte("entry B at gen 1")
	locB, err := bs.Append("bucket/B", dataB)
	require.NoError(t, err)

	// Same blob file, distinct frames.
	require.Equal(t, locA.BlobID, locB.BlobID)

	gotA, err := bs.Read(locA)
	require.NoError(t, err)
	assert.Equal(t, dataA, gotA, "gen-0 entry must still open at gen 0 after rotation")

	gotB, err := bs.Read(locB)
	require.NoError(t, err)
	assert.Equal(t, dataB, gotB, "gen-1 entry must open at gen 1")
}

// TestPackblobGenFramed_PlaintextStoreUnaffected: a plaintext store never sets
// flagGenFramed and its frame is unchanged (regression guard).
func TestPackblobGenFramed_PlaintextStoreUnaffected(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	loc, err := bs.Append("bucket/plain", []byte("plaintext payload"))
	require.NoError(t, err)
	raw, err := os.ReadFile(bs.blobPath(loc.BlobID))
	require.NoError(t, err)
	flags := frameFlagsAt(t, raw, loc.Offset)
	assert.Zero(t, flags&flagGenFramed, "plaintext store must not set flagGenFramed")
	assert.Zero(t, flags&flagEncrypted, "plaintext store must not set flagEncrypted")

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Equal(t, "plaintext payload", string(got))
}

// TestPackblobGenFramed_CompactOverFramedEntries (B1): Compact must parse the
// gen field; otherwise it mis-reads gen as data_len and corrupts survivors.
func TestPackblobGenFramed_CompactOverFramedEntries(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	bs := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs.Close()

	keep1, err := bs.Append("bucket/keep1", []byte("survivor one"))
	require.NoError(t, err)
	_, err = bs.Append("bucket/drop", []byte("to be tombstoned"))
	require.NoError(t, err)
	keep2, err := bs.Append("bucket/keep2", []byte("survivor two"))
	require.NoError(t, err)
	require.Equal(t, keep1.BlobID, keep2.BlobID)

	newLocs, err := bs.Compact(keep1.BlobID, map[string]bool{"bucket/drop": true})
	require.NoError(t, err)
	require.Contains(t, newLocs, "bucket/keep1")
	require.Contains(t, newLocs, "bucket/keep2")
	require.NotContains(t, newLocs, "bucket/drop")

	got1, err := bs.Read(newLocs["bucket/keep1"])
	require.NoError(t, err)
	assert.Equal(t, "survivor one", string(got1))
	got2, err := bs.Read(newLocs["bucket/keep2"])
	require.NoError(t, err)
	assert.Equal(t, "survivor two", string(got2))
}

// TestPackblobGenFramed_ScanAllOverFramedEntries (B1): ScanAll must parse the
// gen field so the rebuilt index has correct offsets/lengths.
func TestPackblobGenFramed_ScanAllOverFramedEntries(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)

	bs1 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	want := map[string]string{"bucket/a": "alpha", "bucket/b": "bravo", "bucket/c": "charlie"}
	appended := make(map[string]BlobLocation, 3)
	appended["bucket/a"], err = bs1.Append("bucket/a", []byte(want["bucket/a"]))
	require.NoError(t, err)
	require.NoError(t, keeper.Rotate()) // bucket/b sealed at gen 1, mixed with gen-0 entries
	appended["bucket/b"], err = bs1.Append("bucket/b", []byte(want["bucket/b"]))
	require.NoError(t, err)
	appended["bucket/c"], err = bs1.Append("bucket/c", []byte(want["bucket/c"]))
	require.NoError(t, err)
	require.NoError(t, bs1.Close())

	bs2 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs2.Close()
	locs, err := bs2.ScanAll()
	require.NoError(t, err)
	for k, v := range want {
		loc, ok := locs[k]
		require.True(t, ok, "ScanAll missing %s", k)
		// ScanAll must rebuild the SAME offset/length as Append (gen not in Length).
		require.Equal(t, appended[k].Offset, loc.Offset, "ScanAll offset for %s", k)
		require.Equal(t, appended[k].Length, loc.Length, "ScanAll length for %s", k)
		got, err := bs2.Read(loc)
		require.NoError(t, err)
		assert.Equal(t, v, string(got))
	}
}

// TestPackblobGenFramed_FrameShiftDowngradeFailsClosed (task 6): clearing
// flagGenFramed on a framed entry mis-parses the gen bytes as data_len → the
// dataLen!=loc.Length cross-check fails closed; ciphertext is never returned.
func TestPackblobGenFramed_FrameShiftDowngradeFailsClosed(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	require.NoError(t, keeper.Rotate()) // active gen 1
	bs := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs.Close()

	loc, err := bs.Append("bucket/secret", []byte("confidential gen-1 data"))
	require.NoError(t, err)

	path := bs.blobPath(loc.BlobID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	flagsPos := int64(loc.Offset) + 4 + int64(len("bucket/secret"))
	var fb [1]byte
	_, err = f.ReadAt(fb[:], flagsPos)
	require.NoError(t, err)
	require.NotZero(t, fb[0]&flagGenFramed)
	fb[0] &^= flagGenFramed // strip the framing bit
	_, err = f.WriteAt(fb[:], flagsPos)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = bs.Read(loc)
	require.Error(t, err, "frame-shift downgrade must fail closed, not return ciphertext")
}

// TestPackblobGenFramed_TamperedGenSelectorFails (task 4): flipping the frame
// gen to an unknown gen (CRC recomputed so the gen byte-rot guard is bypassed)
// makes Open select a missing key → hard error, never silent wrong-plaintext.
func TestPackblobGenFramed_TamperedGenSelectorFails(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	bs := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper) // active gen 0 only
	defer bs.Close()

	key := "bucket/sel"
	loc, err := bs.Append(key, []byte("payload at gen 0"))
	require.NoError(t, err)

	path := bs.blobPath(loc.BlobID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	flagsPos := int64(loc.Offset) + 4 + int64(len(key))
	genPos := flagsPos + 1
	// flip gen 0 -> 99 (no such gen in the keeper)
	var newGen [genFieldSize]byte
	binary.BigEndian.PutUint32(newGen[:], 99)
	_, err = f.WriteAt(newGen[:], genPos)
	require.NoError(t, err)
	// recompute CRC over the tampered gen so the CRC guard does not pre-empt
	dataPos := genPos + genFieldSize + 4
	payload := make([]byte, loc.Length)
	_, err = f.ReadAt(payload, dataPos)
	require.NoError(t, err)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], blobEntryCRC([]byte(key), flagEncrypted|flagGenFramed, 99, payload))
	_, err = f.WriteAt(crc[:], dataPos+int64(loc.Length))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = bs.Read(loc)
	require.Error(t, err, "wrong gen selector must fail, not return wrong plaintext")
}

// TestPackblobGenFramed_GenUnderCRC (task 5): flipping a gen byte without fixing
// the CRC trips the CRC-mismatch path (gen is covered by the framed CRC).
func TestPackblobGenFramed_GenUnderCRC(t *testing.T) {
	dir := t.TempDir()
	bs := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer bs.Close()

	key := "bucket/crc"
	loc, err := bs.Append(key, []byte("crc-covered gen"))
	require.NoError(t, err)

	path := bs.blobPath(loc.BlobID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	genPos := int64(loc.Offset) + 4 + int64(len(key)) + 1
	var gb [genFieldSize]byte
	_, err = f.ReadAt(gb[:], genPos)
	require.NoError(t, err)
	gb[3] ^= 0xFF // corrupt the gen field, leave CRC stale
	_, err = f.WriteAt(gb[:], genPos)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = bs.Read(loc)
	require.Error(t, err, "gen byte-rot must be caught by the CRC")
}

// TestPackblobGenFramed_BackwardReadNoBitEntry (task 3): an encrypted entry
// written WITHOUT flagGenFramed (pre-slice format) still reads at gen 0.
func TestPackblobGenFramed_BackwardReadNoBitEntry(t *testing.T) {
	dir := t.TempDir()
	bs := newPackblobDEKStore(t, dir, 256*1024*1024)
	defer bs.Close()

	const blobID = uint64(0xBEEF)
	key := "bucket/legacy"
	plaintext := []byte("old gen-0 encrypted entry")
	// Seal under the OLD AAD (flags without flagGenFramed), at gen 0.
	flags := flagEncrypted
	sealed, gen, err := bs.segEnc.SealTo(nil, encrypt.DomainShard,
		blobEntryAADFields(blobID, 0, key, flags), plaintext)
	require.NoError(t, err)
	require.Equal(t, uint32(0), gen)

	// Hand-write the legacy frame: [key_len][key][flags][data_len][data][crc]
	var buf bytes.Buffer
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(key)))
	buf.Write(u32[:])
	buf.WriteString(key)
	buf.WriteByte(flags)
	binary.BigEndian.PutUint32(u32[:], uint32(len(sealed)))
	buf.Write(u32[:])
	buf.Write(sealed)
	binary.BigEndian.PutUint32(u32[:], blobEntryCRC([]byte(key), flags, 0, sealed))
	buf.Write(u32[:])
	require.NoError(t, os.WriteFile(bs.blobPath(blobID), buf.Bytes(), 0o644))

	got, err := bs.Read(BlobLocation{BlobID: blobID, Offset: 0, Length: uint32(len(sealed))})
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

// TestPackblobGenFramed_DowngradeDetectedAtGen1 (task 10, M3): strip only
// flagEncrypted (keep flagGenFramed) on a gen-1 entry → the downgrade Open-
// attempt with the parsed gen + preserved framed bit detects it.
func TestPackblobGenFramed_DowngradeDetectedAtGen1(t *testing.T) {
	dir := t.TempDir()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	require.NoError(t, keeper.Rotate()) // active gen 1
	bs := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs.Close()

	key := "bucket/dg1"
	loc, err := bs.Append(key, []byte("gen-1 secret"))
	require.NoError(t, err)
	require.NoError(t, bs.Close())

	tamperStripEncryptedFlag(t, dir, key, loc)

	bs2 := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs2.Close()
	_, err = bs2.Read(loc)
	require.Error(t, err, "downgrade on a gen-1 framed entry must be detected")
}
