package snapshot

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

func sealedSnapshotBytes(t *testing.T, kek []byte, cid [16]byte, kekVer uint32, seq uint64) []byte {
	t.Helper()
	var sid [16]byte
	sid[0] = byte(seq + 1)
	framed, err := encodeSnapshotFramed(&Snapshot{Seq: seq})
	require.NoError(t, err)
	sealed, err := encrypt.SealSnapshotEnvelope(kek, cid[:], sid, kekVer, framed)
	require.NoError(t, err)
	return sealed
}

func TestCountSnapshotsSealedUnderKEK_Normal(t *testing.T) {
	dir := t.TempDir()
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = 0x11
	}
	var cid [16]byte
	cid[0] = 0x01

	// Two .json.zst sealed under v1
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(1, 10)+".json.zst"), sealedSnapshotBytes(t, kek, cid, 1, 1), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(2, 10)+".json.zst"), sealedSnapshotBytes(t, kek, cid, 1, 2), 0o644))

	// One .json.zst sealed under v2
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(3, 10)+".json.zst"), sealedSnapshotBytes(t, kek, cid, 2, 3), 0o644))

	// One in-flight .json.zst.tmp sealed under v1 (must be counted)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(4, 10)+".json.zst.tmp"), sealedSnapshotBytes(t, kek, cid, 1, 4), 0o644))

	// A legacy plaintext .json.zst (encodeSnapshotFramed only, no envelope → not counted)
	legacyFramed, err := encodeSnapshotFramed(&Snapshot{Seq: 5})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(5, 10)+".json.zst"), legacyFramed, 0o644))

	// A non-envelope .tmp with garbage content (not counted)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(6, 10)+".json.zst.tmp"), []byte("x"), 0o644))

	count, err := CountSnapshotsSealedUnderKEK(dir, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(3), count)

	count2, err := CountSnapshotsSealedUnderKEK(dir, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count2)
}

func TestCountSnapshotsSealedUnderKEK_CorruptEnvelope(t *testing.T) {
	dir := t.TempDir()

	// Corrupt: GSNE magic but truncated header — must fail closed (count++)
	corrupt := append([]byte("GSNE"), 0, 0)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-"+strconv.FormatUint(7, 10)+".json.zst"), corrupt, 0o644))

	count, err := CountSnapshotsSealedUnderKEK(dir, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
}

func TestCountSnapshotsSealedUnderKEK_MissingDir(t *testing.T) {
	count, err := CountSnapshotsSealedUnderKEK(t.TempDir()+"/nonexistent", 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)
}
