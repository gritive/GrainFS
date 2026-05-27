package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// RunPruneOld exposes pruneOld for white-box testing.
func RunPruneOld(a *AutoSnapshotter, maxRetain int) {
	a.pruneOld(maxRetain)
}

// NewTestKEK returns a pre-seeded KEKStore (version 1, 32 zero bytes) and a
// non-zero cluster id for use in tests.
func NewTestKEK(t *testing.T) (*encrypt.KEKStore, [16]byte) {
	t.Helper()
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, make([]byte, encrypt.KEKSize)))
	var cid [16]byte
	cid[0] = 0x5A
	return store, cid
}

// NewTestManager creates a Manager wired with a test KEK for use in tests.
func NewTestManager(t *testing.T, dir string, backend storage.Snapshotable, walDir string) *Manager {
	t.Helper()
	store, cid := NewTestKEK(t)
	m, err := NewManagerWithEncryptor(dir, backend, walDir, nil, store, cid)
	require.NoError(t, err)
	return m
}

// NewTestManagerRefSink creates a Manager with a test KEK and chunk-ref sink.
func NewTestManagerRefSink(t *testing.T, dir string, backend storage.Snapshotable, walDir string, refs RefSink) *Manager {
	t.Helper()
	store, cid := NewTestKEK(t)
	m, err := NewManagerWithRefSink(dir, backend, walDir, nil, store, cid, refs)
	require.NoError(t, err)
	return m
}

// WriteLegacyPlaintextSnapshot writes a pre-Slice-2 (un-enveloped) GFSNAP01 file
// to exercise the read-compat shim. Errors fail the test immediately.
func WriteLegacyPlaintextSnapshot(t *testing.T, path string, snap *Snapshot) {
	t.Helper()
	var buf bytes.Buffer
	_, err := buf.Write(snapshotMagic[:])
	require.NoError(t, err)
	require.NoError(t, binary.Write(&buf, binary.BigEndian, currentSnapshotReaderFormat))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, currentSnapshotWriterFormat))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, time.Now().UnixNano()))
	zw, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedDefault))
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(zw).Encode(snap))
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))
}

// WriteSnapshotForTest exposes writeSnapshot for tests that need to craft
// custom on-disk snapshot payloads (e.g. simulating older formats).
func WriteSnapshotForTest(m *Manager, snap *Snapshot) error {
	return m.writeSnapshot(m.path(snap.Seq), snap)
}
