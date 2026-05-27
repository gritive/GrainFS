package snapshot

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// testBackend is a minimal storage.Snapshotable for envelope tests.
type testBackend struct{}

func (b *testBackend) ListAllObjects() ([]storage.SnapshotObject, error) { return nil, nil }
func (b *testBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	return 0, nil, nil
}

func TestSnapshotEnvelope_RoundTrip(t *testing.T) {
	m := NewTestManager(t, t.TempDir(), &testBackend{}, "")
	snap := &Snapshot{Seq: 1, Buckets: []string{"b1"}, Objects: []storage.SnapshotObject{{Bucket: "b1", Key: "secret-key-name"}}}
	require.NoError(t, m.writeSnapshot(m.path(1), snap))

	raw, err := os.ReadFile(m.path(1))
	require.NoError(t, err)
	require.True(t, encrypt.IsSnapshotEnvelope(raw), "snapshot file must be sealed")
	require.False(t, strings.Contains(string(raw), "secret-key-name"), "object key must not appear in plaintext on disk")

	got, err := m.readSnapshot(m.path(1))
	require.NoError(t, err)
	require.Equal(t, "secret-key-name", got.Objects[0].Key)
}

func TestSnapshotEnvelope_NilKEKConstructorFails(t *testing.T) {
	_, err := NewManagerWithEncryptor(t.TempDir(), &testBackend{}, "", nil, nil, [16]byte{0x5A})
	require.Error(t, err, "constructor must reject a nil KEK source")
}

func TestSnapshotEnvelope_ZeroClusterIDFails(t *testing.T) {
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, make([]byte, encrypt.KEKSize)))
	_, err := NewManagerWithEncryptor(t.TempDir(), &testBackend{}, "", nil, store, [16]byte{})
	require.Error(t, err, "constructor must reject an all-zero cluster id")
}

func TestSnapshotEnvelope_LegacyPlaintextRead(t *testing.T) {
	m := NewTestManager(t, t.TempDir(), &testBackend{}, "")
	WriteLegacyPlaintextSnapshot(t, m.path(7), &Snapshot{Seq: 7, Buckets: []string{"old"}})
	got, err := m.readSnapshot(m.path(7))
	require.NoError(t, err)
	require.Equal(t, uint64(7), got.Seq)
}

func TestSnapshotEnvelope_RestoreAcrossKEKRotation(t *testing.T) {
	dir := t.TempDir()
	store := encrypt.NewKEKStore()
	k1 := make([]byte, encrypt.KEKSize)
	for i := range k1 {
		k1[i] = 0x11
	}
	require.NoError(t, store.Add(1, k1))
	var cid [16]byte
	cid[0] = 0xAB
	m, err := NewManagerWithEncryptor(dir, &testBackend{}, "", nil, store, cid)
	require.NoError(t, err)
	require.NoError(t, m.writeSnapshot(m.path(3), &Snapshot{Seq: 3, Buckets: []string{"b"}}))

	k2 := make([]byte, encrypt.KEKSize)
	for i := range k2 {
		k2[i] = 0x22 // distinct from v1 — proves header-version lookup, not active-KEK
	}
	require.NoError(t, store.Add(2, k2))

	got, err := m.readSnapshot(m.path(3))
	require.NoError(t, err)
	require.Equal(t, uint64(3), got.Seq)
}

// TestPITRRestore_FailsClosedOnUnreadableNewerSnapshot regression-tests the
// code-gate fix: List() silently skips an unreadable (e.g. wrong-KEK) snapshot,
// so PITRRestore must refuse rather than restore from an older base when a newer
// snapshot on disk cannot be opened.
func TestPITRRestore_FailsClosedOnUnreadableNewerSnapshot(t *testing.T) {
	dir := t.TempDir()
	store := encrypt.NewKEKStore()
	k1 := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	require.NoError(t, store.Add(1, k1))
	var cid [16]byte
	cid[0] = 0xAB
	m, err := NewManagerWithEncryptor(dir, &testBackend{}, "", nil, store, cid)
	require.NoError(t, err)

	// seq 1: a normal sealed snapshot this manager can read.
	require.NoError(t, m.writeSnapshot(m.path(1), &Snapshot{Seq: 1, Timestamp: time.Now().Add(-time.Hour)}))

	// seq 2 (newer): sealed under a DIFFERENT key at the same KEK version, so the
	// manager resolves v1 to k1 and AEAD-open fails → List() skips it.
	framed, err := encodeSnapshotFramed(&Snapshot{Seq: 2, Timestamp: time.Now().Add(-30 * time.Minute)})
	require.NoError(t, err)
	var sid [16]byte
	sid[0] = 0x02
	sealed, err := encrypt.SealSnapshotEnvelope(bytes.Repeat([]byte{0x99}, encrypt.KEKSize), cid[:], sid, 1, framed)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(m.path(2), sealed, 0o644))

	// Sanity: List() drops the unreadable seq 2.
	snaps, err := m.List()
	require.NoError(t, err)
	require.Len(t, snaps, 1)

	// PITR to now: base would be seq 1, but seq 2 is a newer unreadable snapshot.
	_, err = m.PITRRestore(time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unreadable")
}
