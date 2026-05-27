package cluster

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/raft"
)

func TestMetaFSMProtocolCredentialSnapshotRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 28, 1, 2, 3, 4, time.UTC)
	exp := now.Add(time.Hour)
	used := now.Add(2 * time.Hour)

	srcStore := protocred.NewStore()
	srcStore.Restore([]protocred.Credential{
		{
			ID:         "pc_b",
			SAID:       "sa_b",
			Protocol:   protocred.ProtocolNBD,
			Resource:   "volume/b",
			Mode:       protocred.ModeRW,
			SecretHash: sha256.Sum256([]byte("b")),
			SecretHint: "hint-b",
			CreatedAt:  now.Add(time.Minute),
			CreatedBy:  "admin-b",
			LastUsedAt: &used,
		},
		{
			ID:         "pc_a",
			SAID:       "sa_a",
			Protocol:   protocred.ProtocolS3,
			Resource:   "bucket/a",
			Mode:       protocred.ModeRO,
			SecretHash: sha256.Sum256([]byte("a")),
			SecretHint: "hint-a",
			CreatedAt:  now,
			CreatedBy:  "admin-a",
			ExpiresAt:  &exp,
		},
	})

	src := NewMetaFSM()
	wireTestKEK(t, src)
	src.SetProtocolCredentialStore(srcStore)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dstStore := protocred.NewStore()
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	dst.SetProtocolCredentialStore(dstStore)

	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
	rows := dstStore.Snapshot()
	require.Len(t, rows, 2)
	require.Equal(t, "pc_a", rows[0].ID)
	require.Equal(t, "pc_b", rows[1].ID)
	require.Equal(t, protocred.ProtocolS3, rows[0].Protocol)
	require.NotNil(t, rows[0].ExpiresAt)
	require.True(t, rows[0].ExpiresAt.Equal(exp))
	require.NotNil(t, rows[1].LastUsedAt)
	require.True(t, rows[1].LastUsedAt.Equal(used))
}

func TestMetaFSMProtocolCredentialLegacySnapshotWithoutTrailerLeavesStoreEmpty(t *testing.T) {
	src := NewMetaFSM()
	wireTestKEK(t, src)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dstStore := protocred.NewStore()
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	dst.SetProtocolCredentialStore(dstStore)

	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
	require.Empty(t, dstStore.Snapshot())
}

func TestMetaFSMProtocolCredentialLegacySnapshotWithoutTrailerClearsExistingState(t *testing.T) {
	src := NewMetaFSM()
	wireTestKEK(t, src)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dstStore := protocred.NewStore()
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	dst.SetProtocolCredentialStore(dstStore)
	row := testFSMProtocolCredential("pc_stale_after_restore")
	require.NoError(t, applyProtocolCredentialCreateForTest(dst, "req-stale-before-restore", row))

	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
	require.Empty(t, dstStore.Snapshot())

	conflict := row
	conflict.ID = "pc_after_restore"
	require.NoError(t, applyProtocolCredentialCreateForTest(dst, "req-stale-before-restore", conflict))
	require.Len(t, dstStore.Snapshot(), 1)
}

func TestMetaFSMProtocolCredentialEmptySnapshotClearsWiredStore(t *testing.T) {
	src := NewMetaFSM()
	wireTestKEK(t, src)
	src.SetProtocolCredentialStore(protocred.NewStore())

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dstStore := protocred.NewStore()
	dstStore.Restore([]protocred.Credential{{
		ID:         "pc_stale",
		SAID:       "sa_stale",
		Protocol:   protocred.ProtocolNBD,
		Resource:   "volume/stale",
		Mode:       protocred.ModeRW,
		SecretHash: sha256.Sum256([]byte("stale")),
		SecretHint: "hint-stale",
		CreatedAt:  time.Date(2026, 5, 28, 1, 2, 3, 0, time.UTC),
	}})
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	dst.SetProtocolCredentialStore(dstStore)

	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
	require.Empty(t, dstStore.Snapshot())
}

func TestMetaFSMProtocolCredentialRequestIndexSurvivesRestore(t *testing.T) {
	srcStore := protocred.NewStore()
	src := NewMetaFSM()
	wireTestKEK(t, src)
	src.SetProtocolCredentialStore(srcStore)
	row := testFSMProtocolCredential("pc_restore_req")

	require.NoError(t, applyProtocolCredentialCreateForTest(src, "req-create", row))
	snap, err := src.Snapshot()
	require.NoError(t, err)

	dstStore := protocred.NewStore()
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	dst.SetProtocolCredentialStore(dstStore)
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))

	require.NoError(t, applyProtocolCredentialCreateForTest(dst, "req-create", row))
	require.Len(t, dstStore.Snapshot(), 1)

	conflict := row
	conflict.ID = "pc_restore_conflict"
	err = applyProtocolCredentialCreateForTest(dst, "req-create", conflict)
	require.Error(t, err)
	require.ErrorIs(t, err, protocred.ErrConflict)
}

func TestMetaFSMProtocolCredentialSnapshotNormalizesLegacyGeneration(t *testing.T) {
	row := testFSMProtocolCredential("pc_legacy_generation")
	row.Generation = 0
	payload, err := encodeProtocolCredentialsSnapshot([]protocred.Credential{row})
	require.NoError(t, err)

	rows, err := decodeProtocolCredentialsSnapshot(payload)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(1), rows[0].Generation)
}
