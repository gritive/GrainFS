package cluster

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/protocred"
)

func TestMetaFSMProtocolCredentialCreateReplayIsIdempotent(t *testing.T) {
	fsm, store := newProtocolCredentialFSM()
	row := testFSMProtocolCredential("pc_create")

	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-create", row))
	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-create", row))
	require.Len(t, store.Snapshot(), 1)
}

func TestMetaFSMProtocolCredentialRequestIDConflictRejected(t *testing.T) {
	fsm, _ := newProtocolCredentialFSM()
	row := testFSMProtocolCredential("pc_create")
	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-reused", row))

	other := row
	other.ID = "pc_other"
	err := applyProtocolCredentialCreateForTest(fsm, "req-reused", other)
	require.Error(t, err)
	require.True(t, errors.Is(err, protocred.ErrConflict), "err = %v", err)
}

func TestMetaFSMProtocolCredentialCreateReplayNormalizesLegacyGeneration(t *testing.T) {
	fsm, store := newProtocolCredentialFSM()
	legacy := testFSMProtocolCredential("pc_legacy_create")
	legacy.Generation = 0
	current := legacy
	current.Generation = 1

	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-create", legacy))
	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-create", current))
	rows := store.Snapshot()
	require.Len(t, rows, 1)
	require.Equal(t, uint64(1), rows[0].Generation)
}

func TestMetaFSMProtocolCredentialRotateRevokeStaleAndLastUsed(t *testing.T) {
	fsm, store := newProtocolCredentialFSM()
	row := testFSMProtocolCredential("pc_lifecycle")
	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-create", row))

	rotatedHash := sha256.Sum256([]byte("rotated"))
	rotatedAt := row.CreatedAt.Add(time.Hour)
	require.NoError(t, applyProtocolCredentialRotateForTest(fsm, "req-rotate", row.ID, rotatedHash, "rotated-hint", rotatedAt))
	require.NoError(t, applyProtocolCredentialRotateForTest(fsm, "req-rotate", row.ID, rotatedHash, "rotated-hint", rotatedAt))
	err := applyProtocolCredentialRotateForTest(fsm, "req-rotate", row.ID, sha256.Sum256([]byte("ignored")), "ignored", row.CreatedAt.Add(2*time.Hour))
	require.Error(t, err)
	require.True(t, errors.Is(err, protocred.ErrConflict), "err = %v", err)
	got, err := protocred.NewService(store).Get(row.ID)
	require.NoError(t, err)
	require.Equal(t, rotatedHash, got.SecretHash)
	require.Equal(t, "rotated-hint", got.SecretHint)

	used := row.CreatedAt.Add(3 * time.Hour)
	require.NoError(t, applyProtocolCredentialLastUsedForTest(fsm, row.ID, used))
	require.NoError(t, applyProtocolCredentialLastUsedForTest(fsm, row.ID, used.Add(-time.Hour)))
	got, err = protocred.NewService(store).Get(row.ID)
	require.NoError(t, err)
	require.NotNil(t, got.LastUsedAt)
	require.True(t, got.LastUsedAt.Equal(used))

	staleAt := row.CreatedAt.Add(4 * time.Hour)
	require.NoError(t, applyProtocolCredentialMarkStaleForTest(fsm, "req-stale", row.ID, staleAt, "policy_detached"))
	require.NoError(t, applyProtocolCredentialMarkStaleForTest(fsm, "req-stale", row.ID, staleAt, "policy_detached"))
	err = applyProtocolCredentialMarkStaleForTest(fsm, "req-stale", row.ID, staleAt.Add(time.Hour), "policy_changed")
	require.Error(t, err)
	require.True(t, errors.Is(err, protocred.ErrConflict), "err = %v", err)
	got, err = protocred.NewService(store).Get(row.ID)
	require.NoError(t, err)
	require.NotNil(t, got.StaleAt)
	require.True(t, got.StaleAt.Equal(staleAt))
	require.Equal(t, "policy_detached", got.StaleReason)

	revokedAt := row.CreatedAt.Add(5 * time.Hour)
	require.NoError(t, applyProtocolCredentialRevokeForTest(fsm, "req-revoke", row.ID, revokedAt))
	require.NoError(t, applyProtocolCredentialRevokeForTest(fsm, "req-revoke", row.ID, revokedAt))
	err = applyProtocolCredentialRevokeForTest(fsm, "req-revoke", row.ID, revokedAt.Add(time.Hour))
	require.Error(t, err)
	require.True(t, errors.Is(err, protocred.ErrConflict), "err = %v", err)
	got, err = protocred.NewService(store).Get(row.ID)
	require.NoError(t, err)
	require.NotNil(t, got.RevokedAt)
	require.True(t, got.RevokedAt.Equal(revokedAt))

	err = applyProtocolCredentialRotateForTest(fsm, "req-rotate-after-revoke", row.ID, sha256.Sum256([]byte("after")), "after", revokedAt.Add(time.Hour))
	require.Error(t, err)
	require.True(t, errors.Is(err, protocred.ErrRevoked), "err = %v", err)
}

func TestMetaFSMProtocolCredentialNilStoreIsNoOp(t *testing.T) {
	fsm := NewMetaFSM()
	row := testFSMProtocolCredential("pc_noop")
	require.NoError(t, applyProtocolCredentialCreateForTest(fsm, "req-create", row))
	require.NoError(t, applyProtocolCredentialRotateForTest(fsm, "req-rotate", row.ID, sha256.Sum256([]byte("rotated")), "hint", row.CreatedAt))
	require.NoError(t, applyProtocolCredentialRevokeForTest(fsm, "req-revoke", row.ID, row.CreatedAt))
	require.NoError(t, applyProtocolCredentialMarkStaleForTest(fsm, "req-stale", row.ID, row.CreatedAt, "reason"))
	require.NoError(t, applyProtocolCredentialLastUsedForTest(fsm, row.ID, row.CreatedAt))
}

func newProtocolCredentialFSM() (*MetaFSM, *protocred.Store) {
	store := protocred.NewStore()
	fsm := NewMetaFSM()
	fsm.SetProtocolCredentialStore(store)
	return fsm, store
}

func testFSMProtocolCredential(id string) protocred.Credential {
	return protocred.Credential{
		ID:         id,
		SAID:       "sa_fsm",
		Protocol:   protocred.ProtocolNBD,
		Resource:   "volume/fsm",
		Mode:       protocred.ModeRW,
		SecretHash: sha256.Sum256([]byte("secret")),
		SecretHint: "secret-hint",
		CreatedAt:  time.Date(2026, 5, 28, 1, 2, 3, 0, time.UTC),
		CreatedBy:  "admin",
		Generation: 1,
	}
}

func applyProtocolCredentialCreateForTest(fsm *MetaFSM, requestID string, row protocred.Credential) error {
	payload, err := encodeProtocolCredentialCreateCmd(ProtocolCredentialCreateCmd{RequestID: requestID, Credential: row})
	if err != nil {
		return err
	}
	return applyMetaCmdForTest(fsm, MetaCmdTypeProtocolCredentialCreate, payload)
}

func applyProtocolCredentialRotateForTest(fsm *MetaFSM, requestID, id string, hash [sha256.Size]byte, hint string, at time.Time) error {
	payload, err := encodeProtocolCredentialRotateCmd(ProtocolCredentialRotateCmd{RequestID: requestID, ID: id, SecretHash: hash, SecretHint: hint, RotatedAt: at})
	if err != nil {
		return err
	}
	return applyMetaCmdForTest(fsm, MetaCmdTypeProtocolCredentialRotate, payload)
}

func applyProtocolCredentialRevokeForTest(fsm *MetaFSM, requestID, id string, at time.Time) error {
	payload, err := encodeProtocolCredentialRevokeCmd(ProtocolCredentialRevokeCmd{RequestID: requestID, ID: id, RevokedAt: at})
	if err != nil {
		return err
	}
	return applyMetaCmdForTest(fsm, MetaCmdTypeProtocolCredentialRevoke, payload)
}

func applyProtocolCredentialMarkStaleForTest(fsm *MetaFSM, requestID, id string, at time.Time, reason string) error {
	payload, err := encodeProtocolCredentialMarkStaleCmd(ProtocolCredentialMarkStaleCmd{RequestID: requestID, ID: id, StaleAt: at, Reason: reason})
	if err != nil {
		return err
	}
	return applyMetaCmdForTest(fsm, MetaCmdTypeProtocolCredentialMarkStale, payload)
}

func applyProtocolCredentialLastUsedForTest(fsm *MetaFSM, id string, at time.Time) error {
	payload, err := encodeProtocolCredentialLastUsedCmd(ProtocolCredentialLastUsedCmd{ID: id, LastUsedAt: at})
	if err != nil {
		return err
	}
	return applyMetaCmdForTest(fsm, MetaCmdTypeProtocolCredentialLastUsed, payload)
}

func applyMetaCmdForTest(fsm *MetaFSM, typ MetaCmdType, payload []byte) error {
	raw, err := encodeMetaCmd(typ, payload)
	if err != nil {
		return err
	}
	return fsm.applyCmd(raw)
}
