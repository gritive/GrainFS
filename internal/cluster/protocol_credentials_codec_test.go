package cluster

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/protocred"
)

func TestProtocolCredentialCommandCodecsRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 28, 1, 2, 3, 4, time.UTC)
	exp := now.Add(time.Hour)
	revoked := now.Add(2 * time.Hour)
	used := now.Add(3 * time.Hour)
	hash := sha256.Sum256([]byte("secret"))

	row := protocred.Credential{
		ID:          "pc_1",
		SAID:        "sa_app",
		Protocol:    protocred.ProtocolNBD,
		Resource:    "volume/devdisk",
		Mode:        protocred.ModeRW,
		SecretHash:  hash,
		SecretHint:  "pcsec...tail",
		CreatedAt:   now,
		CreatedBy:   "admin",
		ExpiresAt:   &exp,
		RevokedAt:   &revoked,
		LastUsedAt:  &used,
		Generation:  4,
		StaleAt:     &used,
		StaleReason: "policy_detached",
		SecretEnc:   []byte("sealed-secret"),
	}

	createBytes, err := encodeProtocolCredentialCreateCmd(ProtocolCredentialCreateCmd{
		RequestID:  "req-create",
		Credential: row,
	})
	require.NoError(t, err)
	gotCreate, err := decodeProtocolCredentialCreateCmd(createBytes)
	require.NoError(t, err)
	require.Equal(t, "req-create", gotCreate.RequestID)
	requireCredentialEqual(t, row, gotCreate.Credential)

	rotateBytes, err := encodeProtocolCredentialRotateCmd(ProtocolCredentialRotateCmd{
		RequestID:  "req-rotate",
		ID:         row.ID,
		SecretHash: hash,
		SecretHint: "pcsec...new",
		SecretEnc:  []byte("sealed-new-secret"),
		RotatedAt:  used,
	})
	require.NoError(t, err)
	gotRotate, err := decodeProtocolCredentialRotateCmd(rotateBytes)
	require.NoError(t, err)
	require.Equal(t, "req-rotate", gotRotate.RequestID)
	require.Equal(t, row.ID, gotRotate.ID)
	require.Equal(t, hash, gotRotate.SecretHash)
	require.Equal(t, "pcsec...new", gotRotate.SecretHint)
	require.Equal(t, []byte("sealed-new-secret"), gotRotate.SecretEnc)
	require.True(t, gotRotate.RotatedAt.Equal(used))

	revokeBytes, err := encodeProtocolCredentialRevokeCmd(ProtocolCredentialRevokeCmd{
		RequestID: "req-revoke",
		ID:        row.ID,
		RevokedAt: revoked,
	})
	require.NoError(t, err)
	gotRevoke, err := decodeProtocolCredentialRevokeCmd(revokeBytes)
	require.NoError(t, err)
	require.Equal(t, ProtocolCredentialRevokeCmd{RequestID: "req-revoke", ID: row.ID, RevokedAt: revoked}, gotRevoke)

	staleBytes, err := encodeProtocolCredentialMarkStaleCmd(ProtocolCredentialMarkStaleCmd{
		RequestID: "req-stale",
		ID:        row.ID,
		StaleAt:   used,
		Reason:    "policy-detached",
	})
	require.NoError(t, err)
	gotStale, err := decodeProtocolCredentialMarkStaleCmd(staleBytes)
	require.NoError(t, err)
	require.Equal(t, ProtocolCredentialMarkStaleCmd{RequestID: "req-stale", ID: row.ID, StaleAt: used, Reason: "policy-detached"}, gotStale)

	lastUsedBytes, err := encodeProtocolCredentialLastUsedCmd(ProtocolCredentialLastUsedCmd{
		ID:         row.ID,
		LastUsedAt: used,
	})
	require.NoError(t, err)
	gotLastUsed, err := decodeProtocolCredentialLastUsedCmd(lastUsedBytes)
	require.NoError(t, err)
	require.Equal(t, ProtocolCredentialLastUsedCmd{ID: row.ID, LastUsedAt: used}, gotLastUsed)
}

func TestProtocolCredentialCommandCodecsRejectMalformedBytes(t *testing.T) {
	malformed := []byte{0x01, 0x02, 0x03}

	_, err := decodeProtocolCredentialCreateCmd(malformed)
	require.Error(t, err)
	_, err = decodeProtocolCredentialRotateCmd(malformed)
	require.Error(t, err)
	_, err = decodeProtocolCredentialRevokeCmd(malformed)
	require.Error(t, err)
	_, err = decodeProtocolCredentialMarkStaleCmd(malformed)
	require.Error(t, err)
	_, err = decodeProtocolCredentialLastUsedCmd(malformed)
	require.Error(t, err)
	_, err = decodeProtocolCredentialsSnapshot(malformed)
	require.Error(t, err)
	_, _, err = decodeProtocolCredentialsSnapshotState(malformed)
	require.Error(t, err)
}

func TestProtocolCredentialSnapshotCodecDeterministicAndRoundTrips(t *testing.T) {
	now := time.Date(2026, 5, 28, 4, 5, 6, 7, time.UTC)
	exp := now.Add(time.Hour)
	revoked := now.Add(2 * time.Hour)
	used := now.Add(3 * time.Hour)

	a := protocred.Credential{
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
		Generation: 1,
		SecretEnc:  []byte("sealed-a"),
	}
	b := protocred.Credential{
		ID:          "pc_b",
		SAID:        "sa_b",
		Protocol:    protocred.ProtocolIceberg,
		Resource:    "catalog/b",
		Mode:        protocred.ModeRW,
		SecretHash:  sha256.Sum256([]byte("b")),
		SecretHint:  "hint-b",
		CreatedAt:   now.Add(time.Minute),
		CreatedBy:   "admin-b",
		RevokedAt:   &revoked,
		LastUsedAt:  &used,
		Generation:  3,
		StaleAt:     &used,
		StaleReason: "policy_changed",
		SecretEnc:   []byte("sealed-b"),
	}

	encodedAB, err := encodeProtocolCredentialsSnapshot([]protocred.Credential{a, b})
	require.NoError(t, err)
	encodedBA, err := encodeProtocolCredentialsSnapshot([]protocred.Credential{b, a})
	require.NoError(t, err)
	require.True(t, bytes.Equal(encodedAB, encodedBA), "snapshot bytes must be deterministic")

	rows, err := decodeProtocolCredentialsSnapshot(encodedAB)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	requireCredentialEqual(t, a, rows[0])
	requireCredentialEqual(t, b, rows[1])
}

func TestProtocolCredentialSnapshotRequestIndexDeterministicAndRoundTrips(t *testing.T) {
	now := time.Date(2026, 5, 28, 4, 5, 6, 7, time.UTC)
	rows := []protocred.Credential{
		{
			ID:         "pc_b",
			SAID:       "sa_b",
			Protocol:   protocred.ProtocolNBD,
			Resource:   "volume/b",
			Mode:       protocred.ModeRW,
			SecretHash: sha256.Sum256([]byte("b")),
			SecretHint: "hint-b",
			CreatedAt:  now,
			CreatedBy:  "admin-b",
			Generation: 1,
		},
	}
	requestsAB := []ProtocolCredentialRequestRecord{
		{RequestID: "req_b", Operation: "rotate", CredentialID: "pc_b", PayloadHash: sha256.Sum256([]byte("b"))},
		{RequestID: "req_a", Operation: "create", CredentialID: "pc_b", PayloadHash: sha256.Sum256([]byte("a"))},
	}
	requestsBA := []ProtocolCredentialRequestRecord{requestsAB[1], requestsAB[0]}

	encodedAB, err := encodeProtocolCredentialsSnapshotState(rows, requestsAB)
	require.NoError(t, err)
	encodedBA, err := encodeProtocolCredentialsSnapshotState(rows, requestsBA)
	require.NoError(t, err)
	require.True(t, bytes.Equal(encodedAB, encodedBA), "request index bytes must be deterministic")

	gotRows, gotRequests, err := decodeProtocolCredentialsSnapshotState(encodedAB)
	require.NoError(t, err)
	require.Len(t, gotRows, 1)
	requireCredentialEqual(t, rows[0], gotRows[0])
	require.Equal(t, []ProtocolCredentialRequestRecord{
		{RequestID: "req_a", Operation: "create", CredentialID: "pc_b", PayloadHash: sha256.Sum256([]byte("a"))},
		{RequestID: "req_b", Operation: "rotate", CredentialID: "pc_b", PayloadHash: sha256.Sum256([]byte("b"))},
	}, gotRequests)
}

func TestProtocolCredentialSnapshotRequestIndexRejectsInvalidRows(t *testing.T) {
	rows := []protocred.Credential{{
		ID:         "pc_b",
		SAID:       "sa_b",
		Protocol:   protocred.ProtocolNBD,
		Resource:   "volume/b",
		Mode:       protocred.ModeRW,
		SecretHash: sha256.Sum256([]byte("b")),
		SecretHint: "hint-b",
		CreatedAt:  time.Date(2026, 5, 28, 4, 5, 6, 7, time.UTC),
		CreatedBy:  "admin-b",
		Generation: 1,
	}}
	encoded, err := encodeProtocolCredentialsSnapshotState(rows, []ProtocolCredentialRequestRecord{
		{RequestID: "req_bad", Operation: "unknown", CredentialID: "pc_b", PayloadHash: sha256.Sum256([]byte("bad"))},
	})
	require.NoError(t, err)

	_, _, err = decodeProtocolCredentialsSnapshotState(encoded)
	require.Error(t, err)
}

func TestProtocolCredentialSnapshotRequestIndexAllowsLegacyMissingPayloadHash(t *testing.T) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString("req_legacy")
	operationOff := b.CreateString("create")
	credentialIDOff := b.CreateString("pc_legacy")
	clusterpb.MetaProtocolCredentialRequestEntryStart(b)
	clusterpb.MetaProtocolCredentialRequestEntryAddRequestId(b, requestIDOff)
	clusterpb.MetaProtocolCredentialRequestEntryAddOperation(b, operationOff)
	clusterpb.MetaProtocolCredentialRequestEntryAddCredentialId(b, credentialIDOff)
	reqOff := clusterpb.MetaProtocolCredentialRequestEntryEnd(b)

	clusterpb.MetaProtocolCredentialsSnapshotStartRowsVector(b, 0)
	rowsVec := b.EndVector(0)
	clusterpb.MetaProtocolCredentialsSnapshotStartRequestsVector(b, 1)
	b.PrependUOffsetT(reqOff)
	requestsVec := b.EndVector(1)
	clusterpb.MetaProtocolCredentialsSnapshotStart(b)
	clusterpb.MetaProtocolCredentialsSnapshotAddRows(b, rowsVec)
	clusterpb.MetaProtocolCredentialsSnapshotAddRequests(b, requestsVec)
	encoded := fbFinish(b, clusterpb.MetaProtocolCredentialsSnapshotEnd(b))

	_, requests, err := decodeProtocolCredentialsSnapshotState(encoded)
	require.NoError(t, err)
	require.Equal(t, []ProtocolCredentialRequestRecord{{
		RequestID:    "req_legacy",
		Operation:    "create",
		CredentialID: "pc_legacy",
	}}, requests)
}

func requireCredentialEqual(t *testing.T, want, got protocred.Credential) {
	t.Helper()
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.SAID, got.SAID)
	require.Equal(t, want.Protocol, got.Protocol)
	require.Equal(t, want.Resource, got.Resource)
	require.Equal(t, want.Mode, got.Mode)
	require.Equal(t, want.SecretHash, got.SecretHash)
	require.Equal(t, want.SecretHint, got.SecretHint)
	require.True(t, got.CreatedAt.Equal(want.CreatedAt), "CreatedAt got %s want %s", got.CreatedAt, want.CreatedAt)
	require.Equal(t, want.CreatedBy, got.CreatedBy)
	requireTimePtrEqual(t, want.ExpiresAt, got.ExpiresAt)
	requireTimePtrEqual(t, want.RevokedAt, got.RevokedAt)
	requireTimePtrEqual(t, want.LastUsedAt, got.LastUsedAt)
	require.Equal(t, want.Generation, got.Generation)
	requireTimePtrEqual(t, want.StaleAt, got.StaleAt)
	require.Equal(t, want.StaleReason, got.StaleReason)
	require.Equal(t, want.SecretEnc, got.SecretEnc)
}

func requireTimePtrEqual(t *testing.T, want, got *time.Time) {
	t.Helper()
	if want == nil {
		require.Nil(t, got)
		return
	}
	require.NotNil(t, got)
	require.True(t, got.Equal(*want), "got %s want %s", *got, *want)
}

// encodeProtocolCredentialsSnapshot / decodeProtocolCredentialsSnapshot are
// test-only convenience wrappers over the *State variants (the production
// snapshot path uses the *State functions directly). Kept in _test.go so the
// unused linter does not flag them as dead production code.
func encodeProtocolCredentialsSnapshot(rows []protocred.Credential) ([]byte, error) {
	return encodeProtocolCredentialsSnapshotState(rows, nil)
}

func decodeProtocolCredentialsSnapshot(data []byte) ([]protocred.Credential, error) {
	rows, _, err := decodeProtocolCredentialsSnapshotState(data)
	return rows, err
}
