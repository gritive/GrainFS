package protocred

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testSecretEnvelope struct {
	sealedAAD []byte
	openedAAD []byte
}

func (e *testSecretEnvelope) SealProtocolCredentialSecret(aad []byte, plaintext string) ([]byte, error) {
	e.sealedAAD = append([]byte(nil), aad...)
	out := append([]byte("sealed:"), base64.RawStdEncoding.EncodeToString([]byte(plaintext))...)
	return out, nil
}

func (e *testSecretEnvelope) OpenProtocolCredentialSecret(aad []byte, ciphertext []byte) (string, error) {
	e.openedAAD = append([]byte(nil), aad...)
	encoded, ok := bytes.CutPrefix(ciphertext, []byte("sealed:"))
	if !ok {
		return "", ErrInvalid
	}
	plain, err := base64.RawStdEncoding.DecodeString(string(encoded))
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func TestServiceCreateGetListAndHints(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))

	secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW,
	})
	require.NoError(t, err)
	require.NotEmpty(t, secret.ID)
	require.NotEmpty(t, secret.Secret)
	require.True(t, strings.HasPrefix(secret.ConnectionHint["export_name"], "devdisk@"))

	nfsSecret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNFS, Resource: "bucket/default", Mode: ModeRO,
	})
	require.NoError(t, err)
	require.Equal(t, "default/"+nfsSecret.ID+":"+nfsSecret.Secret, nfsSecret.ConnectionHint["mount_path"])

	p9Secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: Protocol9P, Resource: "bucket/default", Mode: ModeRO,
	})
	require.NoError(t, err)
	require.Equal(t, p9Secret.ID+":"+p9Secret.Secret+"@default", p9Secret.ConnectionHint["aname"])

	item, err := svc.Get(secret.ID)
	require.NoError(t, err)
	require.NotEmpty(t, item.SecretHint)
	require.NotContains(t, item.SecretHint, secret.Secret)
	require.Equal(t, "node-a", item.SAID)
	require.Equal(t, ProtocolNBD, item.Protocol)
	require.Equal(t, "volume/devdisk", item.Resource)
	require.Equal(t, ModeRW, item.Mode)

	items := svc.List(ListFilter{SAID: "node-a", Protocol: ProtocolNBD})
	require.Len(t, items, 1)
	require.Equal(t, secret.ID, items[0].ID)
}

func TestServiceListFiltersByResource(t *testing.T) {
	store := NewStore()
	svc := NewService(store)
	_, err := svc.Create(CreateRequest{SAID: "sa-a", Protocol: ProtocolNBD, Resource: "volume/one", Mode: ModeRW})
	require.NoError(t, err)
	_, err = svc.Create(CreateRequest{SAID: "sa-a", Protocol: ProtocolNBD, Resource: "volume/two", Mode: ModeRW})
	require.NoError(t, err)

	got := svc.List(ListFilter{Protocol: ProtocolNBD, Resource: "volume/two"})
	require.Len(t, got, 1)
	require.Equal(t, "volume/two", got[0].Resource)
}

func TestServiceEnvelopeStoresRecoverableSecretWithoutLeakingPlaintext(t *testing.T) {
	now := time.Unix(101, 0).UTC()
	envelope := &testSecretEnvelope{}
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }), WithSecretEnvelope(envelope))

	secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolS3, Resource: "bucket/default", Mode: ModeRW,
	})
	require.NoError(t, err)
	require.NotEmpty(t, secret.Secret)
	require.NotEmpty(t, envelope.sealedAAD)

	item, err := svc.Get(secret.ID)
	require.NoError(t, err)
	require.NotEmpty(t, item.SecretEnc)
	require.NotContains(t, item.SecretHint, secret.Secret)
	require.NotContains(t, string(item.SecretEnc), secret.Secret)

	listed := svc.List(ListFilter{Protocol: ProtocolS3})
	require.Len(t, listed, 1)
	require.Equal(t, item.SecretEnc, listed[0].SecretEnc)
	require.NotContains(t, listed[0].SecretHint, secret.Secret)

	opened, openedItem, err := svc.OpenSecret(secret.ID)
	require.NoError(t, err)
	require.Equal(t, secret.Secret, opened)
	require.Equal(t, secret.ID, openedItem.ID)
	require.Equal(t, envelope.sealedAAD, envelope.openedAAD)
}

func TestServiceOpenSecretRejectsMissingEnvelopeOrCiphertext(t *testing.T) {
	store := NewStore()
	svc := NewService(store)
	secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolS3, Resource: "bucket/default", Mode: ModeRW,
	})
	require.NoError(t, err)

	_, _, err = svc.OpenSecret(secret.ID)
	require.ErrorIs(t, err, ErrInvalid)

	withEnvelope := NewService(store, WithSecretEnvelope(&testSecretEnvelope{}))
	_, _, err = withEnvelope.OpenSecret(secret.ID)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestMaterializeCreateMatchesServiceCreateShape(t *testing.T) {
	now := time.Unix(123, 0).UTC()
	expires := now.Add(time.Hour)
	item, secret, err := MaterializeCreate(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW, ExpiresAt: &expires, CreatedBy: "admin",
	}, now)
	require.NoError(t, err)
	require.NotEmpty(t, item.ID)
	require.Equal(t, item.ID, secret.ID)
	require.NotEmpty(t, secret.Secret)
	require.True(t, strings.HasPrefix(secret.ConnectionHint["export_name"], "devdisk@"))
	require.Equal(t, now, item.CreatedAt)
	require.Equal(t, "admin", item.CreatedBy)
	require.NotNil(t, item.ExpiresAt)
	require.True(t, item.ExpiresAt.Equal(expires))
	require.NotContains(t, item.SecretHint, secret.Secret)
}

func TestServiceRotateAndRevoke(t *testing.T) {
	svc := NewService(NewStore())
	first, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRO})
	require.NoError(t, err)
	second, err := svc.Rotate(first.ID)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
	require.NotEqual(t, first.Secret, second.Secret)
	require.NoError(t, svc.Revoke(first.ID))
	item, err := svc.Get(first.ID)
	require.NoError(t, err)
	require.NotNil(t, item.RevokedAt)
	_, err = svc.Rotate(first.ID)
	require.Error(t, err)
}

func TestServiceRotateRefreshesEncryptedSecret(t *testing.T) {
	envelope := &testSecretEnvelope{}
	svc := NewService(NewStore(), WithSecretEnvelope(envelope))
	first, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolS3, Resource: "bucket/default", Mode: ModeRO})
	require.NoError(t, err)
	before, err := svc.Get(first.ID)
	require.NoError(t, err)

	second, err := svc.Rotate(first.ID)
	require.NoError(t, err)
	after, err := svc.Get(first.ID)
	require.NoError(t, err)
	require.NotEqual(t, before.SecretEnc, after.SecretEnc)

	opened, _, err := svc.OpenSecret(first.ID)
	require.NoError(t, err)
	require.Equal(t, second.Secret, opened)
}

func TestServiceAuthenticate(t *testing.T) {
	now := time.Unix(180, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))
	secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW,
	})
	require.NoError(t, err)

	item, err := svc.Authenticate(AuthenticateRequest{
		Protocol: ProtocolNBD,
		Resource: "volume/devdisk",
		Mode:     ModeRW,
		Secret:   secret.Secret,
	})
	require.NoError(t, err)
	require.Equal(t, secret.ID, item.ID)

	_, err = svc.Authenticate(AuthenticateRequest{Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW, Secret: "wrong"})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.Authenticate(AuthenticateRequest{Protocol: ProtocolNBD, Resource: "volume/other", Mode: ModeRW, Secret: secret.Secret})
	require.ErrorIs(t, err, ErrInvalid)
	_, err = svc.Authenticate(AuthenticateRequest{Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRO, Secret: secret.Secret})
	require.ErrorIs(t, err, ErrInvalid)
}

func TestServiceAuthenticateRejectsExpiredAndRevoked(t *testing.T) {
	now := time.Unix(220, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))
	past := now.Add(-time.Second)
	expired, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/expired", Mode: ModeRW, ExpiresAt: &past,
	})
	require.NoError(t, err)
	active, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/revoked", Mode: ModeRW,
	})
	require.NoError(t, err)
	require.NoError(t, svc.Revoke(active.ID))

	_, err = svc.Authenticate(AuthenticateRequest{Protocol: ProtocolNBD, Resource: "volume/expired", Mode: ModeRW, Secret: expired.Secret})
	require.ErrorIs(t, err, ErrExpired)
	_, err = svc.Authenticate(AuthenticateRequest{Protocol: ProtocolNBD, Resource: "volume/revoked", Mode: ModeRW, Secret: active.Secret})
	require.ErrorIs(t, err, ErrRevoked)
}

func TestMaterializeRotateDoesNotMutateInput(t *testing.T) {
	now := time.Unix(124, 0).UTC()
	item, _, err := MaterializeCreate(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRO,
	}, now)
	require.NoError(t, err)
	originalHash := item.SecretHash
	hash, hint, secret, err := MaterializeRotate(item)
	require.NoError(t, err)
	require.Equal(t, item.ID, secret.ID)
	require.NotEmpty(t, secret.Secret)
	require.NotEmpty(t, hint)
	require.NotEqual(t, originalHash, hash)
	require.Equal(t, originalHash, item.SecretHash)
}

func TestServiceValidationAndExpiry(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))

	_, err := svc.Create(CreateRequest{SAID: "", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW})
	require.Error(t, err)
	_, err = svc.Create(CreateRequest{SAID: "node-a", Protocol: "ftp", Resource: "volume/devdisk", Mode: ModeRW})
	require.Error(t, err)
	_, err = svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "bad", Mode: ModeRW})
	require.Error(t, err)
	past := now.Add(-time.Second)
	secret, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW, ExpiresAt: &past})
	require.NoError(t, err)
	item, err := svc.Get(secret.ID)
	require.NoError(t, err)
	require.NotNil(t, item.ExpiresAt)
}
