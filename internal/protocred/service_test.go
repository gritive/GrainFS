package protocred

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
