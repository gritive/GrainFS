package admin_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestCredentialHandlersCreateListGetRotateRevoke(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore(), protocred.WithNow(func() time.Time {
		return time.Unix(100, 0).UTC()
	}))

	created, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "node-a", Protocol: "nbd", Resource: "volume/devdisk", Mode: "rw",
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	require.NotEmpty(t, created.Secret)
	require.True(t, strings.HasPrefix(created.ConnectionHint["export_name"], "devdisk@"))

	listed, err := admin.ListCredentials(context.Background(), d, admin.CredentialListReq{SAID: "node-a", Protocol: "nbd"})
	require.NoError(t, err)
	require.Len(t, listed.Credentials, 1)
	require.Empty(t, listed.Credentials[0].Secret)
	require.Equal(t, created.ID, listed.Credentials[0].ID)

	got, err := admin.GetCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Empty(t, got.Secret)
	require.Equal(t, "node-a", got.SAID)

	rotated, err := admin.RotateCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.Equal(t, created.ID, rotated.ID)
	require.NotEmpty(t, rotated.Secret)
	require.NotEqual(t, created.Secret, rotated.Secret)

	revoked, err := admin.RevokeCredential(context.Background(), d, created.ID)
	require.NoError(t, err)
	require.True(t, revoked.Revoked)
	require.Equal(t, created.ID, revoked.ID)
}

func TestCredentialHandlersUnsupportedWhenServiceMissing(t *testing.T) {
	_, err := admin.CreateCredential(context.Background(), newDeps(t), admin.CredentialCreateReq{
		SAID: "node-a", Protocol: "nbd", Resource: "volume/devdisk", Mode: "rw",
	})
	require.Error(t, err)
	var ae *admin.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "unsupported", ae.Code)
}

func TestCredentialHandlersRejectInvalidExpiresAt(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())

	_, err := admin.CreateCredential(context.Background(), d, admin.CredentialCreateReq{
		SAID: "node-a", Protocol: "nbd", Resource: "volume/devdisk", Mode: "rw", ExpiresAt: "tomorrow",
	})
	require.Error(t, err)
	var ae *admin.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "invalid", ae.Code)
}
