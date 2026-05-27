package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/protocred"
)

func TestProtocolCredentialServiceProposesMutationsAndReadsStore(t *testing.T) {
	store := protocred.NewStore()
	fsm := NewMetaFSM()
	fsm.SetProtocolCredentialStore(store)
	svc := NewProtocolCredentialService(store, func(_ context.Context, typ MetaCmdType, payload []byte) error {
		raw, err := encodeMetaCmd(typ, payload)
		if err != nil {
			return err
		}
		return fsm.applyCmd(raw)
	})
	svc.now = func() time.Time { return time.Unix(200, 0).UTC() }

	created, err := svc.Create(protocred.CreateRequest{
		SAID: "node-a", Protocol: protocred.ProtocolNBD, Resource: "volume/devdisk", Mode: protocred.ModeRW,
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	require.NotEmpty(t, created.Secret)
	item, err := svc.Get(created.ID)
	require.NoError(t, err)
	require.Equal(t, "node-a", item.SAID)
	require.Equal(t, "volume/devdisk", item.Resource)

	listed := svc.List(protocred.ListFilter{SAID: "node-a", Protocol: protocred.ProtocolNBD})
	require.Len(t, listed, 1)
	require.Equal(t, created.ID, listed[0].ID)

	rotated, err := svc.Rotate(created.ID)
	require.NoError(t, err)
	require.Equal(t, created.ID, rotated.ID)
	require.NotEmpty(t, rotated.Secret)
	require.NotEqual(t, created.Secret, rotated.Secret)
	afterRotate, err := svc.Get(created.ID)
	require.NoError(t, err)
	require.NotEqual(t, item.SecretHash, afterRotate.SecretHash)
	require.Greater(t, afterRotate.Generation, item.Generation)

	require.NoError(t, svc.Revoke(created.ID))
	afterRevoke, err := svc.Get(created.ID)
	require.NoError(t, err)
	require.NotNil(t, afterRevoke.RevokedAt)
}

func TestProtocolCredentialServicePropagatesProposeError(t *testing.T) {
	store := protocred.NewStore()
	wantErr := protocred.ErrConflict
	svc := NewProtocolCredentialService(store, func(context.Context, MetaCmdType, []byte) error {
		return wantErr
	})

	_, err := svc.Create(protocred.CreateRequest{
		SAID: "node-a", Protocol: protocred.ProtocolNBD, Resource: "volume/devdisk", Mode: protocred.ModeRW,
	})
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, svc.List(protocred.ListFilter{}))
}
