package admin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestWriteAtVolume_RoundTrip(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)

	wresp, err := admin.WriteAtVolume(context.Background(), d, admin.WriteAtVolumeReq{
		Name: "v1", Offset: 0, Data: []byte("hello"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), wresp.Bytes)

	rresp, err := admin.ReadAtVolume(context.Background(), d, admin.ReadAtVolumeReq{
		Name: "v1", Offset: 0, Length: 5,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), rresp.Data)
}

func TestWriteAtVolume_EmptyName(t *testing.T) {
	d := newDeps(t)
	_, err := admin.WriteAtVolume(context.Background(), d, admin.WriteAtVolumeReq{Name: "", Offset: 0, Data: []byte("x")})
	require.Error(t, err)
	if ae, ok := err.(*admin.Error); !ok || ae.Code != "invalid" {
		t.Fatalf("expected invalid error, got %v", err)
	}
}

func TestReadAtVolume_LengthOutOfRange(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	_, err = admin.ReadAtVolume(context.Background(), d, admin.ReadAtVolumeReq{Name: "v1", Offset: 0, Length: 0})
	require.Error(t, err)
	_, err = admin.ReadAtVolume(context.Background(), d, admin.ReadAtVolumeReq{Name: "v1", Offset: 0, Length: 65 << 20})
	require.Error(t, err)
}
