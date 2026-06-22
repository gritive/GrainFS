package admin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestAdminStorageProtocolsFromDeps(t *testing.T) {
	resp, err := admin.AdminStorageProtocols(context.Background(), &admin.Deps{
		Protocols: adminapi.StorageProtocolStatusResp{
			NFS4: adminapi.ProtocolEndpointStatus{Enabled: true, Port: 2049},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.NFS4.Enabled)
	require.Equal(t, 2049, resp.NFS4.Port)
}
