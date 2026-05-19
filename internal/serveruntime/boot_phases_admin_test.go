package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestBootHTTPServerAndAdminWiresBucketWithPolicyProposer(t *testing.T) {
	dataDir, err := os.MkdirTemp("/tmp", "gf-admin-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	backend, err := storage.NewLocalBackend(filepath.Join(dataDir, "objects"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })

	proposer := &iam.MetaProposer{
		Propose: func(context.Context, clusterpb.MetaCmdType, []byte) error {
			return nil
		},
	}
	state := &bootState{
		cfg: Config{
			Addr:        "127.0.0.1:0",
			DataDir:     dataDir,
			AdminSocket: filepath.Join(dataDir, "admin.sock"),
		},
		backend:     backend,
		iamProposer: proposer,
	}
	t.Cleanup(state.Cleanup)

	require.NoError(t, bootHTTPServerAndAdmin(state))
	require.True(t, state.adminDeps.BucketWithPolicyProp == proposer)
}

func TestStorageProtocolStatusFromConfig(t *testing.T) {
	resp := storageProtocolStatusFromConfig(Config{
		NFS4Port: 2049,
		NBDPort:  0,
		P9Bind:   "127.0.0.1",
		P9Port:   564,
	})

	require.True(t, resp.NFS4.Enabled)
	require.Equal(t, 2049, resp.NFS4.Port)
	require.False(t, resp.NBD.Enabled)
	require.True(t, resp.P9.Enabled)
	require.Equal(t, "127.0.0.1", resp.P9.Bind)
	require.Equal(t, 564, resp.P9.Port)
}
