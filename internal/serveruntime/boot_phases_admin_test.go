package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/protocred"
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

func TestBootHTTPServerAndAdminWiresProtocolCredentials(t *testing.T) {
	dataDir, err := os.MkdirTemp("/tmp", "gf-admin-protocred-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	backend, err := storage.NewLocalBackend(filepath.Join(dataDir, "objects"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })

	state := &bootState{
		cfg: Config{
			Addr:        "127.0.0.1:0",
			DataDir:     dataDir,
			AdminSocket: filepath.Join(dataDir, "admin.sock"),
		},
		backend: backend,
	}
	stores, err := WireIAMPolicyStores(context.Background(), nil, 0)
	require.NoError(t, err)
	state.iamPolicyStores = stores
	state.cfgStore = config.NewStore()
	t.Cleanup(state.Cleanup)

	require.NoError(t, bootHTTPServerAndAdmin(state))
	require.NotNil(t, state.adminDeps.ProtocolCredentials)
	require.NotNil(t, state.adminDeps.ProtocolCredAuthz)
	require.NotNil(t, state.adminDeps.AdminAuthz)
	require.NotNil(t, state.adminDeps.ActorAuth)
}

func TestBootHTTPServerAndAdminReusesStandaloneProtocolCredentialStore(t *testing.T) {
	dataDir, err := os.MkdirTemp("/tmp", "gf-admin-protocred-standalone-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	backend, err := storage.NewLocalBackend(filepath.Join(dataDir, "objects"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })

	store := protocred.NewStore()
	state := &bootState{
		cfg: Config{
			Addr:        "127.0.0.1:0",
			DataDir:     dataDir,
			AdminSocket: filepath.Join(dataDir, "admin.sock"),
		},
		backend:                 backend,
		protocolCredentialStore: store,
	}
	stores, err := WireIAMPolicyStores(context.Background(), nil, 0)
	require.NoError(t, err)
	state.iamPolicyStores = stores
	state.cfgStore = config.NewStore()
	t.Cleanup(state.Cleanup)

	require.NoError(t, bootHTTPServerAndAdmin(state))
	require.Same(t, store, state.protocolCredentialStore)
	require.NotNil(t, state.adminDeps.ProtocolCredentials)
}

func TestBootHTTPServerAndAdminUsesDurableProtocolCredentialsWhenMetaRaftIsWired(t *testing.T) {
	dataDir, err := os.MkdirTemp("/tmp", "gf-admin-protocred-durable-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	backend, err := storage.NewLocalBackend(filepath.Join(dataDir, "objects"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:  "node-a",
		RaftID:  "node-a",
		DataDir: dataDir,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metaRaft.Close()) })

	store := protocred.NewStore()
	metaRaft.FSM().SetProtocolCredentialStore(store)
	state := &bootState{
		cfg: Config{
			Addr:        "127.0.0.1:0",
			DataDir:     dataDir,
			AdminSocket: filepath.Join(dataDir, "admin.sock"),
		},
		backend:                 backend,
		metaRaft:                metaRaft,
		protocolCredentialStore: store,
	}
	t.Cleanup(state.Cleanup)

	require.NoError(t, bootHTTPServerAndAdmin(state))
	require.IsType(t, &cluster.ProtocolCredentialService{}, state.adminDeps.ProtocolCredentials)
	require.Same(t, store, state.protocolCredentialStore)
}
