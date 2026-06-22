package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestStorageProtocolStatusFromConfig(t *testing.T) {
	resp := storageProtocolStatusFromConfig(Config{
		NFS4Port: 2049,
	})

	require.True(t, resp.NFS4.Enabled)
	require.Equal(t, 2049, resp.NFS4.Port)
}

func TestLifecycleCascadeEnabled(t *testing.T) {
	for _, c := range []struct {
		name            string
		metaRaftPresent bool
		interval        time.Duration
		want            bool
	}{
		{"enabled: meta-raft + interval", true, time.Hour, true},
		{"disabled: interval zero (store unwired)", true, 0, false},
		{"disabled: no meta-raft", false, time.Hour, false},
		{"disabled: neither", false, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := lifecycleCascadeEnabled(c.metaRaftPresent, c.interval); got != c.want {
				t.Fatalf("lifecycleCascadeEnabled(%v, %v) = %v, want %v", c.metaRaftPresent, c.interval, got, c.want)
			}
		})
	}
}
