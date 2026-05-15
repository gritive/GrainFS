package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestNewWithServerStorageUsesProvidedOperationsAndPolicyStore(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	store := policy.NewCompiledPolicyStore()
	ops := storage.NewOperations(backend, storage.WithPolicyStore(store))

	s := NewWithServerStorage("127.0.0.1:0", ServerStorage{
		Ops:           ops,
		Backend:       backend,
		VolumeBackend: backend,
	}, store)

	require.Same(t, ops, s.ops)
	require.Same(t, store, s.policyStore)
	require.NotNil(t, s.volMgr)
}

func TestNewWithServerStorageUsesStorageBackendAsHandlerBackend(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	s := NewWithServerStorage("127.0.0.1:0", NewServerStorage(backend, nil), nil)

	require.Same(t, backend, s.backend)
}

func TestNormalizeServerStorageFillsPolicyOpsAndVolumeBackend(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	store := policy.NewCompiledPolicyStore()
	ss, gotStore := normalizeServerStorage(ServerStorage{Backend: backend}, store)

	require.Same(t, store, gotStore)
	require.NotNil(t, ss.Ops)
	require.Same(t, backend, ss.Backend)
	require.Same(t, backend, ss.Ops.Backend())
	require.Same(t, backend, ss.VolumeBackend)
}

func TestNormalizeServerStorageDerivesBackendFromOperations(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	ops := storage.NewOperations(backend)
	ss, gotStore := normalizeServerStorage(ServerStorage{Ops: ops}, nil)

	require.NotNil(t, gotStore)
	require.Same(t, ops, ss.Ops)
	require.Same(t, backend, ss.Backend)
	require.Same(t, backend, ss.VolumeBackend)
}
