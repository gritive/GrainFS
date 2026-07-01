package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestNewWithServerStorageUsesProvidedOperationsAndPolicyStore(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)
	store := policy.NewCompiledPolicyStore()
	ops := storage.NewOperations(backend, storage.WithPolicyStore(store))

	s := NewWithServerStorage("127.0.0.1:0", ServerStorage{
		Ops:     ops,
		Backend: backend,
	}, store)

	require.Same(t, ops, s.ops)
	require.Same(t, store, s.policyStore)
}

func TestNewWithServerStorageUsesStorageBackendAsHandlerBackend(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)

	s := NewWithServerStorage("127.0.0.1:0", NewServerStorage(backend, nil), nil)

	require.Same(t, backend, s.backend)
}

func TestNormalizeServerStorageFillsPolicyOps(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)

	store := policy.NewCompiledPolicyStore()
	ss, gotStore := normalizeServerStorage(ServerStorage{Backend: backend}, store)

	require.Same(t, store, gotStore)
	require.NotNil(t, ss.Ops)
	require.Same(t, backend, ss.Backend)
	require.Same(t, backend, ss.Ops.Backend())
}

func TestNormalizeServerStorageDerivesBackendFromOperations(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)

	ops := storage.NewOperations(backend)
	ss, gotStore := normalizeServerStorage(ServerStorage{Ops: ops}, nil)

	require.NotNil(t, gotStore)
	require.Same(t, ops, ss.Ops)
	require.Same(t, backend, ss.Backend)
}
