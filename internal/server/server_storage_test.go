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

	s := NewWithServerStorage("127.0.0.1:0", backend, ServerStorage{
		Ops:           ops,
		VolumeBackend: backend,
	}, store)

	require.Same(t, ops, s.ops)
	require.Same(t, store, s.policyStore)
	require.NotNil(t, s.volMgr)
}
