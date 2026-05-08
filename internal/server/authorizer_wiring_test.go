package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestNewServer_BuildsRequestAuthorizer(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	store := policy.NewCompiledPolicyStore()
	s := NewWithServerStorage("127.0.0.1:0", ServerStorage{
		Ops:           storage.NewOperations(backend, storage.WithPolicyStore(store)),
		Backend:       backend,
		VolumeBackend: backend,
	}, store)

	assert.NotNil(t, s.authz, "server must build a non-nil RequestAuthorizer")
}
