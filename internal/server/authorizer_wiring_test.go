package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestNewServer_BuildsRequestAuthorizer(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)

	store := policy.NewCompiledPolicyStore()
	s := NewWithServerStorage("127.0.0.1:0", ServerStorage{
		Ops:     storage.NewOperations(backend, storage.WithPolicyStore(store)),
		Backend: backend,
	}, store)

	assert.NotNil(t, s.authz, "server must build a non-nil RequestAuthorizer")
}
