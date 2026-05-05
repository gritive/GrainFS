package server

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutationGateAllowsWritesByDefault(t *testing.T) {
	gate := NewMutationGate(nil)
	require.NoError(t, gate.Check("put_object"))
}

func TestMutationGateBlocksWritesWhenReadOnly(t *testing.T) {
	gate := NewMutationGate(errors.New("startup recovery read-only"))
	err := gate.Check("cluster_join")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "startup recovery read-only")
}

func TestMutationGateBlockResponseShape(t *testing.T) {
	gate := NewMutationGate(errors.New("blocked"))
	resp, blocked := gate.BlockResponse("test_mutation")

	require.True(t, blocked)
	assert.Equal(t, http.StatusServiceUnavailable, resp.Status)
	assert.Equal(t, "RecoveryReadOnly", resp.Code)
	assert.Equal(t, "test_mutation", resp.Operation)
	assert.Contains(t, resp.Message, "blocked")
}
