package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIAMPDPKeyRegisteredAndValidated(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	RegisterClusterKeys(s, ReloadHooks{})
	// Valid disabled config accepted.
	require.NoError(t, s.Set(ctx, "iam.pdp", `{"enabled":false}`))
	// Enabled with a non-unix endpoint rejected by the validator.
	require.Error(t, s.Set(ctx, "iam.pdp", `{"enabled":true,"endpoint":"https://x/authorize"}`))
}
