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

func TestIAMPDPTokenKeyRegistered(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	RegisterClusterKeys(s, ReloadHooks{})
	// A well-formed sealed envelope is accepted.
	require.NoError(t, s.Set(ctx, "iam.pdp.token", `{"ct_b64":"YWJj","dek_gen":1}`))
	// A bare plaintext token (not an envelope) is rejected by the validator —
	// this is the load-bearing protection against an unsealed token at rest.
	require.Error(t, s.Set(ctx, "iam.pdp.token", `plaintext`))
	// Empty (cleared) is allowed: it reverts the key to its default.
	require.NoError(t, s.Set(ctx, "iam.pdp.token", ``))
}
