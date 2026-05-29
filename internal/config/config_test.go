package config_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_RegisterAndSet(t *testing.T) {
	s := config.NewStore()
	hookFired := 0
	spec := config.BoolSpec{
		Default: false,
		OnReload: func(_ context.Context, v bool) error {
			hookFired++
			return nil
		},
	}
	s.Register("feature.enabled", spec)

	err := s.Set(context.Background(), "feature.enabled", "true")
	require.NoError(t, err)

	got, ok := s.GetBool("feature.enabled")
	assert.True(t, ok)
	assert.True(t, got)
	assert.Equal(t, 1, hookFired)
}

func TestStore_RejectUnknownKey(t *testing.T) {
	s := config.NewStore()

	err := s.Set(context.Background(), "nonexistent.key", "value")
	require.Error(t, err)
	assert.True(t, errors.Is(err, config.ErrUnknownKey))
}

func TestStore_ReloadHookValidationRejects(t *testing.T) {
	s := config.NewStore()
	hookErr := errors.New("hook rejected")
	spec := config.BoolSpec{
		Default: false,
		OnReload: func(_ context.Context, v bool) error {
			return hookErr
		},
	}
	s.Register("flag.x", spec)

	err := s.Set(context.Background(), "flag.x", "true")
	require.Error(t, err)

	// Value should roll back to default (false)
	got, ok := s.GetBool("flag.x")
	assert.True(t, ok)
	assert.False(t, got)
}

func TestStore_ListAllShowsRegistry(t *testing.T) {
	s := config.NewStore()
	s.Register("key.a", config.BoolSpec{Default: true})
	s.Register("key.b", config.StringSpec{Default: "hello"})

	entries := s.ListAll()
	assert.Len(t, entries, 2)

	keys := make(map[string]bool)
	for _, e := range entries {
		keys[e.Key] = true
	}
	assert.True(t, keys["key.a"])
	assert.True(t, keys["key.b"])
}

func TestStore_PostRestore_FiredWithValidatedValues(t *testing.T) {
	s := config.NewStore()
	s.Register("key.cidr", config.StringSpec{Default: ""})

	var gotValues map[string]string
	s.SetPostRestore(func(values map[string]string) {
		gotValues = values
	})

	s.Restore(map[string]string{
		"key.cidr":    "10.0.0.0/8",
		"unknown.key": "ignored",
	})

	require.NotNil(t, gotValues)
	assert.Equal(t, "10.0.0.0/8", gotValues["key.cidr"])
	_, hasUnknown := gotValues["unknown.key"]
	assert.False(t, hasUnknown, "unknown keys must be filtered before postRestore")
}

func TestStore_PostRestore_FiresOncePerRestore(t *testing.T) {
	s := config.NewStore()
	s.Register("key.x", config.BoolSpec{Default: false})

	callCount := 0
	s.SetPostRestore(func(_ map[string]string) { callCount++ })

	s.Restore(map[string]string{"key.x": "true"})
	s.Restore(map[string]string{"key.x": "false"})

	assert.Equal(t, 2, callCount, "postRestore must fire exactly once per Restore call")
}

func TestStore_PostRestore_InvalidEntriesDroppedBeforeCallback(t *testing.T) {
	s := config.NewStore()
	// BoolSpec only accepts "true" or "false" — "notabool" is invalid.
	s.Register("key.b", config.BoolSpec{Default: true})

	var gotValues map[string]string
	s.SetPostRestore(func(values map[string]string) { gotValues = values })

	s.Restore(map[string]string{"key.b": "notabool"})

	// Invalid value must not appear in the snapshot passed to postRestore.
	assert.Empty(t, gotValues, "invalid entries must be dropped before postRestore receives values")
}

func TestStore_PostRestore_NilCallbackIsNoOp(t *testing.T) {
	s := config.NewStore()
	s.Register("key.x", config.StringSpec{Default: "default"})

	// No SetPostRestore call — Restore must not panic.
	assert.NotPanics(t, func() { s.Restore(map[string]string{"key.x": "v"}) })
}

func TestRegisterClusterKeys_AllPresent(t *testing.T) {
	s := config.NewStore()
	config.RegisterClusterKeys(s, config.ReloadHooks{})

	entries := s.ListAll()
	keys := make(map[string]bool, len(entries))
	for _, e := range entries {
		keys[e.Key] = true
	}

	expected := []string{
		"iam.allow-anonymous-bucket-policy",
		"iam.oidc.issuers",
		"iam.pdp",
		"trusted-proxy.cidr",
		"jwt.signing-key-rotate",
		"jwt.signing-key-prune",
		"encryption.rotate-dek",
		"encryption.prune-dek-version",
		"cluster.read-only",
		"audit.deny-only",
	}
	assert.Len(t, entries, len(expected))
	for _, k := range expected {
		assert.True(t, keys[k], "expected key %q to be registered", k)
	}
}

func TestOIDCIssuersConfigValidation(t *testing.T) {
	s := config.NewStore()
	config.RegisterClusterKeys(s, config.ReloadHooks{})
	valid := `[{"name":"example","issuer_url":"https://idp.example.com/","audience":"grainfs","jwks_url":"https://idp.example.com/.well-known/jwks.json","groups_claim":"groups","group_prefix":"oidc:example:"}]`
	require.NoError(t, s.Set(context.Background(), "iam.oidc.issuers", valid))

	err := s.Set(context.Background(), "iam.oidc.issuers", `[{"name":"bad","issuer_url":"http://idp.example.com/"}]`)
	require.Error(t, err)
	require.ErrorIs(t, err, config.ErrInvalidValue)
}

func TestOIDCIssuersConfigRestoreDropsInvalidSnapshot(t *testing.T) {
	s := config.NewStore()
	config.RegisterClusterKeys(s, config.ReloadHooks{})

	s.Restore(map[string]string{"iam.oidc.issuers": `[{"name":"bad","issuer_url":"http://idp.example.com/"}]`})

	got, ok := s.GetString("iam.oidc.issuers")
	require.True(t, ok)
	require.Equal(t, "[]", got)
}

// TestRegisterClusterKeys_RotateDEKDeferred: encryption.rotate-dek stays
// REGISTERED (catalog presence above) but its use is GATED — Set with the
// only valid trigger value "now" must be rejected with the deferral error so
// the active DEK gen can never advance past 0 (R1: per-segment gen framing for
// the logical-WAL/packblob lanes is a future slice; spec decision #5).
func TestRegisterClusterKeys_RotateDEKDeferred(t *testing.T) {
	s := config.NewStore()
	config.RegisterClusterKeys(s, config.ReloadHooks{})

	err := s.Set(context.Background(), "encryption.rotate-dek", "now")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deferred")
}
