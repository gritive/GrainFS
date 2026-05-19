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
