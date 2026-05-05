package resourcewatch

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoroutineProvider_Snapshot_ReturnsCurrentCount(t *testing.T) {
	p := NewGoroutineProvider(GoroutineProviderOptions{Limit: 20000})

	before := runtime.NumGoroutine()
	sample, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	after := runtime.NumGoroutine()

	assert.GreaterOrEqual(t, sample.Open, before-2, "sample should be at most 2 lower than pre-call NumGoroutine")
	assert.LessOrEqual(t, sample.Open, after+2, "sample should be at most 2 higher than post-call NumGoroutine")
}

func TestGoroutineProvider_Snapshot_LimitFromOptions(t *testing.T) {
	p := NewGoroutineProvider(GoroutineProviderOptions{Limit: 12345})
	sample, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 12345, sample.Limit)
}

func TestGoroutineProvider_Snapshot_NoCategories(t *testing.T) {
	p := NewGoroutineProvider(GoroutineProviderOptions{Limit: 20000})
	sample, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	assert.Nil(t, sample.Categories, "goroutine sample carries no categories — pprof handles breakdown")
}

func TestGoroutineProvider_Snapshot_CollectedAtIsRecent(t *testing.T) {
	p := NewGoroutineProvider(GoroutineProviderOptions{Limit: 20000})
	before := time.Now().UTC()
	sample, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, sample.CollectedAt.Before(before))
	assert.False(t, sample.CollectedAt.After(after))
}

func TestGoroutineProvider_Snapshot_InvalidLimitReturnsError(t *testing.T) {
	p := NewGoroutineProvider(GoroutineProviderOptions{Limit: 0})
	_, err := p.Snapshot(context.Background())
	assert.ErrorIs(t, err, ErrInvalidSample)
}
