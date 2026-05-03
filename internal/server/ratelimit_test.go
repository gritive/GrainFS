package server

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_AllowUnderLimit(t *testing.T) {
	rl := NewRateLimiter(10, 10, 100) // 10 req/sec, burst 10, max 100 entries
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.True(t, rl.Allow("192.168.1.1"))
}

func TestRateLimiter_RejectOverLimit(t *testing.T) {
	rl := NewRateLimiter(1, 1, 100) // 1 req/sec, burst 1
	assert.True(t, rl.Allow("192.168.1.1"))
	// Second request immediately should be rejected
	assert.False(t, rl.Allow("192.168.1.1"))
}

func TestRateLimiter_DisabledWhenRPSIsZero(t *testing.T) {
	rl := NewRateLimiter(0, 1, 100)
	for range 1000 {
		require.True(t, rl.Allow("192.168.1.1"))
	}
}

func TestWithRateLimits_DisablesServerLimitersWhenRPSIsZero(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	srv := New("127.0.0.1:0", backend, WithRateLimits(0, 1, 0, 1))
	for range 1000 {
		require.True(t, srv.ipLimiter.Allow("192.168.1.1"))
		require.True(t, srv.userLimiter.Allow("bench-user"))
	}
}

func TestRateLimiter_IsolateKeys(t *testing.T) {
	rl := NewRateLimiter(1, 1, 100)
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.False(t, rl.Allow("192.168.1.1"))
	// Different key should be independent
	assert.True(t, rl.Allow("192.168.1.2"))
}

func TestRateLimiter_RecoverAfterWait(t *testing.T) {
	rl := NewRateLimiter(100, 1, 100) // 100 req/sec, burst 1
	assert.True(t, rl.Allow("k"))
	assert.False(t, rl.Allow("k"))
	time.Sleep(15 * time.Millisecond) // wait for replenish
	assert.True(t, rl.Allow("k"))
}

func TestRateLimiter_Cleanup(t *testing.T) {
	rl := NewRateLimiter(10, 10, 100)
	rl.Allow("old-key")

	// Manually set last seen to the past
	rl.mu.Lock()
	if e, ok := rl.limiters["old-key"]; ok {
		e.lastSeen = time.Now().Add(-2 * time.Minute)
	}
	rl.mu.Unlock()

	rl.cleanup(30 * time.Second) // TTL 30s

	rl.mu.RLock()
	_, exists := rl.limiters["old-key"]
	rl.mu.RUnlock()
	assert.False(t, exists, "stale entry should be cleaned up")
}

func TestRateLimiter_MaxEntries(t *testing.T) {
	rl := NewRateLimiter(10, 10, 3) // max 3 entries
	require.True(t, rl.Allow("a"))
	require.True(t, rl.Allow("b"))
	require.True(t, rl.Allow("c"))

	// 4th key: oldest should be evicted
	require.True(t, rl.Allow("d"))

	rl.mu.RLock()
	count := len(rl.limiters)
	rl.mu.RUnlock()
	assert.LessOrEqual(t, count, 3)
}

func TestRateLimiter_StartCleanup(t *testing.T) {
	rl := NewRateLimiter(10, 10, 100)
	rl.Allow("cleanup-key")

	// Set lastSeen to the past so cleanup will remove it
	rl.mu.Lock()
	if e, ok := rl.limiters["cleanup-key"]; ok {
		e.lastSeen = time.Now().Add(-5 * time.Minute)
	}
	rl.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start cleanup with a very short TTL so the ticker fires quickly
	rl.StartCleanup(ctx, 200*time.Millisecond)

	// Wait for cleanup to remove the stale entry
	require.Eventually(t, func() bool {
		rl.mu.RLock()
		_, exists := rl.limiters["cleanup-key"]
		rl.mu.RUnlock()
		return !exists
	}, 3*time.Second, 50*time.Millisecond, "stale entry should be cleaned up by StartCleanup")

	cancel() // Stop the cleanup goroutine
}
