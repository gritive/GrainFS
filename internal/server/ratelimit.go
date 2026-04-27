package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"golang.org/x/time/rate"
)

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// RateLimiter implements per-key rate limiting with TTL-based cleanup.
type RateLimiter struct {
	mu         sync.RWMutex
	limiters   map[string]*limiterEntry
	rateLimit  rate.Limit
	burst      int
	maxEntries int
}

// NewRateLimiter creates a rate limiter with the given requests/sec, burst size,
// and maximum number of tracked keys. A non-positive rps disables the limiter
// (returns nil); callers must handle the nil case (the middleware does).
func NewRateLimiter(rps float64, burst, maxEntries int) *RateLimiter {
	if rps <= 0 {
		return nil
	}
	rl := &RateLimiter{
		limiters:   make(map[string]*limiterEntry),
		rateLimit:  rate.Limit(rps),
		burst:      burst,
		maxEntries: maxEntries,
	}
	return rl
}

// Allow checks if the key is within rate limits. A nil receiver always allows
// (limiter disabled).
func (rl *RateLimiter) Allow(key string) bool {
	if rl == nil {
		return true
	}
	rl.mu.Lock()
	e, ok := rl.limiters[key]
	if !ok {
		// Evict oldest if at capacity
		if len(rl.limiters) >= rl.maxEntries {
			rl.evictOldest()
		}
		e = &limiterEntry{
			limiter:  rate.NewLimiter(rl.rateLimit, rl.burst),
			lastSeen: time.Now(),
		}
		rl.limiters[key] = e
	}
	e.lastSeen = time.Now()
	rl.mu.Unlock()

	return e.limiter.Allow()
}

// StartCleanup runs a background goroutine that cleans up stale entries.
// A nil receiver is a no-op (limiter disabled).
func (rl *RateLimiter) StartCleanup(ctx context.Context, ttl time.Duration) {
	if rl == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(ttl / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.cleanup(ttl)
			}
		}
	}()
}

func (rl *RateLimiter) cleanup(ttl time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := time.Now().Add(-ttl)
	for key, e := range rl.limiters {
		if e.lastSeen.Before(cutoff) {
			delete(rl.limiters, key)
		}
	}
}

func (rl *RateLimiter) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for key, e := range rl.limiters {
		if first || e.lastSeen.Before(oldestTime) {
			oldestKey = key
			oldestTime = e.lastSeen
			first = false
		}
	}
	if !first {
		delete(rl.limiters, oldestKey)
	}
}

// ipRateLimitMiddleware limits requests per source IP (pre-auth DDoS defense).
func (s *Server) ipRateLimitMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		ip := clientIP(c)
		if !s.ipLimiter.Allow(ip) {
			writeXMLError(c, consts.StatusTooManyRequests, "SlowDown", "rate limit exceeded")
			c.Abort()
			return
		}
		c.Next(ctx)
	}
}

// userRateLimitMiddleware limits requests per authenticated user (post-auth tenant isolation).
func (s *Server) userRateLimitMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		accessKey := AccessKeyFromContext(ctx)
		if accessKey == "" {
			c.Next(ctx)
			return
		}
		if !s.userLimiter.Allow(accessKey) {
			writeXMLError(c, consts.StatusTooManyRequests, "SlowDown", "rate limit exceeded for user")
			c.Abort()
			return
		}
		c.Next(ctx)
	}
}

func clientIP(c *app.RequestContext) string {
	// Check X-Forwarded-For first
	if xff := string(c.GetHeader("X-Forwarded-For")); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		return strings.TrimSpace(parts[0])
	}
	// Fall back to remote address
	addr := c.RemoteAddr().String()
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}
