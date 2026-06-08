package transport

import "time"

// Defaults for the dormant TCP cluster transport's resource bounds and pool
// policy (S3b). These are not yet load-bearing in production (TCPTransport is not
// wired into boot); they become operative when S4/S5 wire it in.
const (
	defaultServerIdleTimeout = 60 * time.Second // bounds an idle pooled-conn goroutine/FD (pooled conns are longer-lived than QUIC's 10s per-conn idle, so this is intentionally larger)
	defaultServerBodyTimeout = 5 * time.Minute  // generous for 16MiB+ shard bodies over LAN
	defaultClientBodyTimeout = 5 * time.Minute  // idle bound (reset per Read) on the client response-body read; mirrors ServerBodyTimeout so a stalled mid-body server can't pin a client goroutine + pooled slot forever
	defaultMaxConnsPerPeer   = 64               // elastic cap (NOT a fixed 4; see spec §4 — fixed pool caps throughput)
	defaultPoolIdleTimeout   = 60 * time.Second // idle conn eviction; matches the server idle reap

	// defaultMaxControlConnsPerPeer caps the SEPARATE control-plane conn pool
	// (CallPooled — short metadata forwards). Control RPCs are small/fast, so a
	// modest reserved cap keeps them flowing even when the bulk data-plane pool is
	// saturated by large-object shard streams (the multipart-under-load 500 fix).
	defaultMaxControlConnsPerPeer = 16
)

// TCPTransportConfig tunes the dormant TCP cluster transport. A zero-value field
// falls back to the matching default via withDefaults(), except MaxConnsPerPeer
// where 0 deliberately means "unlimited" (S3a behavior, used by S3a tests).
type TCPTransportConfig struct {
	ServerIdleTimeout time.Duration
	ServerBodyTimeout time.Duration
	ClientBodyTimeout time.Duration // idle bound (reset per Read) on the client-side response-body read; 0 = default
	MaxConnsPerPeer   int           // 0 = unlimited (S3a behavior)
	// MaxControlConnsPerPeer caps the separate control-plane (CallPooled) pool;
	// 0 falls back to defaultMaxControlConnsPerPeer (unlike MaxConnsPerPeer, a
	// control pool is always bounded — that bounded reserve is the whole point).
	MaxControlConnsPerPeer int
	PoolIdleTimeout        time.Duration
	ReadBufferBytes        int // 0 = OS default
	WriteBufferBytes       int // 0 = OS default
	MaxConcurrentConns     int // 0 = unlimited serveConn goroutines
	// TrafficLimits gates inbound per-class admission; the zero value is unlimited
	// (parity with a nil QUIC TrafficLimiter).
	TrafficLimits TrafficLimits
}

// withDefaults fills zero timeouts with their defaults. MaxConnsPerPeer is left
// as-is (0 = unlimited is a valid, intentional choice).
func (c TCPTransportConfig) withDefaults() TCPTransportConfig {
	if c.ServerIdleTimeout == 0 {
		c.ServerIdleTimeout = defaultServerIdleTimeout
	}
	if c.ServerBodyTimeout == 0 {
		c.ServerBodyTimeout = defaultServerBodyTimeout
	}
	if c.ClientBodyTimeout == 0 {
		c.ClientBodyTimeout = defaultClientBodyTimeout
	}
	if c.PoolIdleTimeout == 0 {
		c.PoolIdleTimeout = defaultPoolIdleTimeout
	}
	if c.MaxControlConnsPerPeer == 0 {
		c.MaxControlConnsPerPeer = defaultMaxControlConnsPerPeer
	}
	return c
}
