// Package pdp implements an optional External Policy Decision Point adapter.
// It chains an external authorizer AFTER GrainFS IAM (deny-override): a request
// is allowed only if BOTH GrainFS and the PDP allow. Disabled by default; when
// enabled it talks HTTP/JSON over a local Unix socket. See
// docs/superpowers/specs/2026-05-28-oidc-federated-iam-boundary-design.md
// "External PDP Adapter — Slice 5 Detailed Design".
package pdp

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// FailurePolicy decides what happens when the PDP is unreachable/erroring while
// GrainFS has already allowed the request.
type FailurePolicy string

const (
	// FailureClosed (default) denies on any PDP failure — the secure default.
	FailureClosed FailurePolicy = "closed"
	// FailureOpen falls back to the GrainFS-only allow on PDP failure.
	FailureOpen FailurePolicy = "open"
)

const (
	defaultTimeout = 2 * time.Second
	maxTimeout     = 10 * time.Second
)

const (
	maxCacheTTL       = 10 * time.Minute
	maxGraceTTL       = time.Hour
	defaultMaxEntries = 4096
)

// CacheConfig is the parsed, validated iam.pdp.cache configuration.
type CacheConfig struct {
	Active     bool
	TTLAllow   time.Duration
	TTLDeny    time.Duration
	MaxEntries int
	GraceTTL   time.Duration
}

// Config is the parsed, validated iam.pdp configuration.
type Config struct {
	Enabled       bool
	SocketPath    string // absolute path from a unix:// endpoint; "" when disabled
	Timeout       time.Duration
	FailurePolicy FailurePolicy
	Cache         CacheConfig
}

type rawConfig struct {
	Enabled       bool      `json:"enabled"`
	Endpoint      string    `json:"endpoint"`
	Timeout       string    `json:"timeout"`
	FailurePolicy string    `json:"failure_policy"`
	Cache         *rawCache `json:"cache"`
}

type rawCache struct {
	TTLAllow   string `json:"ttl_allow"`
	TTLDeny    string `json:"ttl_deny"`
	MaxEntries int    `json:"max_entries"`
	GraceTTL   string `json:"grace_ttl"`
}

// ParseConfig parses and validates the iam.pdp JSON document. It is the single
// parser: internal/config/keys.go registers the key with this as its validator,
// and the decorator calls it per request. A disabled config skips endpoint
// validation (so an operator can stage a config and flip enabled later).
func ParseConfig(raw []byte) (Config, error) {
	var rc rawConfig
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &rc); err != nil {
			return Config{}, fmt.Errorf("iam.pdp: invalid JSON: %w", err)
		}
	}
	c := Config{Enabled: rc.Enabled, Timeout: defaultTimeout, FailurePolicy: FailureClosed}
	if rc.Timeout != "" {
		d, err := time.ParseDuration(rc.Timeout)
		if err != nil {
			return Config{}, fmt.Errorf("iam.pdp: invalid timeout %q: %w", rc.Timeout, err)
		}
		c.Timeout = d
	}
	if rc.FailurePolicy != "" {
		switch FailurePolicy(rc.FailurePolicy) {
		case FailureClosed, FailureOpen:
			c.FailurePolicy = FailurePolicy(rc.FailurePolicy)
		default:
			return Config{}, fmt.Errorf("iam.pdp: failure_policy must be \"closed\" or \"open\", got %q", rc.FailurePolicy)
		}
	}
	if c.Timeout <= 0 || c.Timeout > maxTimeout {
		return Config{}, fmt.Errorf("iam.pdp: timeout must be >0 and <=%s, got %s", maxTimeout, c.Timeout)
	}
	if rc.Cache != nil {
		cache, err := parseCacheConfig(rc.Cache)
		if err != nil {
			return Config{}, err
		}
		c.Cache = cache
	}
	if !c.Enabled {
		// Disabled: timeout/failure_policy format validated above (cheap); endpoint
		// is NOT validated so a config can be staged and enabled later.
		return c, nil
	}
	const unixPrefix = "unix://"
	if !strings.HasPrefix(rc.Endpoint, unixPrefix) {
		return Config{}, fmt.Errorf("iam.pdp: endpoint must be a unix:// socket in this release, got %q", rc.Endpoint)
	}
	c.SocketPath = strings.TrimPrefix(rc.Endpoint, unixPrefix)
	if !strings.HasPrefix(c.SocketPath, "/") {
		return Config{}, fmt.Errorf("iam.pdp: unix:// endpoint needs an absolute socket path, got %q", rc.Endpoint)
	}
	return c, nil
}

// parseCacheConfig parses and validates the iam.pdp.cache JSON block. Durations
// are strings (empty ⇒ 0); caching is Active when either TTL is positive.
func parseCacheConfig(rc *rawCache) (CacheConfig, error) {
	var cc CacheConfig
	parseDur := func(field, s string, cap time.Duration) (time.Duration, error) {
		if s == "" {
			return 0, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("iam.pdp: invalid %s %q: %w", field, s, err)
		}
		if d < 0 {
			return 0, fmt.Errorf("iam.pdp: %s must be >=0, got %s", field, d)
		}
		if d > cap {
			return 0, fmt.Errorf("iam.pdp: %s must be <=%s, got %s", field, cap, d)
		}
		return d, nil
	}
	var err error
	if cc.TTLAllow, err = parseDur("ttl_allow", rc.TTLAllow, maxCacheTTL); err != nil {
		return CacheConfig{}, err
	}
	if cc.TTLDeny, err = parseDur("ttl_deny", rc.TTLDeny, maxCacheTTL); err != nil {
		return CacheConfig{}, err
	}
	if cc.GraceTTL, err = parseDur("grace_ttl", rc.GraceTTL, maxGraceTTL); err != nil {
		return CacheConfig{}, err
	}
	cc.Active = cc.TTLAllow > 0 || cc.TTLDeny > 0
	if rc.MaxEntries < 0 {
		return CacheConfig{}, fmt.Errorf("iam.pdp: max_entries must be >=0, got %d", rc.MaxEntries)
	}
	if rc.MaxEntries <= 0 {
		if cc.Active {
			cc.MaxEntries = defaultMaxEntries
		}
	} else {
		cc.MaxEntries = rc.MaxEntries
	}
	if cc.GraceTTL > 0 && !cc.Active {
		return CacheConfig{}, fmt.Errorf("iam.pdp: grace_ttl requires caching (set ttl_allow or ttl_deny)")
	}
	return cc, nil
}
