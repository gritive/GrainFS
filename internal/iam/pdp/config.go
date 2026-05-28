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

// Config is the parsed, validated iam.pdp configuration.
type Config struct {
	Enabled       bool
	SocketPath    string // absolute path from a unix:// endpoint; "" when disabled
	Timeout       time.Duration
	FailurePolicy FailurePolicy
}

type rawConfig struct {
	Enabled       bool   `json:"enabled"`
	Endpoint      string `json:"endpoint"`
	Timeout       string `json:"timeout"`
	FailurePolicy string `json:"failure_policy"`
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
