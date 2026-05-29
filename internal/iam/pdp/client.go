package pdp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Decision values returned by Authorize after trimming/lowercasing the PDP reply.
const (
	DecisionAllow = "allow"
	DecisionDeny  = "deny"
)

// Error types classify a PDP *failure* (never a deny) for metrics. An empty
// errType means the call succeeded (allow or authoritative deny).
const (
	ErrTypeTimeout         = "timeout"
	ErrTypeTransport       = "transport"
	ErrTypeStatus          = "status"
	ErrTypeDecode          = "decode"
	ErrTypeInvalidDecision = "invalid_decision"
)

const (
	authorizePath   = "/authorize"
	maxResponseSize = 64 << 10
	maxReasonLen    = 256
)

// WirePrincipal is the JSON shape of the authenticated principal sent to the PDP.
type WirePrincipal struct {
	Kind         string   `json:"kind"`
	ID           string   `json:"id"`
	Issuer       string   `json:"issuer,omitempty"`
	Subject      string   `json:"subject,omitempty"`
	Groups       []string `json:"groups,omitempty"`
	CredentialID string   `json:"credential_id,omitempty"`
}

// Request is the JSON body POSTed to the PDP /authorize endpoint.
type Request struct {
	SchemaVersion int               `json:"schema_version"`
	RequestID     string            `json:"request_id"`
	Principal     WirePrincipal     `json:"principal"`
	Action        string            `json:"action"`
	Resource      string            `json:"resource"`
	Protocol      string            `json:"protocol"`
	Context       map[string]string `json:"context,omitempty"`
}

// response is the JSON shape returned by the PDP.
type response struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
}

// DenyError is returned (alongside DecisionDeny and an empty errType) when the
// PDP authoritatively denies. Callers use errors.As to distinguish an
// authoritative deny from a PDP failure.
type DenyError struct {
	Reason string
}

func (e *DenyError) Error() string {
	if e.Reason == "" {
		return "pdp: denied"
	}
	return "pdp: denied: " + e.Reason
}

// Client talks HTTP/JSON to the external PDP over a Unix socket.
type Client struct {
	hc *http.Client
}

// NewClient builds a Client whose HTTP transport dials the configured Unix
// socket and never follows redirects.
func NewClient(cfg Config) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", cfg.RemoteURL) // TASK1-STUB: replaced by Task 3 (remote http/https transport)
		},
	}
	return &Client{
		hc: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Close releases idle keep-alive connections held by the client's transport.
// The decorator calls it when the cached client is replaced (endpoint
// hot-reload) so stale connections to the old socket don't leak.
func (c *Client) Close() {
	c.hc.CloseIdleConnections()
}

// Authorize POSTs the request to the PDP and classifies the outcome:
//   - allow:   (DecisionAllow, "", nil)
//   - deny:    (DecisionDeny, "", *DenyError{Reason})  — authoritative, NOT a failure
//   - failure: ("", errType, err)                      — timeout/transport/status/decode/invalid
//
// The caller owns the per-request deadline: Authorize uses the passed ctx
// as-is and applies no timeout of its own.
func (c *Client) Authorize(ctx context.Context, req Request) (string, string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", ErrTypeTransport, fmt.Errorf("pdp: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://pdp"+authorizePath, bytes.NewReader(body))
	if err != nil {
		return "", ErrTypeTransport, fmt.Errorf("pdp: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.hc.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", ErrTypeTimeout, fmt.Errorf("pdp: request timed out: %w", err)
		}
		return "", ErrTypeTransport, fmt.Errorf("pdp: transport error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", ErrTypeStatus, fmt.Errorf("pdp: unexpected status %d", resp.StatusCode)
	}

	var r response
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseSize)).Decode(&r); err != nil {
		// A deadline firing mid body-read/decode surfaces here; classify it as a
		// timeout rather than a malformed response.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return "", ErrTypeTimeout, fmt.Errorf("pdp: request timed out: %w", err)
		}
		return "", ErrTypeDecode, fmt.Errorf("pdp: decode response: %w", err)
	}

	switch strings.ToLower(strings.TrimSpace(r.Decision)) {
	case DecisionAllow:
		return DecisionAllow, "", nil
	case DecisionDeny:
		return DecisionDeny, "", &DenyError{Reason: sanitizeReason(r.Reason)}
	default:
		return "", ErrTypeInvalidDecision, fmt.Errorf("pdp: invalid decision %q", r.Decision)
	}
}

// sanitizeReason makes a PDP-supplied reason safe to log/return: valid UTF-8,
// no control characters, capped to maxReasonLen bytes.
func sanitizeReason(s string) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	if len(s) > maxReasonLen {
		s = s[:maxReasonLen]
		// Ensure we don't cut mid-rune.
		for len(s) > 0 && !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
