package pdp

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newUnixPDP starts an httptest server on a unix socket; returns the socket path.
func newUnixPDP(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "pdp.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: handler}}
	srv.Start()
	t.Cleanup(srv.Close)
	return sock
}

func TestClientAuthorize(t *testing.T) {
	var gotBody Request
	sock := newUnixPDP(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/authorize", r.URL.Path)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Empty(t, r.Header.Get("Authorization")) // no token in slice 1
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"decision":"Allow ","reason":"ok"}`))
	})
	c := NewClient(Config{Enabled: true, SocketPath: sock, Timeout: 2 * time.Second, FailurePolicy: FailureClosed})
	dec, etype, err := c.Authorize(context.Background(), Request{
		SchemaVersion: 1, RequestID: "req-1",
		Principal: WirePrincipal{Kind: "oidc", ID: "oidc:abc:u1", Groups: []string{"g"}},
		Action:    "grainfs:CredentialCreate", Resource: "protocol-credential/nbd/v/d", Protocol: "admin",
	})
	require.NoError(t, err)
	require.Equal(t, "", etype)
	require.Equal(t, DecisionAllow, dec) // case-insensitive + trimmed
	require.Equal(t, "oidc:abc:u1", gotBody.Principal.ID)
}

func TestClientAuthorizeDeny(t *testing.T) {
	sock := newUnixPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
	})
	c := NewClient(Config{Enabled: true, SocketPath: sock, Timeout: time.Second, FailurePolicy: FailureClosed})
	dec, etype, err := c.Authorize(context.Background(), Request{SchemaVersion: 1})
	// Deny must be distinguishable from failure. Assert via whatever shape you chose:
	require.Equal(t, "", etype) // a deny is NOT a failure (no errType)
	require.Equal(t, DecisionDeny, dec)
	var de *DenyError
	require.ErrorAs(t, err, &de) // deny carries a *DenyError with the reason
	require.Equal(t, "blocked", de.Reason)
}

func TestClientAuthorizeFailures(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		etype   string
	}{
		{"500", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, ErrTypeStatus},
		{"bad json", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{`)) }, ErrTypeDecode},
		{"unknown decision", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"decision":"maybe"}`)) }, ErrTypeInvalidDecision},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sock := newUnixPDP(t, tc.handler)
			c := NewClient(Config{Enabled: true, SocketPath: sock, Timeout: time.Second, FailurePolicy: FailureClosed})
			_, etype, err := c.Authorize(context.Background(), Request{SchemaVersion: 1})
			require.Error(t, err)
			require.Equal(t, tc.etype, etype)
		})
	}
}

func TestClientTimeout(t *testing.T) {
	sock := newUnixPDP(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	c := NewClient(Config{Enabled: true, SocketPath: sock, Timeout: 50 * time.Millisecond, FailurePolicy: FailureClosed})
	// The caller owns the deadline now: the client no longer applies cfg.Timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, etype, err := c.Authorize(ctx, Request{SchemaVersion: 1})
	require.Error(t, err)
	require.Equal(t, ErrTypeTimeout, etype)
}

func TestSanitizeReason(t *testing.T) {
	require.Equal(t, "ok", sanitizeReason("ok\x00\x07"))
	long := strings.Repeat("a", 500)
	require.LessOrEqual(t, len(sanitizeReason(long)), maxReasonLen)
}
