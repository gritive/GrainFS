package pdp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func pemEncodeCert(t *testing.T, c *x509.Certificate) string {
	t.Helper()
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw}))
}

func hostOf(rawURL string) string { u, _ := url.Parse(rawURL); return u.Host }

func TestClientAuthorize(t *testing.T) {
	var gotBody Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/authorize", r.URL.Path)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Empty(t, r.Header.Get("Authorization")) // no token on http
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"decision":"Allow ","reason":"ok"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{Enabled: true, Scheme: "http", RemoteURL: srv.URL, Host: hostOf(srv.URL), Timeout: 2 * time.Second, FailurePolicy: FailureClosed}, "")
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{Enabled: true, Scheme: "http", RemoteURL: srv.URL, Host: hostOf(srv.URL), Timeout: time.Second, FailurePolicy: FailureClosed}, "")
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
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			c := NewClient(Config{Enabled: true, Scheme: "http", RemoteURL: srv.URL, Host: hostOf(srv.URL), Timeout: time.Second, FailurePolicy: FailureClosed}, "")
			_, etype, err := c.Authorize(context.Background(), Request{SchemaVersion: 1})
			require.Error(t, err)
			require.Equal(t, tc.etype, etype)
		})
	}
}

func TestClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{Enabled: true, Scheme: "http", RemoteURL: srv.URL, Host: hostOf(srv.URL), Timeout: 50 * time.Millisecond, FailurePolicy: FailureClosed}, "")
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

func TestClient_HTTPS_BearerAndTLS(t *testing.T) {
	var gotAuth string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]string{"decision": "allow"})
	}))
	defer srv.Close()
	caPEM := pemEncodeCert(t, srv.Certificate())
	cfg := Config{
		Enabled: true, Scheme: "https", RemoteURL: srv.URL, Host: hostOf(srv.URL),
		Timeout: 2 * time.Second, TLS: TLSConfig{CAPEM: caPEM, MinVersion: tls.VersionTLS12},
		SSRF: SSRFConfig{AllowPrivate: true}, // httptest binds 127.0.0.1
	}
	c := NewClient(cfg, "tok-123")
	dec, errType, err := c.Authorize(context.Background(), Request{SchemaVersion: 1, Action: "x", Resource: "y"})
	if err != nil || errType != "" || dec != DecisionAllow {
		t.Fatalf("dec=%q errType=%q err=%v", dec, errType, err)
	}
	if gotAuth != "Bearer tok-123" {
		t.Fatalf("Authorization = %q, want Bearer tok-123", gotAuth)
	}
	tr, ok := c.hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", c.hc.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("Transport.Proxy must be nil (SSRF: no proxy bypass)")
	}
}

func TestClient_HTTP_NoBearer(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]string{"decision": "allow"})
	}))
	defer srv.Close()
	cfg := Config{Enabled: true, Scheme: "http", RemoteURL: srv.URL, Host: hostOf(srv.URL), Timeout: 2 * time.Second}
	c := NewClient(cfg, "tok-should-not-send")
	if _, _, err := c.Authorize(context.Background(), Request{SchemaVersion: 1}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization must be empty on http, got %q", gotAuth)
	}
}

func TestClient_SSRF_BlocksPrivateHTTPS(t *testing.T) {
	cfg := Config{Enabled: true, Scheme: "https", RemoteURL: "https://10.0.0.5:8443", Host: "10.0.0.5:8443", Timeout: time.Second}
	c := NewClient(cfg, "")
	_, errType, err := c.Authorize(context.Background(), Request{SchemaVersion: 1})
	if err == nil || errType != ErrTypeSSRF {
		t.Fatalf("want ErrTypeSSRF, got errType=%q err=%v", errType, err)
	}
}
