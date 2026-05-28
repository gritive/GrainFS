package pdp

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/stretchr/testify/require"
)

type spyInner struct {
	decision policy.Decision
	calls    int32
}

func (s *spyInner) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	atomic.AddInt32(&s.calls, 1)
	return policy.EvalResult{Decision: s.decision}
}
func (s *spyInner) AuthorizePrincipal(_ context.Context, _ principal.Principal, _ string, _ policy.RequestContext) policy.EvalResult {
	atomic.AddInt32(&s.calls, 1)
	return policy.EvalResult{Decision: s.decision}
}

type staticCfg string

func (s staticCfg) GetString(key string) (string, bool) {
	if key == ConfigKey {
		return string(s), true
	}
	return "", false
}

func decoUnixPDP(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "pdp.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: h}}
	srv.Start()
	t.Cleanup(srv.Close)
	return sock
}

func decoCfg(sock, policyMode string) string {
	b, _ := json.Marshal(map[string]any{"enabled": true, "endpoint": "unix://" + sock, "failure_policy": policyMode})
	return string(b)
}

func TestDecoratorDisabledIsPassThrough(t *testing.T) {
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg(`{"enabled":false}`))
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, inner.calls)
}

type missingCfg struct{}

func (missingCfg) GetString(string) (string, bool) { return "", false }

func TestDecoratorMalformedConfigIsPassThrough(t *testing.T) {
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg("{"))
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, inner.calls)

	inner2 := &spyInner{decision: policy.DecisionAllow}
	d2 := NewDecorator(inner2, staticCfg("{"))
	got2 := d2.Authorize(context.Background(), "sa", "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got2.Decision)
	require.EqualValues(t, 1, inner2.calls)
}

func TestDecoratorMissingConfigKeyIsPassThrough(t *testing.T) {
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, missingCfg{})
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, inner.calls)

	inner2 := &spyInner{decision: policy.DecisionAllow}
	d2 := NewDecorator(inner2, missingCfg{})
	got2 := d2.Authorize(context.Background(), "sa", "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got2.Decision)
	require.EqualValues(t, 1, inner2.calls)
}

func TestDecoratorShortCircuitsInnerDeny(t *testing.T) {
	var dialed int32
	sock := decoUnixPDP(t, func(w http.ResponseWriter, r *http.Request) { atomic.AddInt32(&dialed, 1) })
	inner := &spyInner{decision: policy.DecisionDeny}
	d := NewDecorator(inner, staticCfg(decoCfg(sock, "closed")))
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.EqualValues(t, 0, dialed)
}

func TestDecoratorAllowAndAllow(t *testing.T) {
	sock := decoUnixPDP(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"decision":"allow"}`)) })
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg(decoCfg(sock, "closed")))
	got := d.AuthorizePrincipal(context.Background(), principal.OIDC("iss", "sub", "oidc:x:u", []string{"g"}), "", policy.RequestContext{Action: "grainfs:CredentialCreate", Resource: "protocol-credential/nbd/v/d"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
}

func TestDecoratorAllowThenPDPDeny(t *testing.T) {
	sock := decoUnixPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
	})
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg(decoCfg(sock, "closed")))
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, "pdp_deny")
}

func TestDecoratorFailClosedVsOpen(t *testing.T) {
	down := decoUnixPDP(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	closed := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(down, "closed")))
	require.Equal(t, policy.DecisionDeny, closed.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"}).Decision)
	open := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(down, "open")))
	got := open.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.Contains(t, got.Reason, "pdp_skipped_fail_open")
}

func TestDecoratorCanceledCtxDeniesEvenFailOpen(t *testing.T) {
	sock := decoUnixPDP(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"decision":"allow"}`)) })
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(sock, "open")))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := d.AuthorizePrincipal(ctx, principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, "request canceled")
}

func TestDecoratorAuthorizeMapsServiceAccount(t *testing.T) {
	var got Request
	sock := decoUnixPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(sock, "closed")))
	d.Authorize(context.Background(), "sa-app", "", policy.RequestContext{Action: "grainfs:CredentialGet", Resource: "protocol-credential/nbd/v/d"})
	require.Equal(t, string(principal.KindServiceAccount), got.Principal.Kind)
	require.Equal(t, "sa-app", got.Principal.ID)
	require.Equal(t, "sa-app", got.Context["target_sa"])
}
