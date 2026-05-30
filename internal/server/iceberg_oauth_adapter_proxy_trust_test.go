package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

type oauthAdapterAuthorizer struct {
	lastReq policy.RequestContext
}

func (a *oauthAdapterAuthorizer) Authorize(_ context.Context, _, _ string, ctxReq policy.RequestContext) policy.EvalResult {
	a.lastReq = ctxReq
	return policy.EvalResult{Decision: policy.DecisionAllow}
}

// TestIcebergOAuthAdapterUsesTrustedProxySourceIP drives the assembled server's
// OAuth token endpoint over loopback (a trusted proxy hop) and asserts the
// authorizer sees the X-Forwarded-For client IP — i.e. the iceberg OAuth adapter
// threads the proxy-trust-resolved source IP into the authz request. The
// proxy-trust resolution itself (CIDR/XFF semantics) is covered separately in
// proxy_trust_integration_test.go; here we verify the adapter wiring end-to-end.
func TestIcebergOAuthAdapterUsesTrustedProxySourceIP(t *testing.T) {
	h := newIAMTestHelper(t)
	h.applySACreate(t, "sa-test")
	h.applyKeyCreate(t, "AKIA-test", "sa-test", "secret")

	ks := iamjwt.NewKeySet()
	_, err := ks.GenerateCurrent()
	require.NoError(t, err)

	authz := &oauthAdapterAuthorizer{}
	base := setupTestServerWithOptions(t,
		WithIAMStore(h.store),
		WithJWTKeySet(ks),
		WithPolicyAuthorizer(authz),
		// The test client connects over loopback, so trust 127.0.0.0/8 as the
		// proxy hop and let the X-Forwarded-For below carry the real client IP.
		WithProxyTrust(NewProxyTrust([]string{"127.0.0.0/8"})),
	)

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {"AKIA-test"},
		"client_secret": {"secret"},
		"scope":         {"PRINCIPAL_ROLE:analytics"},
	}
	req, err := http.NewRequest(http.MethodPost, base+"/iceberg/v1/oauth/tokens", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-For", "198.51.100.3")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "198.51.100.3", authz.lastReq.SourceIP)
}
