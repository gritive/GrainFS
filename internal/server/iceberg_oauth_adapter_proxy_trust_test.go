package server

import (
	"context"
	"net"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
	"github.com/stretchr/testify/require"

	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

type oauthAdapterSAResolver struct{}

func (oauthAdapterSAResolver) LookupByAccessKey(_ context.Context, _ string) (string, []byte, error) {
	return "sa-test", []byte("secret"), nil
}

type oauthAdapterAuthorizer struct {
	lastReq policy.RequestContext
}

func (a *oauthAdapterAuthorizer) Authorize(_ context.Context, _, _ string, ctxReq policy.RequestContext) policy.EvalResult {
	a.lastReq = ctxReq
	return policy.EvalResult{Decision: policy.DecisionAllow}
}

func TestIcebergOAuthAdapterUsesTrustedProxySourceIP(t *testing.T) {
	ks := iamjwt.NewKeySet()
	_, err := ks.GenerateCurrent()
	require.NoError(t, err)

	authz := &oauthAdapterAuthorizer{}
	s := stubServer([]string{"10.0.0.0/8"})
	h := newIcebergOAuthHandler(oauthAdapterSAResolver{}, ks, authz, s.authoritativeClientIP)

	c := app.NewContext(0)
	addr, err := net.ResolveTCPAddr("tcp", "10.0.0.5:7777")
	require.NoError(t, err)
	c.SetConn(&remoteAddrConn{Conn: mock.NewConn(""), addr: addr})
	c.Request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c.Request.Header.Set("X-Forwarded-Proto", "https")
	c.Request.Header.Set("X-Forwarded-For", "198.51.100.3, 10.0.0.5")
	c.Request.SetBodyString("grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE:analytics")

	h.handle(context.Background(), c)

	require.Equal(t, 200, c.Response.StatusCode())
	require.Equal(t, "198.51.100.3", authz.lastReq.SourceIP)
}
