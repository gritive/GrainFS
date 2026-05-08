package server

import (
	"context"
	"encoding/xml"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// stubPolicyChecker satisfies s3auth.PolicyChecker with a fixed verdict.
type stubPolicyChecker struct{ allow bool }

func (s stubPolicyChecker) Allow(ctx context.Context, in s3auth.PermCheckInput) bool {
	return s.allow
}

// newAuthzHelperServer wires a *Server with just enough state for the helper
// methods. policyAllow controls Layer 2; nil iam keeps auth disabled so
// Layer 1 is skipped and Layer 3 (PostLoad ACL) drives the decision.
func newAuthzHelperServer(policyAllow bool) *Server {
	return &Server{
		authz: s3auth.NewRequestAuthorizer(
			nil, // iam disabled — Layer 1 skipped
			nil,
			stubPolicyChecker{allow: policyAllow},
			nil,
			nil,
		),
	}
}

func TestMustAuthorize_Allow_NoResponseWritten(t *testing.T) {
	s := newAuthzHelperServer(true)
	c := app.NewContext(0)

	denied := s.mustAuthorize(context.Background(), c, "bucket", "key", s3auth.GetObject)

	assert.False(t, denied, "policy allow → denied=false")
	assert.Empty(t, c.Response.Body(), "no body written on allow")
}

func TestMustAuthorize_Deny_Writes403XML(t *testing.T) {
	s := newAuthzHelperServer(false)
	c := app.NewContext(0)

	denied := s.mustAuthorize(context.Background(), c, "bucket", "key", s3auth.GetObject)

	require.True(t, denied, "policy deny → denied=true")
	assert.Equal(t, consts.StatusForbidden, c.Response.StatusCode())
	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "AccessDenied", got.Code)
	assert.Equal(t, "Access Denied", got.Message)
}

func TestMustAuthorizePostLoad_Allow_PublicReadWrite(t *testing.T) {
	// iam=nil + policy=allow → Layer 1/2 pass. Layer 3: PutObject vs
	// ACLPublicReadWrite is permitted (anonymous writes allowed).
	s := newAuthzHelperServer(true)
	c := app.NewContext(0)

	denied := s.mustAuthorizePostLoad(context.Background(), c, "bucket", "key",
		s3auth.PutObject, uint8(s3auth.ACLPublicReadWrite))

	assert.False(t, denied)
	assert.Empty(t, c.Response.Body())
}

func TestMustAuthorizePostLoad_Deny_ACLPrivateAnonymous(t *testing.T) {
	// iam=nil + policy=allow → Layer 1/2 pass. Layer 3: ACLPrivate denies
	// anonymous (accessKey=""). Confirms aclByte→ACLGrant conversion and
	// that ACL deny path emits 403.
	s := newAuthzHelperServer(true)
	c := app.NewContext(0)

	denied := s.mustAuthorizePostLoad(context.Background(), c, "bucket", "key",
		s3auth.GetObject, uint8(s3auth.ACLPrivate))

	require.True(t, denied)
	assert.Equal(t, consts.StatusForbidden, c.Response.StatusCode())
	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "AccessDenied", got.Code)
}

func TestMustAuthorizePostLoad_AccessKeyFromContext(t *testing.T) {
	// With an accessKey in ctx, ACLPrivate must permit (authenticated owner).
	// This locks in that the helper sources Principal.AccessKey from ctx.
	s := newAuthzHelperServer(true)
	c := app.NewContext(0)
	ctx := WithAccessKey(context.Background(), "AKID")

	denied := s.mustAuthorizePostLoad(ctx, c, "bucket", "key",
		s3auth.GetObject, uint8(s3auth.ACLPrivate))

	assert.False(t, denied, "authenticated request to ACLPrivate must pass")
	assert.Empty(t, c.Response.Body())
}
