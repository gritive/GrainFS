package server

import (
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type authnFailure struct {
	status  int
	code    string
	message string
	reason  string
}

func (s *Server) authenticateSignedRequest(ctx context.Context, r *http.Request) (context.Context, *authnFailure) {
	accessKey, err := s.verifier.Verify(r)
	if err != nil {
		return ctx, &authnFailure{
			status:  consts.StatusForbidden,
			code:    "AccessDenied",
			message: err.Error(),
			reason:  "authn",
		}
	}
	ctx = WithAccessKey(ctx, accessKey)

	return s.resolveAccessKeyPrincipal(ctx, accessKey)
}
