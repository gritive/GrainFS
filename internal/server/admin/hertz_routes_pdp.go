package admin

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gritive/GrainFS/internal/adminapi"
)

// registerIAMPDP wires the External-PDP bearer-token admin endpoints:
//   - POST   /v1/iam/pdp/token   (set; token in request BODY, never the URL)
//   - DELETE /v1/iam/pdp/token   (clear)
//   - GET    /v1/iam/pdp/status  (show; never returns the token)
//
// nil PDPTokens or ConfigProposer disables the endpoints (the feature is wired
// only when the runtime supplies a token manager + proposer).
func registerIAMPDP(g router, d *Deps) {
	if d == nil || d.PDPTokens == nil || d.ConfigProposer == nil || d.ConfigStore == nil {
		return
	}
	actor := adminActorMiddleware(d)
	writeAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigWrite",
		resource: adminConfigResource,
	})
	deleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigDelete",
		resource: adminConfigResource,
	})
	readAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminConfigRead",
		resource: adminConfigResource,
	})
	g.POST(routePathIAMPDPToken, actor, writeAuthz, pdpSetTokenHandler(d))
	g.DELETE(routePathIAMPDPToken, actor, deleteAuthz, pdpClearTokenHandler(d))
	g.GET(routePathIAMPDPStatus, actor, readAuthz, pdpStatusHandler(d))
}

func pdpSetTokenHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req adminapi.PDPSetTokenReq
		if err := decodeOptionalJSON(c.Request.Body(), &req); err != nil {
			writeError(c, err)
			return
		}
		if req.Token == "" {
			writeError(c, NewInvalid("pdp set-token: empty token"))
			return
		}
		if err := PDPSetToken(ctx, d, req.Token); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func pdpClearTokenHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := PDPClearToken(ctx, d); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func pdpStatusHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		status, err := PDPStatus(ctx, d)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, adminapi.PDPStatusResp{Status: status})
	}
}
