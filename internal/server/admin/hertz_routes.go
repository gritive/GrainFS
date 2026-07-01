package admin

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/route"
)

// router is the subset of route.IRoutes we use; satisfied by both *server.Hertz
// and *route.RouterGroup so callers can register at any path prefix.
type router interface {
	GET(path string, handlers ...app.HandlerFunc) route.IRoutes
	POST(path string, handlers ...app.HandlerFunc) route.IRoutes
	DELETE(path string, handlers ...app.HandlerFunc) route.IRoutes
	PUT(path string, handlers ...app.HandlerFunc) route.IRoutes
	PATCH(path string, handlers ...app.HandlerFunc) route.IRoutes
}

// RegisterAdmin wires the admin handlers under the `/v1/...` prefix on the
// given Hertz instance. This is what the Unix-socket admin server calls.
func RegisterAdmin(h *server.Hertz, d *Deps) {
	h.Use(peerCredMiddleware())
	g := h.Group(routePrefixAdmin)
	registerScrub(g, d)
	registerCluster(g, d)
	registerResource(g, d)
	registerDashboard(g, d)
	registerIAM(g, d)
	registerConfig(g, d)
	registerIAMPDP(g, d)
	registerBucket(g, d)
	registerCredentials(g, d)
	registerStatus(g, d)
}

// RegisterUI wires a subset of admin handlers under `/ui/api/...` on the
// data-plane Hertz instance. Token auth is the caller's responsibility (install
// the middleware before calling this so unauthorized requests never reach
// handler logic).
func RegisterUI(h *server.Hertz, d *Deps) {
	g := h.Group(routePrefixUI)
	registerScrubUI(g, d)
	registerCluster(g, d)
	registerResource(g, d)
	registerStorageUI(g, d)
	// Dashboard token endpoints are intentionally NOT mounted on /ui/api;
	// they live only on the local admin Unix socket.
	// Policy and group admin endpoints are intentionally NOT mounted on /ui/api:
	// they grant powers (attach Resource:* policies, create groups, modify SA
	// membership) that are root-equivalent. The CLI Resource:* warning lives in
	// the binary; the wire shape carries no such guard. Dashboard-token holders
	// get the SA / Key / BucketUpstream surface only.
	registerIAMUI(g, d)
	// registerBucket is intentionally NOT mounted on /ui/api: AdminGetBucket
	// performs an unbounded CountObjects walk (full Badger scan) that any
	// dashboard-token holder could trigger remotely, causing write starvation.
	// Bucket admin ops are admin-UDS only.
}

// RegisterIAMOnly wires only the IAM admin routes. Used in tests to avoid
// registering all routes which would panic with a nil Manager.
func RegisterIAMOnly(h *server.Hertz, d *Deps) {
	g := h.Group(routePrefixAdmin)
	registerIAM(g, d)
}

func registerStatus(g router, d *Deps) {
	g.GET(routePathStatus, wrapZero(d, GetStatus))
}

func registerCluster(g router, d *Deps) {
	g.GET(routePathClusterPeers, wrapZero(d, ListClusterPeers))
}

func registerResource(g router, d *Deps) {
	g.GET(routePathResourceVlogBreakdown, wrapZero(d, GetVlogBreakdown))
}

func registerDashboard(g router, d *Deps) {
	actor := adminActorMiddleware(d)
	tokenReadAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminDashboardTokenRead",
		resource: adminDashboardTokenResource,
	})
	tokenRotateAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:AdminDashboardTokenRotate",
		resource: adminDashboardTokenRotateResource,
	})
	g.GET(routePathDashboardToken, actor, tokenReadAuthz, wrapZero(d, GetDashboardToken))
	g.POST(routePathDashboardRotate, actor, tokenRotateAuthz, wrapZero(d, RotateDashboardToken))
}

func registerCredentials(g router, d *Deps) {
	actor := credentialActorMiddleware(d)
	g.POST(routePathCredentials, actor, wrapBody[CredentialCreateReq, CredentialResp](d, CreateCredential))
	g.GET(routePathCredentials, actor, listCredentialsHandler(d))
	g.GET(routePathCredential, actor, credentialGetHandler(d))
	g.POST(routePathCredentialRotate, actor, credentialRotateHandler(d))
	g.DELETE(routePathCredential, actor, credentialRevokeHandler(d))
}
