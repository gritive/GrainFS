package serveruntime

import (
	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/iam"
)

// RegisterBucketAdminRoutes wires bucket-scoped admin endpoints under
// /v1/buckets/* on the admin UDS. Handlers live in internal/iam (FSM
// remains in IAM Store per ADR 0010).
//
//	PUT    /v1/buckets/upstream
//	GET    /v1/buckets/upstream
//	GET    /v1/buckets/:bucket/upstream
//	DELETE /v1/buckets/:bucket/upstream
func RegisterBucketAdminRoutes(h *hzserver.Hertz, api *iam.AdminAPI) {
	g := h.Group("/v1/buckets")
	g.PUT("/upstream", wrapStdlibNoParam(api.HandleBucketUpstreamPut))
	g.GET("/upstream", wrapStdlibNoParam(api.HandleBucketUpstreamList))
	g.GET("/:bucket/upstream", wrapStdlibOneParam("bucket", api.HandleBucketUpstreamGet))
	g.DELETE("/:bucket/upstream", wrapStdlibOneParam("bucket", api.HandleBucketUpstreamDelete))
}
