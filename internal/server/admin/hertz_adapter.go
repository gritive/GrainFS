package admin

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/gritive/GrainFS/internal/iam"
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
	g := h.Group("/v1")
	registerSnapshot(g, d)
	registerVolume(g, d)
	registerScrub(g, d)
	registerCluster(g, d)
	registerResource(g, d)
	registerDashboard(g, d)
	registerIAM(g, d)
	registerBucket(g, d)
	registerNfsExports(g, d)
}

// RegisterUI wires a subset of admin handlers under `/ui/api/...` on the
// data-plane Hertz instance. Token auth is the caller's responsibility (install
// the middleware before calling this so unauthorized requests never reach
// handler logic).
func RegisterUI(h *server.Hertz, d *Deps) {
	g := h.Group("/ui/api")
	registerSnapshot(g, d)
	registerVolume(g, d)
	registerScrub(g, d)
	registerCluster(g, d)
	registerResource(g, d)
	// Dashboard token endpoints are intentionally NOT mounted on /ui/api —
	// they live only on the local admin Unix socket.
	registerIAM(g, d)
	// registerBucket is intentionally NOT mounted on /ui/api: AdminGetBucket
	// performs an unbounded CountObjects walk (full Badger scan) that any
	// dashboard-token holder could trigger remotely, causing write starvation.
	// Bucket admin ops are admin-UDS only.
}

// RegisterIAMOnly wires only the IAM admin routes. Used in tests to avoid
// registering all routes which would panic with a nil Manager.
func RegisterIAMOnly(h *server.Hertz, d *Deps) {
	g := h.Group("/v1")
	registerIAM(g, d)
}

func registerCluster(g router, d *Deps) {
	g.GET("/cluster/peers", wrapZero(d, ListClusterPeers))
}

func registerResource(g router, d *Deps) {
	g.GET("/resource/vlog/breakdown", wrapZero(d, GetVlogBreakdown))
}

func registerVolume(g router, d *Deps) {
	g.GET("/volumes", wrapZero(d, ListVolumes))
	g.POST("/volumes", wrapBody[CreateVolumeReq, VolumeInfo](d, CreateVolume))
	g.GET("/volumes/:name", wrapName(d, GetVolume))
	g.DELETE("/volumes/:name", deleteVolumeHandler(d))
	g.GET("/volumes/:name/stat", wrapName(d, StatVolume))
	g.POST("/volumes/:name/resize", wrapNameBody[ResizeReq, ResizeResp](d, ResizeVolume))
	g.POST("/volumes/:name/recalculate", wrapName(d, RecalculateVolume))
	g.POST("/volumes/clone", wrapBodyNoOut[CloneReq](d, CloneVolume))
	g.POST("/volumes/:name/write-at", wrapBody[WriteAtVolumeReq, WriteAtVolumeResp](d, WriteAtVolume))
	g.POST("/volumes/:name/read-at", wrapBody[ReadAtVolumeReq, ReadAtVolumeResp](d, ReadAtVolume))
}

func registerSnapshot(g router, d *Deps) {
	g.POST("/volumes/:name/snapshots", wrapName(d, CreateSnapshot))
	g.GET("/volumes/:name/snapshots", wrapName(d, ListSnapshots))
	g.DELETE("/volumes/:name/snapshots/:snap", deleteSnapshotHandler(d))
	g.POST("/volumes/:name/snapshots/:snap/rollback", rollbackHandler(d))
}

func registerScrub(g router, d *Deps) {
	g.POST("/volumes/:name/scrub", scrubVolumeHandler(d))
	g.POST("/scrub", wrapBody[ScrubReq, ScrubResp](d, TriggerScrub))
	g.GET("/scrub/jobs", wrapZero(d, ListScrubJobs))
	g.GET("/scrub/jobs/:id", scrubJobByIDHandler(d, GetScrubJob))
	g.DELETE("/scrub/jobs/:id", scrubJobCancelHandler(d))
}

func scrubVolumeHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req ScrubVolumeReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		req.Name = c.Param("name")
		resp, err := ScrubVolume(ctx, d, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, resp)
	}
}

func scrubJobByIDHandler(d *Deps, fn func(context.Context, *Deps, string) (ScrubJobInfo, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := c.Param("id")
		resp, err := fn(ctx, d, id)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func scrubJobCancelHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := c.Param("id")
		if err := CancelScrubJob(ctx, d, id); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func registerDashboard(g router, d *Deps) {
	g.GET("/dashboard/token", wrapZero(d, GetDashboardToken))
	g.POST("/dashboard/token/rotate", wrapZero(d, RotateDashboardToken))
}

func registerBucket(g router, d *Deps) {
	if d.Buckets == nil {
		return
	}
	g.POST("/buckets", wrapBody[CreateBucketAdminReq, BucketInfo](d, AdminCreateBucket))
	g.GET("/buckets", wrapZero(d, AdminListBuckets))
	g.GET("/buckets/:name", wrapName(d, AdminGetBucket))
	g.DELETE("/buckets/:name", bucketDeleteHandler(d))
	g.GET("/buckets/:name/policy", wrapName(d, AdminGetBucketPolicy))
	g.PUT("/buckets/:name/policy", bucketSetPolicyHandler(d))
	g.DELETE("/buckets/:name/policy", bucketDeletePolicyHandler(d))
	g.GET("/buckets/:name/versioning", wrapName(d, AdminGetBucketVersioning))
	g.PUT("/buckets/:name/versioning", bucketSetVersioningHandler(d))
}

func registerNfsExports(g router, d *Deps) {
	if d.NfsExports == nil {
		return
	}
	g.POST("/nfs/exports", wrapBody[NfsExportUpsertReq, NfsExportInfo](d, AdminNfsExportUpsert))
	g.GET("/nfs/exports", wrapZero(d, AdminNfsExportList))
	g.GET("/nfs/exports/:name/debug", wrapName(d, AdminNfsExportDebug))
	g.GET("/nfs/exports/:name", wrapName(d, AdminNfsExportGet))
	g.DELETE("/nfs/exports/:name", nfsExportDeleteHandler(d))
	g.PATCH("/nfs/exports/:name", nfsExportPatchHandler(d))
}

func nfsExportDeleteHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		if err := AdminNfsExportDelete(ctx, d, name); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func nfsExportPatchHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req NfsExportUpsertReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		resp, err := AdminNfsExportUpdate(ctx, d, name, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func bucketSetPolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req BucketPolicySetReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if req.Policy == nil || bytes.Equal(req.Policy, []byte("null")) {
			writeError(c, NewInvalid("policy field is required"))
			return
		}
		if err := AdminSetBucketPolicy(ctx, d, name, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketDeletePolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		if err := AdminDeleteBucketPolicy(ctx, d, name); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketSetVersioningHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req BucketVersioningSetReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := AdminSetBucketVersioning(ctx, d, name, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketDeleteHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		force := string(c.Query("force")) == "true"
		if err := AdminDeleteBucket(ctx, d, name, force); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func registerIAM(g router, d *Deps) {
	if d.IAM == nil {
		return
	}
	// SA
	g.POST("/iam/sa", wrapBody[iam.SACreateRequest, iam.SACreateResponse](d, CreateSA))
	g.GET("/iam/sa", wrapZero(d, ListSA))
	g.GET("/iam/sa/:id", iamGetSAHandler(d))
	g.DELETE("/iam/sa/:id", iamDeleteSAHandler(d))
	// Key
	g.POST("/iam/sa/:id/key", iamCreateKeyHandler(d))
	g.DELETE("/iam/sa/:id/key/:ak", iamRevokeKeyHandler(d))
	// Grant (PUT upsert → 204)
	g.PUT("/iam/grant", wrapBodyNoOut204[iam.GrantPutRequest](d, PutGrant))
	g.DELETE("/iam/grant", iamDeleteGrantHandler(d))
	g.GET("/iam/grant", iamListGrantsHandler(d))
	// Bucket upstream (PUT upsert → 204). Routes under /upstreams (not
	// /buckets/upstream) to avoid Hertz static-beats-param collision with
	// GET /buckets/:name used by AdminGetBucket.
	g.PUT("/upstreams", wrapBodyNoOut204[iam.BucketUpstreamPutRequest](d, PutBucketUpstream))
	g.GET("/upstreams", wrapZero(d, ListBucketUpstreams))
	g.GET("/buckets/:bucket/upstream", iamGetBucketUpstreamHandler(d))
	g.DELETE("/buckets/:bucket/upstream", iamDeleteBucketUpstreamHandler(d))
}

func iamGetSAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := GetSA(ctx, d, c.Param("id"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func iamGetBucketUpstreamHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := GetBucketUpstream(ctx, d, c.Param("bucket"))
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func iamDeleteSAHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := DeleteSA(ctx, d, c.Param("id")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamCreateKeyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		saID := c.Param("id")
		var req iam.KeyCreateRequest
		if body := c.Request.Body(); len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		resp, err := CreateKey(ctx, d, saID, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, resp)
	}
}

func iamRevokeKeyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := RevokeKey(ctx, d, c.Param("id"), c.Param("ak")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamDeleteGrantHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req iam.GrantDeleteRequest
		if body := c.Request.Body(); len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := DeleteGrant(ctx, d, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func iamListGrantsHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		sa := string(c.Query("sa"))
		bucket := string(c.Query("bucket"))
		resp, err := ListGrants(ctx, d, sa, bucket)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func iamDeleteBucketUpstreamHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if err := DeleteBucketUpstream(ctx, d, c.Param("bucket")); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

// statusForCode maps domain error codes to HTTP status codes.
func statusForCode(c string) int {
	switch c {
	case "not_found":
		return consts.StatusNotFound
	case "bucket_not_found", "export_not_found":
		return consts.StatusNotFound
	case "conflict":
		return consts.StatusConflict
	case "export_already_exists":
		return consts.StatusConflict
	case "invalid":
		return consts.StatusBadRequest
	case "unsupported":
		return consts.StatusUnprocessableEntity
	case "unauthorized":
		return consts.StatusUnauthorized
	case "forbidden":
		return consts.StatusForbidden
	case "retry":
		return consts.StatusServiceUnavailable
	case "export_propagation_timeout":
		return consts.StatusGatewayTimeout
	default:
		return consts.StatusInternalServerError
	}
}

func writeError(c *app.RequestContext, err error) {
	if ae, ok := err.(*Error); ok {
		c.JSON(statusForCode(ae.Code), ae)
		return
	}
	c.JSON(consts.StatusInternalServerError, &Error{Code: "internal", Message: err.Error()})
}

func writeOK(c *app.RequestContext, status int, body any) {
	if body == nil {
		c.SetStatusCode(status)
		return
	}
	c.JSON(status, body)
}

// --- Generic wrapper helpers ---

func wrapZero[Resp any](d *Deps, fn func(context.Context, *Deps) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := fn(ctx, d)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func wrapName[Resp any](d *Deps, fn func(context.Context, *Deps, string) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		resp, err := fn(ctx, d, name)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func wrapBody[Req any, Resp any](d *Deps, fn func(context.Context, *Deps, Req) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		resp, err := fn(ctx, d, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusCreated, resp)
	}
}

func wrapBodyNoOut[Req any](d *Deps, fn func(context.Context, *Deps, Req) error) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := fn(ctx, d, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusCreated)
	}
}

// wrapBodyNoOut204 is like wrapBodyNoOut but returns 204 No Content on success.
// Use for idempotent upsert routes (PUT) where creating vs. updating is not
// distinguishable at the transport layer.
func wrapBodyNoOut204[Req any](d *Deps, fn func(context.Context, *Deps, Req) error) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := fn(ctx, d, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func wrapNameBody[Req any, Resp any](d *Deps, fn func(context.Context, *Deps, string, Req) (Resp, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req Req
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		resp, err := fn(ctx, d, name, req)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func deleteVolumeHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		force := string(c.Query("force")) == "true"
		resp, err := DeleteVolume(ctx, d, name, force)
		if err != nil {
			writeError(c, err)
			return
		}
		writeOK(c, consts.StatusOK, resp)
	}
}

func deleteSnapshotHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		snap := c.Param("snap")
		if err := DeleteSnapshot(ctx, d, name, snap); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func rollbackHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		snap := c.Param("snap")
		if err := RollbackVolume(ctx, d, name, snap); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
