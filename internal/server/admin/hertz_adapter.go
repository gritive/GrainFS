package admin

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
)

// router is the subset of route.IRoutes we use; satisfied by both *server.Hertz
// and *route.RouterGroup so callers can register at any path prefix.
type router interface {
	GET(path string, handlers ...app.HandlerFunc) route.IRoutes
	POST(path string, handlers ...app.HandlerFunc) route.IRoutes
	DELETE(path string, handlers ...app.HandlerFunc) route.IRoutes
	PUT(path string, handlers ...app.HandlerFunc) route.IRoutes
}

// RegisterAdmin wires the admin handlers under the `/v1/...` prefix on the
// given Hertz instance. This is what the Unix-socket admin server calls.
func RegisterAdmin(h *server.Hertz, d *Deps) {
	g := h.Group("/v1")
	registerVolume(g, d)
	registerSnapshot(g, d)
	registerDashboard(g, d)
}

// RegisterUI wires a subset of admin handlers under `/ui/api/...` on the
// data-plane Hertz instance. Token auth is the caller's responsibility (install
// the middleware before calling this so unauthorized requests never reach
// handler logic).
func RegisterUI(h *server.Hertz, d *Deps) {
	g := h.Group("/ui/api")
	registerVolume(g, d)
	registerSnapshot(g, d)
	// Dashboard token endpoints are intentionally NOT mounted on /ui/api —
	// they live only on the local admin Unix socket.
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
}

func registerSnapshot(g router, d *Deps) {
	g.POST("/volumes/:name/snapshots", wrapName(d, CreateSnapshot))
	g.GET("/volumes/:name/snapshots", wrapName(d, ListSnapshots))
	g.DELETE("/volumes/:name/snapshots/:snap", deleteSnapshotHandler(d))
	g.POST("/volumes/:name/snapshots/:snap/rollback", rollbackHandler(d))
}

func registerDashboard(g router, d *Deps) {
	g.GET("/dashboard/token", wrapZero(d, GetDashboardToken))
	g.POST("/dashboard/token/rotate", wrapZero(d, RotateDashboardToken))
}

// statusForCode maps domain error codes to HTTP status codes.
func statusForCode(c string) int {
	switch c {
	case "not_found":
		return consts.StatusNotFound
	case "conflict":
		return consts.StatusConflict
	case "invalid":
		return consts.StatusBadRequest
	case "unsupported":
		return consts.StatusUnprocessableEntity
	case "unauthorized":
		return consts.StatusUnauthorized
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
