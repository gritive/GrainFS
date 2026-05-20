package admin

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

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
	case "job_cancelled":
		return consts.StatusConflict
	case "invalid":
		return consts.StatusBadRequest
	case "unsupported":
		return consts.StatusUnprocessableEntity
	case "unauthorized":
		return consts.StatusUnauthorized
	case "forbidden":
		return consts.StatusForbidden
	case "retry", "unavailable":
		return consts.StatusServiceUnavailable
	case "export_propagation_timeout":
		return consts.StatusGatewayTimeout
	case "job_timeout":
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
