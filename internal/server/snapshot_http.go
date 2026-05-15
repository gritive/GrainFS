package server

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/snapshot"
)

func snapshotSeqParam(c *app.RequestContext) (uint64, bool) {
	seq, err := strconv.ParseUint(c.Param("seq"), 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, apiError("invalid seq", "seq must be a positive integer"))
		return 0, false
	}
	return seq, true
}

func writeSnapshotCommandError(c *app.RequestContext, op string, seq uint64, err error) {
	if errors.Is(err, errSnapshotUnavailable) {
		c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
		return
	}
	if errors.Is(err, snapshot.ErrNotFound) {
		c.JSON(consts.StatusNotFound, apiError(
			fmt.Sprintf("snapshot %d not found", seq),
			"list available snapshots with GET /admin/snapshots",
		))
		return
	}
	if errors.Is(err, snapshot.ErrUnsupportedSnapshotFormat) {
		c.JSON(consts.StatusConflict, apiError("unsupported snapshot format", err.Error()))
		return
	}
	c.JSON(consts.StatusInternalServerError, apiError(op+" failed", err.Error()))
}
