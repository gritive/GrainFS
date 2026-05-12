package serveruntime

import (
	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

// RegisterClusterConfigRoutes wires GET /v1/cluster/config (and the PATCH stub
// — 501 until Task 10) onto the admin UDS Hertz server. The handler is a plain
// net/http handler bridged via wrapStdlibNoParam.
//
// proposer may be nil during Task 9; Task 10 will pass a real raft proposer
// once the propose path lands.
func RegisterClusterConfigRoutes(h *hzserver.Hertz, fsm *cluster.MetaFSM, proposer adminapi.ClusterConfigProposer) {
	handler := adminapi.NewClusterConfigHandler(fsm, proposer)
	h.GET("/v1/cluster/config", wrapStdlibNoParam(handler.ServeHTTP))
	h.PATCH("/v1/cluster/config", wrapStdlibNoParam(handler.ServeHTTP))
}
