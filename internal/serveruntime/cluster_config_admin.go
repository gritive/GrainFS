package serveruntime

import (
	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/clusteradmin"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// RegisterClusterConfigRoutes wires GET/PATCH /v1/cluster/config onto the
// admin UDS Hertz server. The handler is a plain net/http handler bridged via
// wrapStdlibNoParam.
//
// proposer may be nil when raft has not yet been initialized — PATCH then
// returns 503. enc may be nil in --no-encryption mode; PATCH with a webhook
// secret is then rejected with 403 early (before propose).
func RegisterClusterConfigRoutes(h *hzserver.Hertz, fsm *cluster.MetaFSM, proposer clusteradmin.ClusterConfigProposer, enc *encrypt.Encryptor) {
	handler := clusteradmin.NewClusterConfigHandler(fsm, proposer, enc)
	h.GET("/v1/cluster/config", wrapStdlibNoParam(handler.ServeHTTP))
	h.PATCH("/v1/cluster/config", wrapStdlibNoParam(handler.ServeHTTP))
}
