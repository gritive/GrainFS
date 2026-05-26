package serveruntime

import (
	"net/http"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/clusteradmin"
)

// kekStatusReaderAdapter bridges *cluster.MetaFSM into the
// clusteradmin.KEKStatusReader interface. The interface deliberately omits
// `*encrypt.KEKStore` so tests can inject lighter fakes.
type kekStatusReaderAdapter struct{ fsm *cluster.MetaFSM }

func (a kekStatusReaderAdapter) ActiveKEKVersion() uint32 { return a.fsm.ActiveKEKVersion() }
func (a kekStatusReaderAdapter) KEKStoreVersions() []uint32 {
	store := a.fsm.KEKStore()
	if store == nil {
		return nil
	}
	return store.Versions()
}
func (a kekStatusReaderAdapter) LookupKEKStatus(v uint32) (uint32, cluster.KEKLifecycleStatus, uint64, bool) {
	return a.fsm.LookupKEKStatus(v)
}

// RegisterEncryptionKEKRoutes wires the four KEK envelope admin endpoints
// onto the admin-UDS Hertz server. orchestrator and gate may be nil — the
// handlers then return 503 "kek admin disabled" on POST. fsm is required.
//
// UDS-only: this function is only called from boot_phases_admin.go's
// ExtraRoutes closure (the admin UDS Hertz). The public/UI Hertz (RegisterUI)
// never invokes it, so KEK routes are physically absent from any TCP
// listener.
func RegisterEncryptionKEKRoutes(
	h *hzserver.Hertz,
	orchestrator clusteradmin.KEKRotationOrchestrator,
	gate clusteradmin.KEKCapabilityGate,
	fsm *cluster.MetaFSM,
) {
	var reader clusteradmin.KEKStatusReader
	if fsm != nil {
		reader = kekStatusReaderAdapter{fsm: fsm}
	}
	handler := clusteradmin.NewEncryptionKEKHandler(orchestrator, gate, reader)
	h.POST("/v1/encrypt/kek/rotate", wrapStdlibNoParam(handler.ServeRotate))
	h.POST("/v1/encrypt/kek/retire", wrapStdlibNoParam(handler.ServeRetire))
	h.POST("/v1/encrypt/kek/prune", wrapStdlibNoParam(handler.ServePrune))
	h.GET("/v1/encrypt/kek/status", wrapStdlibNoParam(handler.ServeStatus))
}

// kekRoutesProbe is a no-op http.Handler used to ensure the route paths can be
// constructed at compile time. Not registered.
var _ http.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
