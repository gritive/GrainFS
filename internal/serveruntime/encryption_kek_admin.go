package serveruntime

import (
	"net/http"
	"sort"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/clusteradmin"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// kekStatusReaderAdapter bridges *cluster.MetaFSM (lifecycle status + version
// list) plus the DEK keeper (seal counts) and lease tracker (lease counts)
// into the clusteradmin.KEKStatusReader interface. The interface deliberately
// omits `*encrypt.KEKStore` so tests can inject lighter fakes. keeper and
// tracker may be nil (encryption disabled / Phase A); the adapter then reports
// 0 for the corresponding counters.
type kekStatusReaderAdapter struct {
	fsm     *cluster.MetaFSM
	keeper  *encrypt.DEKKeeper
	tracker *encrypt.KEKLeaseTracker
}

func (a kekStatusReaderAdapter) ActiveKEKVersion() uint32 { return a.fsm.ActiveKEKVersion() }

// KEKStoreVersions returns the sorted union of the live keystore versions and
// the lifecycle-tracked versions. A pruned version is unlinked from the
// keystore but its kek_status record persists, so unioning keeps it visible in
// the status response (reported as "pruned" via lifecycleStatusString).
func (a kekStatusReaderAdapter) KEKStoreVersions() []uint32 {
	seen := make(map[uint32]struct{})
	if store := a.fsm.KEKStore(); store != nil {
		for _, v := range store.Versions() {
			seen[v] = struct{}{}
		}
	}
	for _, v := range a.fsm.LifecycleKEKVersions() {
		seen[v] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
func (a kekStatusReaderAdapter) LookupKEKStatus(v uint32) (uint32, cluster.KEKLifecycleStatus, uint64, bool) {
	return a.fsm.LookupKEKStatus(v)
}
func (a kekStatusReaderAdapter) SealCount(v uint32) uint64 {
	if a.keeper == nil {
		return 0
	}
	return a.keeper.SealCount(v)
}
func (a kekStatusReaderAdapter) LeaseCount(v uint32) uint64 {
	if a.tracker == nil {
		return 0
	}
	return a.tracker.Count(v)
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
	keeper *encrypt.DEKKeeper,
	tracker *encrypt.KEKLeaseTracker,
) {
	var reader clusteradmin.KEKStatusReader
	if fsm != nil {
		reader = kekStatusReaderAdapter{fsm: fsm, keeper: keeper, tracker: tracker}
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
