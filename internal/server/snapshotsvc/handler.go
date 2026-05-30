package snapshotsvc

import (
	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/snapshot"
)

// Deps carries what the snapshot REST handlers need from core, as closures + the
// snapshot.Manager backing, so this package never imports `server` (one-way edge).
type Deps struct {
	SnapMgr          *snapshot.Manager
	FeatureAvailable func() bool
	MutationDisabled func(*app.RequestContext, string) bool
	LocalhostOnly    func() app.HandlerFunc
	EmitEvent        func(eventstore.Event)
}

type Handler struct {
	deps Deps
}

func NewHandler(d Deps) *Handler { return &Handler{deps: d} }
