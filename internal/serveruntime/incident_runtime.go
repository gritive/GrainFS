package serveruntime

import (
	"context"
	"fmt"
	"sync"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
)

// IncidentRecorderSink receives the incident recorder used by lower runtime
// layers. It is optional; nil means only server/admin incident readers are
// wired.
type IncidentRecorderSink interface {
	SetIncidentRecorder(cluster.IncidentRecorder)
}

// IncidentRuntimeOptions groups the incident role, incident recorder, and
// resource guard wiring that used to live directly in serve.go.
type IncidentRuntimeOptions struct {
	RoleRegistry badgerrole.Registry
	DataDir      string
	NodeID       string

	IncidentRecorderSink IncidentRecorderSink
	Alerts               resourceguard.AlertsSender

	FDWatchEnabled        bool
	FDOpts                resourceguard.FDOptions
	GoroutineWatchEnabled bool
	GoroutineOpts         resourceguard.GoroutineOptions
	VlogWatchEnabled      bool
	VlogOpts              resourceguard.VlogOptions
}

// IncidentRuntime is the assembled incident runtime plus the server options it
// contributes. Close is idempotent and owns the DB/resourcewatch cleanup.
type IncidentRuntime struct {
	Recorder         *incident.Recorder
	ClusterRecorder  cluster.IncidentRecorder
	ScrubberRecorder scrubber.IncidentRecorder
	ServerOptions    []server.Option
	closeOnce        sync.Once
	closeFn          func() error
	closeErr         error
}

func (r *IncidentRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			r.closeErr = r.closeFn()
		}
	})
	return r.closeErr
}

// SetupIncidentRuntime opens the optional incident Badger role, wires the
// recorder into runtime consumers, and starts resource guards that depend on
// incident recording. Optional role disablement returns an empty runtime.
func SetupIncidentRuntime(ctx context.Context, opts IncidentRuntimeOptions) (*IncidentRuntime, error) {
	runtime := &IncidentRuntime{}
	db, decision, err := badgerrole.OpenRole(opts.RoleRegistry, badgerrole.RoleIncidentState, badgerrole.PathContext{DataDir: opts.DataDir})
	if err != nil {
		if feature, ok := OptionalRoleDisabled(opts.RoleRegistry, decision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleIncidentState, feature, err)
			return runtime, nil
		}
		return runtime, fmt.Errorf("incident runtime: open incident db: %w", err)
	}

	registered := resourcewatch.RegisterDB(resourcewatch.DBCategoryIncident, db)
	store := badgerstore.New(db)
	recorder := incident.NewRecorder(store, incident.NewReducer())
	clusterRecorder, scrubberRecorder := IncidentRecorderInterfaces(recorder)

	runtime.Recorder = recorder
	runtime.ClusterRecorder = clusterRecorder
	runtime.ScrubberRecorder = scrubberRecorder
	runtime.ServerOptions = []server.Option{server.WithIncidentStore(store)}
	runtime.closeFn = func() error {
		resourcewatch.DeregisterDB(registered)
		return db.Close()
	}

	if opts.IncidentRecorderSink != nil {
		opts.IncidentRecorderSink.SetIncidentRecorder(clusterRecorder)
	}

	guardDeps := resourceguard.Deps{
		NodeID:   opts.NodeID,
		Alerts:   opts.Alerts,
		Recorder: recorder,
	}
	if opts.FDWatchEnabled {
		resourceguard.StartFD(ctx, opts.FDOpts, guardDeps)
	}
	if opts.GoroutineWatchEnabled {
		resourceguard.StartGoroutine(ctx, opts.GoroutineOpts, guardDeps)
	}
	if opts.VlogWatchEnabled {
		resourceguard.StartVlog(ctx, opts.VlogOpts, guardDeps)
	}

	return runtime, nil
}
