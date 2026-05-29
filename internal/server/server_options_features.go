package server

import (
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/volume"
)

func WithDataDir(dir string) Option {
	return func(s *Server) {
		s.dataDir = dir
	}
}

func WithSnapshotKEK(kek snapshot.KEKSource, clusterID [16]byte) Option {
	return func(s *Server) {
		s.snapshotKEK = kek
		s.snapshotClusterID = clusterID
	}
}

// WithSnapshotManager injects a pre-built snapshot Manager so the server shares
// the exact instance the serveruntime auto-snapshotter uses, rather than building
// its own. Unifies nextSeq allocation + method serialization across the
// auto-snapshotter and the HTTP create/restore/delete handlers. When set,
// initSnapshotManager skips self-construction.
func WithSnapshotManager(mgr *snapshot.Manager) Option {
	return func(s *Server) {
		s.snapMgr = mgr
	}
}

func WithScrubber(sc *scrubber.BackgroundScrubber) Option {
	return func(s *Server) {
		s.scrubber = sc
	}
}

func WithLifecycleService(svc *lifecycle.Service) Option {
	return func(s *Server) {
		s.lifecycle = svc
	}
}

func WithEventStore(store *eventstore.Store) Option {
	return func(s *Server) {
		s.evStore = store
	}
}

func WithReceiptAPI(api *receipt.API) Option {
	return func(s *Server) {
		s.receiptAPI = api
	}
}

func WithIncidentStore(store incident.StateStore) Option {
	return func(s *Server) {
		s.incidentStore = store
	}
}

func WithVolumeManager(mgr *volume.Manager) Option {
	return func(s *Server) {
		s.volMgr = mgr
	}
}

func WithIcebergCatalog(catalog icebergcatalog.Catalog) Option {
	return func(s *Server) {
		s.icebergCatalog = catalog
	}
}

func WithIcebergCatalogStore(store *icebergcatalog.Store) Option {
	return WithIcebergCatalog(store)
}
