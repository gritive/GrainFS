package server

import (
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/volume"
)

func WithDataDir(dir string) Option {
	return func(s *Server) {
		s.dataDir = dir
	}
}

func WithSnapshotEncryptor(enc *encrypt.Encryptor) Option {
	return func(s *Server) {
		s.snapshotEnc = enc
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
