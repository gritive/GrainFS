package server

import (
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

func WithDataDir(dir string) Option {
	return func(s *Server) {
		s.dataDir = dir
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
