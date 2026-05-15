package server

import (
	"errors"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

var errSnapshotUnavailable = errors.New("backend does not support snapshots")

func (s *Server) snapshotManager() (*snapshot.Manager, error) {
	if !s.routeFeatureAvailable(routeFeatureSnapshot) {
		return nil, errSnapshotUnavailable
	}
	return s.snapMgr, nil
}

func (s *Server) createSnapshot(reason string) (*snapshot.Snapshot, error) {
	mgr, err := s.snapshotManager()
	if err != nil {
		return nil, err
	}
	snap, err := mgr.Create(reason)
	if err != nil {
		return nil, err
	}
	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotCreate})
	return snap, nil
}

func (s *Server) listSnapshots() ([]*snapshot.Snapshot, error) {
	mgr, err := s.snapshotManager()
	if err != nil {
		return nil, err
	}
	return mgr.List()
}

func (s *Server) restoreSnapshot(seq uint64) (int, []storage.StaleBlob, error) {
	mgr, err := s.snapshotManager()
	if err != nil {
		return 0, nil, err
	}
	count, stale, err := mgr.Restore(seq)
	if err != nil {
		return 0, nil, err
	}
	if stale == nil {
		stale = []storage.StaleBlob{}
	}
	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotRestore})
	return count, stale, nil
}

func (s *Server) deleteSnapshot(seq uint64) error {
	mgr, err := s.snapshotManager()
	if err != nil {
		return err
	}
	if err := mgr.Delete(seq); err != nil {
		return err
	}
	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotDelete})
	return nil
}
