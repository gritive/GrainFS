package snapshotsvc

import (
	"errors"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

var errSnapshotUnavailable = errors.New("backend does not support snapshots")

func (h *Handler) snapshotManager() (*snapshot.Manager, error) {
	if !h.deps.FeatureAvailable() {
		return nil, errSnapshotUnavailable
	}
	return h.deps.SnapMgr, nil
}

func (h *Handler) createSnapshot(reason string) (*snapshot.Snapshot, error) {
	mgr, err := h.snapshotManager()
	if err != nil {
		return nil, err
	}
	snap, err := mgr.Create(reason)
	if err != nil {
		return nil, err
	}
	h.deps.EmitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotCreate})
	return snap, nil
}

func (h *Handler) listSnapshots() ([]*snapshot.Snapshot, error) {
	mgr, err := h.snapshotManager()
	if err != nil {
		return nil, err
	}
	return mgr.List()
}

func (h *Handler) restoreSnapshot(seq uint64) (int, []storage.StaleBlob, error) {
	mgr, err := h.snapshotManager()
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
	h.deps.EmitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotRestore})
	return count, stale, nil
}

func (h *Handler) deleteSnapshot(seq uint64) error {
	mgr, err := h.snapshotManager()
	if err != nil {
		return err
	}
	if err := mgr.Delete(seq); err != nil {
		return err
	}
	h.deps.EmitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotDelete})
	return nil
}
