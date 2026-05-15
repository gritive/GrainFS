package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

func (s *Server) raftSnapshotStatus() (raft.SnapshotStatus, error) {
	return s.raftSnapshots.RaftSnapshotStatus()
}

func (s *Server) triggerRaftSnapshot(ctx context.Context) (raft.SnapshotResult, error) {
	result, err := s.raftSnapshots.TriggerRaftSnapshot(ctx)
	if err != nil {
		metrics.RaftSnapshotTriggerTotal.WithLabelValues("failed").Inc()
		return result, err
	}
	metrics.RaftSnapshotTriggerTotal.WithLabelValues("success").Inc()
	metrics.RaftSnapshotLastIndex.Set(float64(result.Index))
	metrics.RaftSnapshotLastSizeBytes.Set(float64(result.SizeBytes))
	return result, nil
}
