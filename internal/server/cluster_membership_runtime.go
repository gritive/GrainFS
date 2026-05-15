package server

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/raft"
)

var errRemovePeerSnapshotUnavailable = errors.New("peer snapshot unavailable")

type removePeerRequest struct {
	ID    string `json:"id"`
	Force bool   `json:"force"`
}

type clusterNotLeaderError struct {
	leaderID string
	retry    bool
}

func (e clusterNotLeaderError) Error() string {
	if e.retry {
		return "leadership changed during transfer"
	}
	return "not leader"
}

type removePeerPreflightError struct {
	result cluster.RemovePeerPreflightResult
	id     string
}

func (e removePeerPreflightError) Error() string {
	return string(e.result.Reason)
}

func (s *Server) removeClusterPeer(ctx context.Context, req removePeerRequest) error {
	if ok, leaderID := clusterNodeIsLeader(s.cluster); !ok {
		return clusterNotLeaderError{leaderID: leaderID}
	}

	result, ok := evaluateRemovePeerPreflight(s.cluster, req.ID)
	if !ok {
		return errRemovePeerSnapshotUnavailable
	}
	if !result.Allowed {
		if result.Reason != cluster.RemovePeerPreflightQuorumWouldBreak || !req.Force {
			return removePeerPreflightError{result: result, id: req.ID}
		}
	}

	if err := s.membership.RemoveVoter(ctx, req.ID); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return clusterNotLeaderError{leaderID: clusterLeaderID(s.cluster)}
		}
		return err
	}

	s.emitEvent(eventstore.Event{
		Type:   eventstore.EventTypeSystem,
		Action: eventstore.EventActionClusterRemovePeer,
		Metadata: map[string]any{
			"removed_id": req.ID,
			"force":      req.Force,
		},
	})
	return nil
}
