package admin

import (
	"context"
	"strings"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/execution"
	"github.com/gritive/GrainFS/internal/volume"
)

// ScrubVolume triggers a scrub session over the named volume's blocks.
// Idempotent — repeating the same request returns the existing session id.
func ScrubVolume(ctx context.Context, d *Deps, req ScrubVolumeReq) (ScrubVolumeResp, error) {
	if d.Director == nil {
		return ScrubVolumeResp{}, NewInternal("scrub director not configured")
	}
	if req.Name == "" {
		return ScrubVolumeResp{}, NewInvalid("name required")
	}
	id, created := d.Director.Trigger(scrubber.TriggerReq{
		Bucket:    volume.VolumeBucketName,
		KeyPrefix: volume.BlockKeyPrefix(req.Name),
		DryRun:    req.DryRun,
	})
	if id == "" {
		return ScrubVolumeResp{}, NewInternal("scrub queue full")
	}
	return ScrubVolumeResp{SessionID: id, Created: created}, nil
}

// ListScrubJobs returns every scrub session the Director currently tracks.
func ListScrubJobs(ctx context.Context, d *Deps) (ListScrubJobsResp, error) {
	if d.Director == nil {
		return ListScrubJobsResp{}, nil
	}
	out := ListScrubJobsResp{}
	for _, s := range d.Director.Sessions() {
		out.Jobs = append(out.Jobs, sessionToInfo(s))
	}
	return out, nil
}

// TriggerScrub publishes a cluster-wide scrub trigger via raft. Each node's
// onScrubTrigger callback creates a session for the same SessionID; the
// resolver then walks the bucket's group's BadgerDB if locally owned. The
// operator polls GET /v1/scrub/jobs/<id> for aggregated stats.
func TriggerScrub(ctx context.Context, d *Deps, req ScrubReq) (ScrubResp, error) {
	if req.Bucket == "" {
		return ScrubResp{}, NewInvalid("bucket required")
	}
	if d.Execution != nil {
		// Scrub keeps its existing session_id contract; Operation.ID stays zero here.
		plan, err := (execution.Planner{ClusterAvailable: true}).Plan(execution.Operation{
			Kind: execution.OperationScrub,
			Scrub: execution.ScrubOperation{
				Bucket:    req.Bucket,
				KeyPrefix: req.KeyPrefix,
				DryRun:    req.DryRun,
			},
		})
		if err != nil {
			return ScrubResp{}, executionErrorToAdmin(err)
		}
		result, err := d.Execution.Execute(ctx, plan)
		if err != nil {
			return ScrubResp{}, executionErrorToAdmin(err)
		}
		return ScrubResp{SessionID: result.Scrub.SessionID, Created: result.Scrub.Created}, nil
	}
	if d.ScrubProposer == nil {
		return ScrubResp{}, NewInternal("scrub proposer not configured")
	}
	entry, created, err := d.ScrubProposer.Propose(ctx, scrubber.TriggerReq{
		Bucket: req.Bucket, KeyPrefix: req.KeyPrefix, DryRun: req.DryRun,
	})
	if err != nil {
		return ScrubResp{}, NewInternal("propose scrub: " + err.Error())
	}
	return ScrubResp{SessionID: entry.SessionID, Created: created}, nil
}

// GetScrubJob returns one session by id, aggregated across cluster peers when
// d.ScrubAggregator is wired. Counters are summed; Partial=true and
// PeerFailures populated when any peer RPC failed/timed out.
func GetScrubJob(ctx context.Context, d *Deps, sessionID string) (ScrubJobInfo, error) {
	if d.Director == nil {
		return ScrubJobInfo{}, NewNotFound("scrub director not configured")
	}
	local, hasLocal := d.Director.GetSession(sessionID)

	peerInfos, peerFailures, err := aggregateScrubPeers(ctx, d, sessionID)
	if err != nil {
		return ScrubJobInfo{}, err
	}
	if !hasLocal && len(peerInfos) == 0 {
		if len(peerFailures) > 0 {
			return ScrubJobInfo{}, NewInternal("all peers unreachable: " + strings.Join(peerFailures, ","))
		}
		return ScrubJobInfo{}, NewNotFound("session not found")
	}

	out := sessionToInfo(local)
	out.OwnedHere = hasLocal && local.Stats.Checked > 0
	return mergeScrubPeerInfo(out, peerInfos, peerFailures), nil
}

// CancelScrubJob marks a running session as cancelled. Best-effort: in-flight
// block verifications may still complete before the worker observes the flag.
func CancelScrubJob(ctx context.Context, d *Deps, sessionID string) error {
	if d.Director == nil {
		return NewNotFound("scrub director not configured")
	}
	if err := d.Director.CancelSession(sessionID); err != nil {
		return NewNotFound(err.Error())
	}
	return nil
}
