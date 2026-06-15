package admin

import (
	"context"
	"strings"

	"github.com/gritive/GrainFS/internal/scrubber"
)

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
