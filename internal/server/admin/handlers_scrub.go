package admin

import (
	"context"
	"strings"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/volume"
)

// ScrubVolume triggers a scrub session over the named volume's blocks. Default
// scope is "full" (every block including snapshot-only); pass "live" to limit
// to currently-live blocks. Idempotent — repeating the same request returns
// the existing session id.
func ScrubVolume(ctx context.Context, d *Deps, req ScrubVolumeReq) (ScrubVolumeResp, error) {
	if d.Director == nil {
		return ScrubVolumeResp{}, NewInternal("scrub director not configured")
	}
	if req.Name == "" {
		return ScrubVolumeResp{}, NewInvalid("name required")
	}
	scope := scrubber.ScopeFull
	switch strings.ToLower(req.Scope) {
	case "", "full":
		scope = scrubber.ScopeFull
	case "live":
		scope = scrubber.ScopeLive
	default:
		return ScrubVolumeResp{}, NewInvalid("scope must be 'full' or 'live'")
	}
	id, created := d.Director.Trigger(scrubber.TriggerReq{
		Bucket:    volume.VolumeBucketName,
		KeyPrefix: volume.BlockKeyPrefix(req.Name),
		Scope:     scope,
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
	if d.ScrubProposer == nil {
		return ScrubResp{}, NewInternal("scrub proposer not configured")
	}
	if req.Bucket == "" {
		return ScrubResp{}, NewInvalid("bucket required")
	}
	scope := scrubber.ScopeFull
	switch strings.ToLower(req.Scope) {
	case "", "full":
		scope = scrubber.ScopeFull
	case "live":
		scope = scrubber.ScopeLive
	default:
		return ScrubResp{}, NewInvalid("scope must be 'full' or 'live'")
	}
	entry, err := d.ScrubProposer.Propose(ctx, scrubber.TriggerReq{
		Bucket: req.Bucket, KeyPrefix: req.KeyPrefix, Scope: scope, DryRun: req.DryRun,
	})
	if err != nil {
		return ScrubResp{}, NewInternal("propose scrub: " + err.Error())
	}
	return ScrubResp{SessionID: entry.SessionID, Created: true}, nil
}

// GetScrubJob returns one session by id, aggregated across cluster peers when
// d.ScrubAggregator is wired. Counters are summed; Partial=true and
// PeerFailures populated when any peer RPC failed/timed out.
func GetScrubJob(ctx context.Context, d *Deps, sessionID string) (ScrubJobInfo, error) {
	if d.Director == nil {
		return ScrubJobInfo{}, NewNotFound("scrub director not configured")
	}
	local, hasLocal := d.Director.GetSession(sessionID)

	var peerInfos []ScrubJobInfo
	var peerFailures []string
	if d.ScrubAggregator != nil {
		var err error
		peerInfos, peerFailures, err = d.ScrubAggregator.Peers(ctx, sessionID)
		if err != nil {
			return ScrubJobInfo{}, NewInternal("aggregate peers: " + err.Error())
		}
	}
	if !hasLocal && len(peerInfos) == 0 {
		return ScrubJobInfo{}, NewNotFound("session not found")
	}

	out := sessionToInfo(local)
	out.OwnedHere = hasLocal && local.Stats.Checked > 0
	for _, p := range peerInfos {
		out.Checked += p.Checked
		out.Healthy += p.Healthy
		out.Detected += p.Detected
		out.Repaired += p.Repaired
		out.Unrepairable += p.Unrepairable
		out.Skipped += p.Skipped
		if p.Status == "running" {
			out.Status = "running"
		}
		if p.OwnedHere && (out.Bucket == "" || out.Bucket == "—") {
			out.Bucket = p.Bucket
			out.KeyPrefix = p.KeyPrefix
			out.Scope = p.Scope
			out.DryRun = p.DryRun
		}
	}
	if len(peerFailures) > 0 {
		out.Partial = true
		out.PeerFailures = peerFailures
	}
	return out, nil
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

func sessionToInfo(s scrubber.Session) ScrubJobInfo {
	scope := "full"
	if s.Scope == scrubber.ScopeLive {
		scope = "live"
	}
	info := ScrubJobInfo{
		SessionID:    s.ID,
		Bucket:       s.Bucket,
		KeyPrefix:    s.KeyPrefix,
		Scope:        scope,
		DryRun:       s.DryRun,
		Status:       s.Status,
		StartedAt:    s.StartedAt.Unix(),
		Checked:      s.Stats.Checked,
		Healthy:      s.Stats.Healthy,
		Detected:     s.Stats.Detected,
		Repaired:     s.Stats.Repaired,
		Unrepairable: s.Stats.Unrepairable,
		Skipped:      s.Stats.Skipped,
	}
	if !s.DoneAt.IsZero() {
		info.DoneAt = s.DoneAt.Unix()
	}
	return info
}
