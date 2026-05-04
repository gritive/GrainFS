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

// GetScrubJob returns one session by id.
func GetScrubJob(ctx context.Context, d *Deps, sessionID string) (ScrubJobInfo, error) {
	if d.Director == nil {
		return ScrubJobInfo{}, NewNotFound("scrub director not configured")
	}
	s, ok := d.Director.GetSession(sessionID)
	if !ok {
		return ScrubJobInfo{}, NewNotFound("session not found")
	}
	return sessionToInfo(s), nil
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
