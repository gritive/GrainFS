package admin

import "github.com/gritive/GrainFS/internal/scrubber"

func sessionToInfo(s scrubber.Session) ScrubJobInfo {
	info := ScrubJobInfo{
		SessionID:    s.ID,
		Bucket:       s.Bucket,
		KeyPrefix:    s.KeyPrefix,
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
