package migration

import "time"

// JobStatus represents the lifecycle state of a migration job.
type JobStatus string

const (
	StatusIdle     JobStatus = "idle" // reserved for future pre-submission state
	StatusRunning  JobStatus = "running"
	StatusComplete JobStatus = "complete"
	StatusFailed   JobStatus = "failed"
)

// JobState is the persistent record for a per-bucket migration job.
type JobState struct {
	Bucket    string    `json:"bucket"`
	Status    JobStatus `json:"status"`
	Copied    int64     `json:"copied"`
	Errors    int64     `json:"errors"`
	Reason    string    `json:"reason,omitempty"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
