package scrubber

import (
	"errors"
	"os"
	"time"
)

// ShardStatus describes the health of an EC object's shards.
type ShardStatus struct {
	Bucket  string
	Key     string
	Missing []int // shard indices that are absent
	Corrupt []int // shard indices with bad CRC or read errors
}

// IsHealthy reports whether all shards are present and valid.
func (s ShardStatus) IsHealthy() bool {
	return len(s.Missing)+len(s.Corrupt) == 0
}

// ShardVerifier checks shard existence and CRC32 integrity.
type ShardVerifier struct {
	backend    Scrubbable
	retryDelay time.Duration // transient-error retry delay (Eng Review #12)
}

// VerifierOption configures ShardVerifier.
type VerifierOption func(*ShardVerifier)

// WithVerifyRetryDelay overrides the transient-error retry delay (default 100ms).
// Use 0 in tests to avoid sleeping.
func WithVerifyRetryDelay(d time.Duration) VerifierOption {
	return func(v *ShardVerifier) { v.retryDelay = d }
}

// NewShardVerifier creates a ShardVerifier with a 100ms transient retry delay.
func NewShardVerifier(b Scrubbable, opts ...VerifierOption) *ShardVerifier {
	v := &ShardVerifier{backend: b, retryDelay: 100 * time.Millisecond}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// Verify checks all shards for rec. On transient corruption, retries once
// after retryDelay before reporting corrupt (Eng Review #12).
func (v *ShardVerifier) Verify(rec ObjectRecord) ShardStatus {
	status := v.check(rec)
	if len(status.Corrupt) == 0 {
		return status
	}
	// Transient retry: wait and re-check
	time.Sleep(v.retryDelay)
	return v.check(rec)
}

func (v *ShardVerifier) check(rec ObjectRecord) ShardStatus {
	total := rec.DataShards + rec.ParityShards
	paths := v.backend.ShardPaths(rec.Bucket, rec.Key, total)
	status := ShardStatus{Bucket: rec.Bucket, Key: rec.Key}
	for i, path := range paths {
		_, err := v.backend.ReadShard(rec.Bucket, rec.Key, path)
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			status.Missing = append(status.Missing, i)
		} else {
			status.Corrupt = append(status.Corrupt, i)
		}
	}
	return status
}
