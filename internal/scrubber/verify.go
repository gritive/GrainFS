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
	Corrupt []int // shard indices with bad CRC or read errors (bit-rot)
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
	return v.VerifyIndices(rec, nil)
}

// VerifyIndices is like Verify but restricts the check to the given shard
// indices. Passing nil verifies every shard (the legacy full-object path).
// Used by cluster mode so each node only verifies its owned shards. Slice 3
// of refactor/unify-storage-paths.
func (v *ShardVerifier) VerifyIndices(rec ObjectRecord, indices []int) ShardStatus {
	status := v.check(rec, indices)
	if len(status.Corrupt) == 0 {
		return status
	}
	// Transient retry: wait and re-check
	time.Sleep(v.retryDelay)
	return v.check(rec, indices)
}

func (v *ShardVerifier) check(rec ObjectRecord, indices []int) ShardStatus {
	total := rec.DataShards + rec.ParityShards
	paths := v.backend.ShardPaths(rec.Bucket, rec.Key, rec.VersionID, total)
	status := ShardStatus{Bucket: rec.Bucket, Key: rec.Key}
	check := func(i int) {
		if i < 0 || i >= len(paths) {
			return
		}
		_, err := v.backend.ReadShard(rec.Bucket, rec.Key, paths[i])
		if err == nil {
			return
		}
		switch {
		case errors.Is(err, os.ErrNotExist):
			status.Missing = append(status.Missing, i)
		default:
			status.Corrupt = append(status.Corrupt, i)
		}
	}
	if indices == nil {
		for i := range paths {
			check(i)
		}
		return status
	}
	for _, i := range indices {
		check(i)
	}
	return status
}
