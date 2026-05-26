package scrubber

import "context"

// Block is the unit of work flowing through the scrub loop.
type Block struct {
	Bucket       string
	Key          string
	VersionID    string // EC: live versionID. Volume: "current".
	ExpectedETag string
	ExpectedSize int64
}

// BlockStatus is the result of Verify on a single Block.
type BlockStatus struct {
	Healthy bool
	Missing bool
	Corrupt bool
	// Skipped marks blocks the verifier cannot evaluate (e.g. legacy data
	// without an ETag oracle). Skipped is neither healthy nor a finding —
	// the Director leaves them out of detect/repair counts.
	Skipped bool
	Detail  string
}

// IsHealthy returns true when the block matched its expected metadata.
func (s BlockStatus) IsHealthy() bool { return s.Healthy }

// BlockSource produces Blocks for verification. bucket selects the target
// domain; keyPrefix=="" iterates the entire bucket, a non-empty value narrows
// to a sub-prefix (e.g. CLI scrub of one volume). When bucket=="" the source
// falls back to its constructor-bound bucket (background path). Implementations
// MUST honor ctx cancellation and close the returned channel when iteration
// completes or the context is canceled. Readers MUST drain the channel after
// canceling so the producer goroutine can exit cleanly.
type BlockSource interface {
	Name() string
	Iter(ctx context.Context, bucket, keyPrefix string) (<-chan Block, error)
}

// BlockVerifier checks a Block's local copy and (optionally) repairs it.
// Verify is read-only; Repair mutates state and is only called when Verify
// returns a non-healthy BlockStatus.
type BlockVerifier interface {
	Verify(ctx context.Context, b Block) (BlockStatus, error)
	Repair(ctx context.Context, b Block) error
}
