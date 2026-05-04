package scrubber

import "context"

// ScrubScope selects which blocks Iter emits.
type ScrubScope int

const (
	// ScopeFull iterates every block under the source's domain (e.g. all
	// blocks of a volume including those referenced only by snapshots).
	ScopeFull ScrubScope = iota
	// ScopeLive iterates only the blocks reachable via the live mapping
	// (lat: index for EC; live_map for volumes). Cheaper, used by the
	// background scheduler's hot-path ticker.
	ScopeLive
)

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
	Detail  string
}

// IsHealthy returns true when the block matched its expected metadata.
func (s BlockStatus) IsHealthy() bool { return s.Healthy }

// BlockSource produces Blocks for verification. keyPrefix=="" iterates the
// full source (background); a non-empty prefix narrows to a single subject
// (e.g. CLI scrub of one volume). Implementations MUST honor ctx cancellation
// and close the returned channel when iteration completes or the context
// is canceled. Readers MUST drain the channel after canceling so the
// producer goroutine can exit cleanly.
type BlockSource interface {
	Name() string
	Iter(ctx context.Context, scope ScrubScope, keyPrefix string) (<-chan Block, error)
}

// BlockVerifier checks a Block's local copy and (optionally) repairs it.
// Verify is read-only; Repair mutates state and is only called when Verify
// returns a non-healthy BlockStatus.
type BlockVerifier interface {
	Verify(ctx context.Context, b Block) (BlockStatus, error)
	Repair(ctx context.Context, b Block) error
}
