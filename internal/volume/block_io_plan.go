package volume

// BlockActionKind is the write strategy chosen by the block I/O planner for a
// single block within a WriteAt range.
type BlockActionKind uint8

const (
	// ActionDirect writes to the canonical blockKey. Async=true means the
	// executor should use PutObjectAsync.
	ActionDirect BlockActionKind = iota
)

// BlockAction describes the write strategy for one block within a planWrite call.
// Fields are populated by blockIOPlanner; blockIOExecutor consumes them without
// calling HeadObject or dedup.ReadBlock again.
type BlockAction struct {
	Kind      BlockActionKind
	BlkNum    int64 // logical block number
	BlkOff    int64 // byte offset within the block where the write starts
	DataStart int   // index into the caller's p[] where this block's data begins
	CanWrite  int   // number of bytes to write from p[DataStart:]

	// Key is the target storage key: blockKey(name, blkNum).
	Key string

	// OldKey is the key currently in store; same as Key (blockKey), never empty.
	OldKey string

	// IsNew is true when no existing block was found for this blkNum. The
	// planner determines this via HeadObject. Used by the executor to track
	// AllocationBytesDelta.
	IsNew bool

	// Async, when true, makes the executor use PutObjectAsync (write-back NBD
	// path); when false it uses PutObject/WriteAt.
	Async bool
}
