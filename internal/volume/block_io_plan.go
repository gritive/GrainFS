package volume

// BlockActionKind is the write strategy chosen by the block I/O planner for a
// single block within a WriteAt range.
type BlockActionKind uint8

const (
	// ActionDirect writes to the canonical blockKey. Used when dedup and CoW
	// are inactive. Async=true means the executor should use PutObjectAsync.
	ActionDirect BlockActionKind = iota
	// ActionDedup writes through the dedup index. The executor computes the
	// SHA-256, calls dedup.WriteBlock, and puts the object only if IsNew.
	ActionDedup
	// ActionCow writes to a copy-on-write key. Used when vol.SnapshotCount > 0.
	ActionCow
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

	// Key is the target storage key.
	//   ActionDirect: blockKey(name, blkNum)
	//   ActionDedup:  proposed new key (UUID); final canonical comes from dedup.WriteBlock
	//   ActionCow:    cowBlockKey(name, blkNum)
	Key string

	// OldKey is the key currently in store.
	//   ActionDirect: same as Key (blockKey); never empty
	//   ActionDedup:  canonical key from dedup.ReadBlock; "" if no entry exists (IsNew=true)
	//   ActionCow:    physicalKey(name, blkNum, liveMap); equals blockKey when block is new
	OldKey string

	// IsNew is true when no existing block was found for this blkNum.
	// The planner determines this via HeadObject (Direct/Cow) or dedup.ReadBlock (Dedup).
	// Used by the executor to track AllocationBytesDelta.
	IsNew bool

	// Async is set only for ActionDirect. When true the executor uses
	// PutObjectAsync (write-back NBD path); when false it uses PutObject/WriteAt.
	Async bool
}
