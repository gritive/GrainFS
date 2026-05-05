package badgerutil

import (
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	SmallMemTableSize   int64 = 8 << 20
	SmallBlockCacheSize int64 = 8 << 20
	SmallNumMemtables         = 2
)

// valueThresholdOverride, when > 0, forces every DB opened via SmallOptions
// to spill values larger than the threshold into the value log. Default 0
// keeps Badger's built-in heuristic (1 MiB). Used by e2e leak-fire tests to
// make the vlog grow naturally without writing 1 MiB-plus payloads.
var valueThresholdOverride atomic.Int64

// SetValueThresholdOverride installs a process-wide ValueThreshold override.
// Pass 0 to clear. Must be called before any DB Open via SmallOptions.
func SetValueThresholdOverride(v int64) {
	if v < 0 {
		v = 0
	}
	valueThresholdOverride.Store(v)
}

// SmallOptions returns Badger options for GrainFS metadata stores that are
// numerous but small. Badger defaults reserve a 64 MiB memtable arena per DB;
// multi-raft clusters open many DBs, so the default arena dominates HeapAlloc.
func SmallOptions(path string) badger.Options {
	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		WithMemTableSize(SmallMemTableSize).
		WithNumMemtables(SmallNumMemtables).
		WithBlockCacheSize(SmallBlockCacheSize).
		WithNumCompactors(2)
	if v := valueThresholdOverride.Load(); v > 0 {
		opts = opts.WithValueThreshold(v)
	}
	return opts
}

// RaftLogOptions keeps raft log durability semantics while using the smaller
// metadata-sized arena budget.
func RaftLogOptions(path string, syncWrites bool) badger.Options {
	return SmallOptions(path).
		WithSyncWrites(syncWrites).
		WithNumVersionsToKeep(1)
}
