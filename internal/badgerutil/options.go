package badgerutil

import badger "github.com/dgraph-io/badger/v4"

const (
	SmallMemTableSize   int64 = 8 << 20
	SmallBlockCacheSize int64 = 8 << 20
	SmallNumMemtables         = 2
)

// SmallOptions returns Badger options for GrainFS metadata stores that are
// numerous but small. Badger defaults reserve a 64 MiB memtable arena per DB;
// multi-raft clusters open many DBs, so the default arena dominates HeapAlloc.
func SmallOptions(path string) badger.Options {
	return badger.DefaultOptions(path).
		WithLogger(nil).
		WithMemTableSize(SmallMemTableSize).
		WithNumMemtables(SmallNumMemtables).
		WithBlockCacheSize(SmallBlockCacheSize).
		WithNumCompactors(2)
}

// RaftLogOptions keeps raft log durability semantics while using the smaller
// metadata-sized arena budget.
func RaftLogOptions(path string, syncWrites bool) badger.Options {
	return SmallOptions(path).
		WithSyncWrites(syncWrites).
		WithNumVersionsToKeep(1)
}
