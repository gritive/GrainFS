package badgerutil

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestSmallOptionsReduceDefaultArenaBudget(t *testing.T) {
	defaults := badger.DefaultOptions(t.TempDir())
	opts := SmallOptions(t.TempDir())

	require.Equal(t, SmallMemTableSize, opts.MemTableSize)
	require.Equal(t, SmallNumMemtables, opts.NumMemtables)
	require.Equal(t, SmallBlockCacheSize, opts.BlockCacheSize)
	require.LessOrEqual(t, opts.MemTableSize, defaults.MemTableSize/8)
	require.Less(t, opts.BlockCacheSize, defaults.BlockCacheSize)
}

func TestRaftLogOptionsPreserveDurabilityAndVersionSettings(t *testing.T) {
	opts := RaftLogOptions(t.TempDir(), true)

	require.True(t, opts.SyncWrites)
	require.Equal(t, 1, opts.NumVersionsToKeep)
	require.Equal(t, SmallMemTableSize, opts.MemTableSize)
}
