package badgerutil

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	badgeropts "github.com/dgraph-io/badger/v4/options"
	"github.com/stretchr/testify/require"
)

func TestSmallOptionsReduceDefaultArenaBudget(t *testing.T) {
	defaults := badger.DefaultOptions(t.TempDir())
	opts := SmallOptions(t.TempDir())

	require.Equal(t, SmallMemTableSize, opts.MemTableSize)
	require.Equal(t, SmallNumMemtables, opts.NumMemtables)
	require.Equal(t, SmallBlockCacheSize, opts.BlockCacheSize)
	require.Equal(t, SmallValueThreshold, opts.ValueThreshold)
	require.Equal(t, SmallValueLogFileSize, opts.ValueLogFileSize)
	require.Equal(t, badgeropts.None, opts.Compression)
	require.LessOrEqual(t, opts.MemTableSize, defaults.MemTableSize/16)
	require.Less(t, opts.BlockCacheSize, defaults.BlockCacheSize)
	require.Less(t, opts.ValueLogFileSize, defaults.ValueLogFileSize)
}

func TestRaftLogOptionsPreserveDurabilityAndVersionSettings(t *testing.T) {
	opts := RaftLogOptions(t.TempDir(), true)

	require.True(t, opts.SyncWrites)
	require.Equal(t, 1, opts.NumVersionsToKeep)
	require.Equal(t, SmallMemTableSize, opts.MemTableSize)
	require.Equal(t, SmallValueThreshold, opts.ValueThreshold)
	require.Equal(t, SmallValueLogFileSize, opts.ValueLogFileSize)
}

func TestValueThresholdOverrideClampsToSmallBatchLimit(t *testing.T) {
	SetValueThresholdOverride(SmallMaxBatchSize + 1)
	t.Cleanup(func() { SetValueThresholdOverride(0) })

	opts := SmallOptions(t.TempDir())

	require.Equal(t, SmallMaxBatchSize, opts.ValueThreshold)
}
