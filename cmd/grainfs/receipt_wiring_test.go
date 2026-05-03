package main

import (
	"testing"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/stretchr/testify/require"
)

func TestReceiptDBOptionsUseSmallBadgerArenas(t *testing.T) {
	opts := receiptDBOptions(t.TempDir())

	require.Equal(t, badgerutil.SmallMemTableSize, opts.MemTableSize)
	require.Equal(t, badgerutil.SmallNumMemtables, opts.NumMemtables)
	require.Equal(t, badgerutil.SmallBlockCacheSize, opts.BlockCacheSize)
}
