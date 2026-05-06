package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
)

func TestReceiptDBOptionsUseSmallBadgerArenas(t *testing.T) {
	opts := receiptDBOptions(t.TempDir())

	require.Equal(t, badgerutil.SmallMemTableSize, opts.MemTableSize)
	require.Equal(t, badgerutil.SmallNumMemtables, opts.NumMemtables)
	require.Equal(t, badgerutil.SmallBlockCacheSize, opts.BlockCacheSize)
}

func TestOpenReceiptDBRemovesEmptyMemtableWAL(t *testing.T) {
	dataDir := t.TempDir()
	subDir := filepath.Join(dataDir, "receipts")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "000001.mem"), nil, 0o644))

	db, decision, err := openReceiptDB(dataDir)
	require.NoError(t, err)
	require.Equal(t, subDir, decision.Path)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	require.NoFileExists(t, filepath.Join(subDir, "000001.mem"))
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("ok"), []byte("1"))
	}))
}
