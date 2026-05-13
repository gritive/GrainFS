package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster"
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

func TestReceiptPeerAddressesUseMetaNodesForJoinedCluster(t *testing.T) {
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-a", Address: "127.0.0.1:10001"},
		{ID: "node-b", Address: "127.0.0.1:10002"},
		{ID: "node-c", Address: "127.0.0.1:10003"},
	}

	got := receiptPeerAddresses("node-b", "127.0.0.1:10002", []string{"127.0.0.1:10001"}, nodes)

	require.ElementsMatch(t, []string{"127.0.0.1:10001", "127.0.0.1:10003"}, got)
	require.NotContains(t, got, "127.0.0.1:10002")
}
