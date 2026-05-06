package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestSetupReceiptRuntimeReturnsServerOptionsAndIdempotentClose(t *testing.T) {
	runtime, err := SetupReceiptRuntime(
		context.Background(),
		ReceiptRuntimeOptions{
			Options: ReceiptOptions{
				Enabled:        true,
				Retention:      time.Hour,
				GossipInterval: time.Hour,
				WindowSize:     1,
			},
			DataDir: t.TempDir(),
			NodeID:  "node-a",
		},
	)

	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotEmpty(t, runtime.ServerOptions)
	require.NotNil(t, runtime.Store())
	require.NotNil(t, runtime.KeyStore())
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
}

func TestSetupReceiptRuntimeDisabledReturnsEmptyRuntime(t *testing.T) {
	runtime, err := SetupReceiptRuntime(context.Background(), ReceiptRuntimeOptions{
		Options: ReceiptOptions{Enabled: false},
		DataDir: t.TempDir(),
		NodeID:  "node-a",
	})

	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.Empty(t, runtime.ServerOptions)
	require.Nil(t, runtime.Store())
	require.Nil(t, runtime.KeyStore())
	require.NoError(t, runtime.Close())
}

func TestSetupReceiptRuntimeRequiresPSKWhenPeersConfigured(t *testing.T) {
	runtime, err := SetupReceiptRuntime(context.Background(), ReceiptRuntimeOptions{
		Options: ReceiptOptions{
			Enabled:        true,
			Retention:      time.Hour,
			GossipInterval: time.Hour,
			WindowSize:     1,
		},
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		Peers:   []string{"node-b"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a PSK")
	require.NotNil(t, runtime)
	require.Empty(t, runtime.ServerOptions)
	require.Nil(t, runtime.Store())
	require.NoError(t, runtime.Close())
}

func TestSetupReceiptRuntimeDisablesOptionalReceiptRole(t *testing.T) {
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))

	runtime, err := SetupReceiptRuntime(context.Background(), ReceiptRuntimeOptions{
		Options: ReceiptOptions{
			Enabled:        true,
			Retention:      time.Hour,
			GossipInterval: time.Hour,
			WindowSize:     1,
		},
		DataDir: dataFile,
		NodeID:  "node-a",
	})

	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.Empty(t, runtime.ServerOptions)
	require.Nil(t, runtime.Store())
	require.Nil(t, runtime.KeyStore())
	require.NoError(t, runtime.Close())
}
