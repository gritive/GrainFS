package putpipeline

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

// testClusterID is the fixed 16-byte data-plane AAD id shared by the
// putpipeline test fixtures.
func testClusterID() []byte {
	cid := make([]byte, 16)
	for i := range cid {
		cid[i] = byte(i + 1)
	}
	return cid
}

func testDEKKeeper(t testing.TB) *encrypt.DEKKeeper {
	t.Helper()
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = byte(i + 1)
	}
	keeper, err := encrypt.NewDEKKeeper(kek, testClusterID())
	require.NoError(t, err)
	return keeper
}

func testShardEncryptor(t testing.TB) eccodec.ShardEncryptor {
	t.Helper()
	return storage.NewDEKKeeperAdapter(testDEKKeeper(t), testClusterID())
}
