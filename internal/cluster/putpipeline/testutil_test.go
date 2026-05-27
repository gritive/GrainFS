package putpipeline

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

func testEncryptorRaw(t testing.TB) *encrypt.Encryptor {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}

func testShardEncryptor(t testing.TB) eccodec.ShardEncryptor {
	t.Helper()
	return storage.NewEncryptorAdapter(testEncryptorRaw(t), make([]byte, 16))
}
