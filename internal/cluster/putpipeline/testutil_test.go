package putpipeline

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

func testEncryptor(t testing.TB) *encrypt.Encryptor {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}
