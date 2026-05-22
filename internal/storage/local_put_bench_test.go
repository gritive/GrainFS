package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

func BenchmarkLocalBackendPutObject5MiBEncrypted(b *testing.B) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(b, err)
	backend, err := NewEncryptedLocalBackend(b.TempDir(), enc)
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, backend.Close())
	})
	require.NoError(b, backend.CreateBucket(context.Background(), "bench"))

	payload := bytes.Repeat([]byte("x"), 5<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := backend.PutObject(context.Background(), "bench", "key", bytes.NewReader(payload), "application/octet-stream")
		require.NoError(b, err)
	}
}
