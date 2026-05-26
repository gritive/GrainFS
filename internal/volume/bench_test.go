package volume

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// benchVolSize is large enough to hold all benchmark writes without wrapping.
const benchVolSize = 1 << 30 // 1 GiB

func benchBackend(b *testing.B) storage.Backend {
	b.Helper()
	dir := b.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	return backend
}

// BenchmarkWriteAt measures baseline write throughput.
func BenchmarkWriteAt(b *testing.B) {
	mgr := NewManager(benchBackend(b))
	if _, err := mgr.Create("vol", benchVolSize); err != nil {
		b.Fatal(err)
	}
	data := bytes.Repeat([]byte{0xAB}, DefaultBlockSize)

	b.SetBytes(int64(DefaultBlockSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i%256) * int64(DefaultBlockSize)
		if _, err := mgr.WriteAt("vol", data, off); err != nil {
			b.Fatal(err)
		}
	}
}
