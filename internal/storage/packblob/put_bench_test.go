package packblob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

func BenchmarkPutObject64KB(b *testing.B) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 64*1024)

	b.Run("local-object-files", func(b *testing.B) {
		backend, err := storage.NewLocalBackend(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = backend.Close() })
		if err := backend.CreateBucket(ctx, "bench"); err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := backend.PutObject(ctx, "bench", fmt.Sprintf("obj-%d", i), bytes.NewReader(payload), "application/octet-stream"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("packed-blob-compress", func(b *testing.B) {
		inner, err := storage.NewLocalBackend(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		packed, err := NewPackedBackend(inner, b.TempDir(), int64(len(payload)+1))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = packed.Close() })
		if err := packed.CreateBucket(ctx, "bench"); err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := packed.PutObject(ctx, "bench", fmt.Sprintf("obj-%d", i), bytes.NewReader(payload), "application/octet-stream"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("packed-blob-no-compress", func(b *testing.B) {
		inner, err := storage.NewLocalBackend(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		packed, err := NewPackedBackendWithOptions(inner, b.TempDir(), int64(len(payload)+1), PackedBackendOptions{Compress: false})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = packed.Close() })
		if err := packed.CreateBucket(ctx, "bench"); err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := packed.PutObject(ctx, "bench", fmt.Sprintf("obj-%d", i), bytes.NewReader(payload), "application/octet-stream"); err != nil {
				b.Fatal(err)
			}
		}
	})

}

func BenchmarkBlobStoreAppend64KBNoCompress(b *testing.B) {
	bs, err := NewBlobStore(b.TempDir(), math.MaxInt64)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bs.Close() })

	key := "bucket/key"
	payload := bytes.Repeat([]byte("x"), 64*1024)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bs.Append(key, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func newPackblobBenchmarkEncryptor(b *testing.B) *encrypt.Encryptor {
	b.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatal(err)
	}
	return enc
}

func BenchmarkGetObject64KB(b *testing.B) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 64*1024)

	b.Run("local-object-files", func(b *testing.B) {
		backend, err := storage.NewLocalBackend(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = backend.Close() })
		if err := backend.CreateBucket(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
		if _, err := backend.PutObject(ctx, "bench", "obj", bytes.NewReader(payload), "application/octet-stream"); err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rc, _, err := backend.GetObject(ctx, "bench", "obj")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, rc); err != nil {
				_ = rc.Close()
				b.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("packed-blob-no-compress", func(b *testing.B) {
		inner, err := storage.NewLocalBackend(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		packed, err := NewPackedBackendWithOptions(inner, b.TempDir(), int64(len(payload)+1), PackedBackendOptions{Compress: false})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = packed.Close() })
		if err := packed.CreateBucket(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
		if _, err := packed.PutObject(ctx, "bench", "obj", bytes.NewReader(payload), "application/octet-stream"); err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rc, _, err := packed.GetObject(ctx, "bench", "obj")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, rc); err != nil {
				_ = rc.Close()
				b.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("packed-blob-encrypted-no-compress", func(b *testing.B) {
		inner, err := storage.NewLocalBackend(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		packed, err := NewPackedBackendWithOptions(inner, b.TempDir(), int64(len(payload)+1), PackedBackendOptions{
			Compress:  false,
			Encryptor: newPackblobBenchmarkEncryptor(b),
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = packed.Close() })
		if err := packed.CreateBucket(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
		if _, err := packed.PutObject(ctx, "bench", "obj", bytes.NewReader(payload), "application/octet-stream"); err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rc, _, err := packed.GetObject(ctx, "bench", "obj")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, rc); err != nil {
				_ = rc.Close()
				b.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
