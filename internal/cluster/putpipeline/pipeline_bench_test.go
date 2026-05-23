package putpipeline

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

func benchPipeline(b *testing.B, size int) {
	dirs := []string{b.TempDir(), b.TempDir(), b.TempDir(), b.TempDir()}
	p := New(Config{
		DataDirs:    dirs,
		Encryptor:   testEncryptor(b),
		ECConfig:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		StripeBytes: 1 << 20,
	})
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	})

	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i % 251)
	}
	totalSize := int64(size)

	ctx := context.Background()
	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		_, err := p.Put(ctx, PutRequest{
			Bucket:   "bench",
			Key:      "obj-" + strconv.Itoa(i),
			Body:     bytes.NewReader(body),
			SizeHint: &totalSize,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipelinePut5MiB(b *testing.B)  { benchPipeline(b, 5*1024*1024) }
func BenchmarkPipelinePut10MiB(b *testing.B) { benchPipeline(b, 10*1024*1024) }

// Parallel variant: emulates concurrent S3 PUT requests, the warp workload.
func BenchmarkPipelinePut5MiBParallel(b *testing.B)  { benchPipelineParallel(b, 5*1024*1024) }
func BenchmarkPipelinePut10MiBParallel(b *testing.B) { benchPipelineParallel(b, 10*1024*1024) }

func benchPipelineParallel(b *testing.B, size int) {
	dirs := []string{b.TempDir(), b.TempDir(), b.TempDir(), b.TempDir()}
	p := New(Config{
		DataDirs:    dirs,
		Encryptor:   testEncryptor(b),
		ECConfig:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		StripeBytes: 1 << 20,
	})
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	})

	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i % 251)
	}
	totalSize := int64(size)
	ctx := context.Background()

	b.ResetTimer()
	b.SetBytes(int64(size))
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := p.Put(ctx, PutRequest{
				Bucket:   "bench",
				Key:      fmt.Sprintf("obj-%p-%d", pb, i),
				Body:     bytes.NewReader(body),
				SizeHint: &totalSize,
			})
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
