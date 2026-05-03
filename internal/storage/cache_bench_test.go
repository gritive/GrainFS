package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

// BenchmarkGetObject_NoCache measures GetObject without caching.
func BenchmarkGetObject_NoCache(b *testing.B) {
	dir := b.TempDir()
	backend, err := NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	_ = backend.CreateBucket(context.Background(), "bench")
	data := strings.Repeat("x", 4096)
	for i := 0; i < 100; i++ {
		backend.PutObject(context.Background(), "bench", fmt.Sprintf("key-%d", i), strings.NewReader(data), "text/plain")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%100)
		rc, _, err := backend.GetObject(context.Background(), "bench", key)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}

// BenchmarkGetObject_Cached measures GetObject with LRU cache (warm).
func BenchmarkGetObject_Cached(b *testing.B) {
	dir := b.TempDir()
	backend, err := NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	cached := NewCachedBackend(backend)

	_ = cached.CreateBucket(context.Background(), "bench")
	data := strings.Repeat("x", 4096)
	for i := 0; i < 100; i++ {
		cached.PutObject(context.Background(), "bench", fmt.Sprintf("key-%d", i), strings.NewReader(data), "text/plain")
	}

	// Warm cache
	for i := 0; i < 100; i++ {
		rc, _, _ := cached.GetObject(context.Background(), "bench", fmt.Sprintf("key-%d", i))
		io.Copy(io.Discard, rc)
		rc.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%100)
		rc, _, err := cached.GetObject(context.Background(), "bench", key)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}

// BenchmarkHeadObject_NoCache measures HeadObject without caching.
func BenchmarkHeadObject_NoCache(b *testing.B) {
	dir := b.TempDir()
	backend, err := NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	_ = backend.CreateBucket(context.Background(), "bench")
	for i := 0; i < 100; i++ {
		backend.PutObject(context.Background(), "bench", fmt.Sprintf("key-%d", i), strings.NewReader("data"), "text/plain")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := backend.HeadObject(context.Background(), "bench", fmt.Sprintf("key-%d", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeadObject_Cached measures HeadObject with LRU cache (warm).
func BenchmarkHeadObject_Cached(b *testing.B) {
	dir := b.TempDir()
	backend, err := NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()

	cached := NewCachedBackend(backend)

	_ = cached.CreateBucket(context.Background(), "bench")
	for i := 0; i < 100; i++ {
		cached.PutObject(context.Background(), "bench", fmt.Sprintf("key-%d", i), strings.NewReader("data"), "text/plain")
	}

	// Warm cache via GetObject (populates both content and metadata cache)
	for i := 0; i < 100; i++ {
		rc, _, _ := cached.GetObject(context.Background(), "bench", fmt.Sprintf("key-%d", i))
		io.Copy(io.Discard, rc)
		rc.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cached.HeadObject(context.Background(), "bench", fmt.Sprintf("key-%d", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}
