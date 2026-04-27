//go:build linux

// Phase 2 research benchmark — measure O_DIRECT vs page-cache impact on EC
// shard writes BEFORE committing to a production directio package. Linux-only
// because O_DIRECT is a Linux flag; the macOS counterpart (F_NOCACHE) needs a
// separate post-open fcntl path and a different alignment story (no required
// alignment), so it lives in its own benchmark when written.
//
// Run inside the e2e Docker image (host macOS cannot exercise O_DIRECT):
//
//	make test-e2e-docker  # full suite, or:
//	docker run --rm -v /var/run/docker.sock:/var/run/docker.sock grainfs-e2e \
//	  go test -run=^$ -bench=BenchmarkShardWrite -benchtime=3s -count=3 \
//	  ./internal/cluster/...
//
// What we're measuring: the ShardService.WriteLocalShard pattern (open+write+
// fsync+close+rename+parent-dir-fsync). Production currently writes with the
// kernel page cache; the question is whether bypassing it via O_DIRECT moves
// throughput, latency tail, or sustained-write memory pressure for typical EC
// shard sizes (~1-16 MB).
//
// Test matrix: {1MB, 4MB, 16MB} × {default, O_DIRECT}. Concurrency=1 for now
// — single-shard latency is what matters for the put-path tail; concurrent
// shard fan-out is a separate axis we'll measure once the single-shard
// numbers tell us whether to bother.
//
// First-pass results (Docker on Linux VM, 2026-04-28, 3s × 3 runs):
//
//	1MB:   default 13-19 MB/s   |  O_DIRECT 166-195 MB/s  → ~10x faster
//	4MB:   default 192-379 MB/s |  O_DIRECT 379-511 MB/s  → ~40% faster
//	16MB:  default 327-366 MB/s |  O_DIRECT 316-384 MB/s  → no meaningful diff
//
// Reading: O_DIRECT pays off massively at the small-shard end (k=4 split of
// small-to-medium objects) and is neutral once writes are large enough to
// amortize the page-cache copy. Production EC shards land in the 1-4 MB
// sweet spot, so the implementation is worth it. Bare-metal Linux numbers
// may differ in magnitude but the qualitative shape (helps small, neutral
// large) is consistent with general filesystem behavior.

package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"unsafe"
)

// alignedBuffer returns a slice whose backing storage is page-aligned to
// `align` bytes — required for O_DIRECT writes on Linux. The standard library
// has no public way to ask Go for an aligned allocation, so we over-allocate
// and slice into the aligned offset.
func alignedBuffer(size, align int) []byte {
	raw := make([]byte, size+align)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	off := int((align - int(addr%uintptr(align))) % align)
	return raw[off : off+size]
}

// writeShardDefault matches the ShardService.WriteLocalShard sequence
// (tmp+fsync+rename+parent-fsync) using the default open flags. This is the
// baseline against which O_DIRECT is compared.
func writeShardDefault(b *testing.B, path string, payload []byte) {
	b.Helper()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	if _, err := f.Write(payload); err != nil {
		b.Fatalf("write: %v", err)
	}
	if err := f.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		b.Fatalf("rename: %v", err)
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

// writeShardDirect uses O_DIRECT to bypass the page cache. Payload buffer
// MUST be 4096-byte aligned and sized in 4096-byte multiples — the benchmark
// driver supplies aligned buffers. fsync is still issued because O_DIRECT
// alone does not flush disk firmware caches; production parity demands it.
func writeShardDirect(b *testing.B, path string, payload []byte) {
	b.Helper()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT, 0o600)
	if err != nil {
		b.Fatalf("open O_DIRECT: %v", err)
	}
	if _, err := f.Write(payload); err != nil {
		b.Fatalf("write: %v", err)
	}
	if err := f.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		b.Fatalf("rename: %v", err)
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

// shardSizes mirrors typical EC shard sizes: ~1 MB for small objects after
// k=4 split, ~4 MB for medium, ~16 MB for the upper end before multipart
// kicks in. All are 4096-multiples so the same buffer satisfies both paths.
var shardSizes = []struct {
	name string
	size int
}{
	{"1MB", 1 << 20},
	{"4MB", 4 << 20},
	{"16MB", 16 << 20},
}

func BenchmarkShardWrite_Default(b *testing.B) {
	for _, sz := range shardSizes {
		b.Run(sz.name, func(b *testing.B) {
			dir := b.TempDir()
			payload := alignedBuffer(sz.size, 4096)
			for i := range payload {
				payload[i] = byte(i)
			}
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				path := filepath.Join(dir, fmt.Sprintf("shard_%d", i))
				writeShardDefault(b, path, payload)
			}
		})
	}
}

func BenchmarkShardWrite_ODirect(b *testing.B) {
	for _, sz := range shardSizes {
		b.Run(sz.name, func(b *testing.B) {
			dir := b.TempDir()
			payload := alignedBuffer(sz.size, 4096)
			for i := range payload {
				payload[i] = byte(i)
			}
			// Probe once: some filesystems (overlayfs, tmpfs in some configs)
			// reject O_DIRECT. Skip rather than fail so the benchmark runs
			// wherever it can.
			probePath := filepath.Join(dir, "probe")
			f, err := os.OpenFile(probePath, os.O_WRONLY|os.O_CREATE|syscall.O_DIRECT, 0o600)
			if err != nil {
				b.Skipf("O_DIRECT not supported on this filesystem: %v", err)
			}
			f.Close()
			os.Remove(probePath)

			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				path := filepath.Join(dir, fmt.Sprintf("shard_%d", i))
				writeShardDirect(b, path, payload)
			}
		})
	}
}

// concurrencies covers single-writer (current cluster put-path latency),
// modest fan-out (typical EC k=4 split sending one shard local + 3 over the
// network — only 1 hits this benchmark), and high-concurrency stress (many
// PUTs in flight, e.g. backup ingest). The shape of the throughput curve at
// 16 and 64 tells us whether goroutine + blocking-syscall fan-out is enough
// or whether io_uring batching would meaningfully help.
//
// First-pass results (Docker on Linux VM, 4MB shards, 3s × 2 runs):
//
//	conc=1   ~725 MB/s (379-1070 — extreme variance, single-thread peaks high)
//	conc=4   ~297 MB/s (266-327)
//	conc=16  ~589 MB/s (385-793)
//	conc=64  ~661 MB/s (661-662)
//
// Reading: throughput plateaus around 600-700 MB/s regardless of concurrency.
// Higher concurrency doesn't help — sometimes hurts. This means the disk/
// fsync layer is the bottleneck, NOT syscall overhead. io_uring's value
// proposition (batched submissions, no per-op syscall) attacks the syscall
// layer and would not move the needle here.
//
// Verdict for GrainFS: io_uring is NOT worth pursuing on this workload class.
// EC shard writes are fsync-dominated by design (we want durability before
// rename). For a different workload — e.g. high-throughput append-only WAL
// without per-op fsync — io_uring evaluation would re-open. Bare-metal Linux
// + NVMe might raise the absolute numbers but the curve shape (plateau at
// moderate concurrency) is consistent with general fsync behavior on
// rotating queues.
var concurrencies = []int{1, 4, 16, 64}

// BenchmarkShardWrite_ODirect_Concurrent issues b.N writes split across N
// goroutines. Each goroutine has its own aligned buffer (no false sharing).
// The benchmark deliberately uses 4MB shards — the EC k=4 sweet spot — so
// the result speaks directly to production load.
func BenchmarkShardWrite_ODirect_Concurrent(b *testing.B) {
	const shardSize = 4 << 20
	for _, conc := range concurrencies {
		b.Run(fmt.Sprintf("conc=%d", conc), func(b *testing.B) {
			dir := b.TempDir()

			// One probe to skip cleanly when the FS rejects O_DIRECT.
			probePath := filepath.Join(dir, "probe")
			pf, err := os.OpenFile(probePath, os.O_WRONLY|os.O_CREATE|syscall.O_DIRECT, 0o600)
			if err != nil {
				b.Skipf("O_DIRECT not supported on this filesystem: %v", err)
			}
			pf.Close()
			os.Remove(probePath)

			// Atomic counter so every goroutine writes to a unique path.
			// Without this, two goroutines start at idx=0 and the second
			// rename races the first, surfacing as ENOENT on the .tmp file.
			var counter atomic.Int64
			b.SetBytes(int64(shardSize))
			b.ResetTimer()
			b.SetParallelism(conc)
			b.RunParallel(func(pb *testing.PB) {
				payload := alignedBuffer(shardSize, 4096)
				for i := range payload {
					payload[i] = byte(i)
				}
				for pb.Next() {
					id := counter.Add(1)
					path := filepath.Join(dir, fmt.Sprintf("shard_%d", id))
					writeShardDirect(b, path, payload)
				}
			})
		})
	}
}
