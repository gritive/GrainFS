package directio

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// BenchmarkShardWriteSync isolates the per-shard durability cost of the three
// SyncMode policies on the EC-shard write recipe (open tmp → write a shard-sized
// payload → Sync → rename). EC encode / XAES seal CPU is intentionally omitted:
// it is identical across modes, so the mode-to-mode delta here IS the fsync
// cost. Set GRAINFS_FSYNC_MODE={full,fast,off} to pick the policy (read at
// package init). On Linux full==fast==fdatasync, so full-vs-off is the lever
// that the cross-node-EC durability hypothesis turns on; on macOS full is
// F_FULLFSYNC.
//
// One EC 10MiB object spreads into 4 data shards of ~2.5MiB; this benchmarks a
// single such shard write, run in parallel to mimic concurrent PUTs.
func BenchmarkShardWriteSync(b *testing.B) {
	const shardSize = 10 * 1024 * 1024 / 4 // ~2.5MiB, one shard of a 10MiB EC2+2 object
	payload := make([]byte, shardSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	dir := b.TempDir()
	var gid atomic.Int64

	b.ResetTimer()
	b.SetBytes(shardSize)
	b.RunParallel(func(pb *testing.PB) {
		g := itoa(int(gid.Add(1))) // per-goroutine unique path
		final := filepath.Join(dir, "shard-"+g)
		tmp := final + ".tmp"
		for pb.Next() {
			f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := f.Write(payload); err != nil {
				b.Fatal(err)
			}
			if err := Sync(f); err != nil {
				b.Fatal(err)
			}
			if err := f.Close(); err != nil {
				b.Fatal(err)
			}
			// rename tmp→final (replaces previous iteration, bounded file count).
			if err := os.Rename(tmp, final); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// itoa avoids importing strconv just for a label.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
