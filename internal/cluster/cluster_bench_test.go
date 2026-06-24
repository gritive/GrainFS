package cluster

import (
	"testing"
)

// NOTE: BenchmarkIterObjectMetas_* and BenchmarkFSM_Apply_PutObjectMeta were
// removed in the quorum-meta blob codec decouple: per-object FSM commands are
// retired (apply no-ops them) and objects no longer live in the FSM obj: tree,
// so both benchmarks measured an empty/no-op path. Object-write throughput is
// covered by the off-raft quorum-meta blob + EC benchmarks (make bench).

// ─── ECSplit (baseline comparison) ───────────────────────────────────────────

func BenchmarkECSplit_1MB_4plus2(b *testing.B) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 1<<20) // 1 MiB
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		shards, err := ECSplit(cfg, data)
		if err != nil {
			b.Fatal(err)
		}
		_ = shards
	}
}
