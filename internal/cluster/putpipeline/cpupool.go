package putpipeline

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// CPUPool runs GOMAXPROCS worker goroutines that EC-split incoming
// stripes and AES-GCM-seal each shard chunk.
type CPUPool struct {
	in       chan StripePlaintext
	enc      *encrypt.Encryptor
	ecCfg    cluster.ECConfig
	workers  int
	outByPut map[uint64][]chan<- EncryptedShardChunk // protected by mu
}

// registerPut wires per-shard fan-out channels for one PUT.
func (p *CPUPool) registerPut(putID uint64, shardChans []chan<- EncryptedShardChunk) {
	// TODO Phase 2
}

// unregisterPut clears the per-PUT fan-out (called by Pipeline after PUT
// finalization).
func (p *CPUPool) unregisterPut(putID uint64) {
	// TODO Phase 2
}

// Run launches worker goroutines until ctx is done.
func (p *CPUPool) Run(ctx context.Context) {
	// TODO Phase 2
}
