package putpipeline

import (
	"context"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"

	"github.com/dgraph-io/badger/v4"
)

// Config bundles the wiring a Pipeline needs.
type Config struct {
	DataDirs     []string
	Encryptor    *encrypt.Encryptor
	ECConfig     cluster.ECConfig
	StripeBytes  int // k * blockSize; defaults to 1<<20 (1 MiB) if 0
	ChannelDepth int // per-actor channel cap; defaults to 8 if 0
	BatchSize    int // metadata batch size; defaults to 32 if 0
	BadgerDB     *badger.DB
}

// Pipeline owns the long-lived actors and dispatches PUT requests.
type Pipeline struct {
	cfg          Config
	nextPutID    atomic.Uint64
	cpu          *CPUPool
	drives       []*DriveActor
	commit       *CommitCoord
	metaBatcher  *MetadataBatcher
	driveIns     []chan EncryptedShardChunk
	shutdownCtx  context.Context
	shutdownStop context.CancelFunc
}

// New constructs and starts a Pipeline.
func New(cfg Config) *Pipeline {
	// TODO Phase 5
	return nil
}

// Put dispatches one PUT through the pipeline. Blocks until K data
// shards complete (early-ack) and returns the storage.Object the
// handler should send back to the client.
func (p *Pipeline) Put(ctx context.Context, req PutRequest) (*storage.Object, error) {
	// TODO Phase 5
	return nil, nil
}

// Shutdown drains in-flight PUTs (with timeout) and stops all actor
// goroutines.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	// TODO Phase 5
	return nil
}
