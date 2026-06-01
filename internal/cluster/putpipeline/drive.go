package putpipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gritive/GrainFS/internal/storage/directio"
)

// DriveActor owns shard writes for one local data directory. One
// long-lived goroutine per drive.
type DriveActor struct {
	in       chan EncryptedShardChunk
	dataDir  string
	commitCh chan<- ShardWriteResult
	pending  map[uint64]*shardWriteState

	mu       sync.Mutex // guards pending + registry
	registry map[uint64]registryEntry

	// skipFsync defers durability to the WAL fsync paid once per PUT
	// in CommitCoord. When true, finalize() does close + rename only.
	// Set by Pipeline.New when Config.WAL != nil.
	skipFsync bool

	// panicOnPut is a test-only seam: when non-zero, handle() panics on
	// the first chunk whose PutID matches, to exercise recover().
	panicOnPut uint64
}

// registryEntry records where a registered PUT's shard file lives.
type registryEntry struct {
	bucket   string
	shardKey string
	shardIdx int
}

// registerPut tells this drive that PutID will arrive with shard
// chunks, and where the final shard file should land.
func (d *DriveActor) registerPut(putID uint64, bucket, shardKey string, shardIdx int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.registry == nil {
		d.registry = make(map[uint64]registryEntry)
	}
	d.registry[putID] = registryEntry{bucket: bucket, shardKey: shardKey, shardIdx: shardIdx}
}

// Run consumes from d.in until ctx is done. A panic inside chunk
// handling is recovered so one bad PUT cannot kill the drive actor.
func (d *DriveActor) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-d.in:
			if !ok {
				return
			}
			d.handle(chunk)
		}
	}
}

func (d *DriveActor) handle(chunk EncryptedShardChunk) {
	defer putCiphertextBuf(chunk.Ciphertext)
	defer func() {
		if r := recover(); r != nil {
			d.failPanic(chunk, r)
		}
	}()

	if chunk.Err != nil {
		d.mu.Lock()
		state := d.pending[chunk.PutID]
		d.mu.Unlock()
		if state != nil {
			d.failChunk(chunk, state, chunk.Err) // closes+unlinks tmp, dropPending, emits ONE failed result
		} else {
			// no tmp opened yet (seal failed on the shard's first stripe): no tmp to clean,
			// but still drop the drive registry entry so the PUT's state does not leak,
			// then emit the single terminal result.
			d.dropPending(chunk.PutID)
			d.commitCh <- ShardWriteResult{PutID: chunk.PutID, ShardIdx: chunk.ShardIdx, Err: chunk.Err}
		}
		return
	}

	if d.panicOnPut != 0 && chunk.PutID == d.panicOnPut {
		d.panicOnPut = 0 // panic once, then let the PUT's retry/next PUT proceed
		panic("test-injected panic")
	}

	state := d.stateFor(chunk)
	if state == nil {
		return // error already reported via commitCh
	}

	n, err := state.f.Write(chunk.Ciphertext)
	if err != nil {
		d.failChunk(chunk, state, fmt.Errorf("write shard chunk: %w", err))
		return
	}
	state.bytesWritten += int64(n)

	if chunk.LastInPut {
		d.finalize(chunk, state)
	}
}

// stateFor returns the in-progress write state for chunk.PutID,
// lazily opening the tmp file on the first chunk. Returns nil after
// reporting a failure if the PUT was not registered or the file
// could not be created.
//
// The filesystem syscalls (mkdir + open + fcntl) run OUTSIDE the
// mutex so concurrent registerPut/dropPending calls from
// Pipeline.Put goroutines don't queue behind them. With 32 concurrent
// warp PUTs, holding the mutex across these syscalls dominated the
// mutex profile (94% of contention delay).
func (d *DriveActor) stateFor(chunk EncryptedShardChunk) *shardWriteState {
	d.mu.Lock()
	if s := d.pending[chunk.PutID]; s != nil {
		d.mu.Unlock()
		return s
	}
	entry, ok := d.registry[chunk.PutID]
	d.mu.Unlock()
	if !ok {
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      fmt.Errorf("drive actor: unregistered put %d", chunk.PutID),
		}
		return nil
	}
	shardDir := filepath.Join(d.dataDir, entry.bucket, entry.shardKey)
	// Containment guard (mirrors ShardService.ShardPathUnderDataDir): the
	// candidate path and its containment root both derive from bucket, so a
	// bucket of ".." (or one carrying a separator) would move both up
	// together and slip past the Rel check while physically escaping
	// d.dataDir — reject such buckets outright. shardKey is
	// ecObjectShardKey(objectKey, versionID) and getKey does not normalize
	// URL-encoded ".." in the S3 key, so the Rel check keeps the key from
	// escaping {dataDir}/{bucket}. S3 ingress (ValidBucketName) blocks the
	// public vector; this also guards the trusted peer shard-RPC / mover
	// paths that reach the chokepoint directly.
	if !isSafePathSegment(entry.bucket) || !pathUnderRoot(filepath.Join(d.dataDir, entry.bucket), shardDir) {
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      fmt.Errorf("drive actor: shard location escapes data dir (bucket=%q key=%q)", entry.bucket, entry.shardKey),
		}
		return nil
	}
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      fmt.Errorf("drive actor mkdir: %w", err),
		}
		return nil
	}
	finalPath := filepath.Join(shardDir, fmt.Sprintf("shard_%d", entry.shardIdx))
	tmpPath := finalPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      fmt.Errorf("drive actor open tmp: %w", err),
		}
		return nil
	}
	_ = directio.ApplyNoCacheHint(f)
	s := &shardWriteState{
		f:         f,
		finalPath: finalPath,
		tmpPath:   tmpPath,
		bucket:    entry.bucket,
		shardKey:  entry.shardKey,
		shardIdx:  entry.shardIdx,
	}
	d.mu.Lock()
	d.pending[chunk.PutID] = s
	d.mu.Unlock()
	return s
}

// finalize fsyncs (when no WAL is wired) + renames the completed
// shard and reports success.
func (d *DriveActor) finalize(chunk EncryptedShardChunk, state *shardWriteState) {
	if !d.skipFsync {
		if err := directio.Sync(state.f); err != nil {
			d.failChunk(chunk, state, fmt.Errorf("fsync shard: %w", err))
			return
		}
	}
	if err := state.f.Close(); err != nil {
		d.failChunk(chunk, state, fmt.Errorf("close shard: %w", err))
		return
	}
	if err := os.Rename(state.tmpPath, state.finalPath); err != nil {
		_ = os.Remove(state.tmpPath)
		d.dropPending(chunk.PutID)
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      fmt.Errorf("rename shard: %w", err),
		}
		return
	}
	d.dropPending(chunk.PutID)
	d.commitCh <- ShardWriteResult{
		PutID:    chunk.PutID,
		ShardIdx: chunk.ShardIdx,
		Bytes:    state.bytesWritten,
	}
}

// failChunk closes + unlinks the tmp file and reports the error.
func (d *DriveActor) failChunk(chunk EncryptedShardChunk, state *shardWriteState, err error) {
	_ = state.f.Close()
	_ = os.Remove(state.tmpPath)
	d.dropPending(chunk.PutID)
	d.commitCh <- ShardWriteResult{
		PutID:    chunk.PutID,
		ShardIdx: chunk.ShardIdx,
		Err:      err,
	}
}

// failPanic is the recover() path: clean up the PUT's tmp file if one
// was opened and report the panic as a shard failure.
func (d *DriveActor) failPanic(chunk EncryptedShardChunk, r any) {
	d.mu.Lock()
	state := d.pending[chunk.PutID]
	delete(d.pending, chunk.PutID)
	delete(d.registry, chunk.PutID)
	d.mu.Unlock()
	if state != nil {
		_ = state.f.Close()
		_ = os.Remove(state.tmpPath)
	}
	d.commitCh <- ShardWriteResult{
		PutID:    chunk.PutID,
		ShardIdx: chunk.ShardIdx,
		Err:      fmt.Errorf("drive actor panic: %v", r),
	}
}

func (d *DriveActor) dropPending(putID uint64) {
	d.mu.Lock()
	delete(d.pending, putID)
	delete(d.registry, putID)
	d.mu.Unlock()
}

// pathUnderRoot reports whether p resolves inside root after cleaning,
// i.e. no "../" traversal escapes root. Mirrors the containment logic of
// ShardService.ShardPathUnderDataDir for the pipeline's drive-local path.
func pathUnderRoot(root, p string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(p))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// isSafePathSegment reports whether name is a single, non-traversal path
// component — non-empty, not "." or "..", and free of any path separator.
// Keeps a bucket from re-rooting the containment check (it derives both the
// candidate path and its root). Mirrors cluster.isSafePathSegment, which is
// unexported in that package.
func isSafePathSegment(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsRune(name, '/') && !strings.ContainsRune(name, filepath.Separator)
}
