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

	// newSink opens the destination for one shard's sealed ciphertext chunks.
	// nil ⟹ the default local-file sink (tmp file + atomic rename), which is
	// today's behavior. A remote-stream sink (S2) overrides this to ship the
	// same sealed bytes to a peer's WriteSealedShard RPC. Called outside d.mu.
	newSink func(bucket, shardKey string, shardIdx int) (shardSink, error)

	// panicOnPut is a test-only seam: when non-zero, handle() panics on
	// the first chunk whose PutID matches, to exercise recover().
	panicOnPut uint64
}

// shardSink is the destination one shard's sealed ciphertext chunks stream to.
// Sealing happens once upstream (CPUPool), so a sink only transports already-
// encrypted bytes. The default localFileSink writes a tmp file and atomically
// renames on Finalize (today's behavior, byte-identical). The S2 remote sink
// streams the same bytes to a peer, so at-rest plaintext never crosses the wire.
type shardSink interface {
	// Write appends sealed ciphertext for the shard.
	Write(p []byte) (int, error)
	// Finalize durably commits the shard (local: fsync + close + rename).
	// CONTRACT: DriveActor does NOT call Abort after a Finalize error — the
	// sink MUST have fully released and cleaned its own state (local: tmp
	// removed; remote: in-flight stream aborted + receiver told to discard the
	// partial shard) by the time Finalize returns non-nil. A Write error or a
	// panic, by contrast, IS followed by an Abort call.
	Finalize() error
	// Abort discards a partially-written shard (local: close + unlink). Called
	// by DriveActor after a Write error or a recovered panic — never after a
	// Finalize error (see Finalize's contract).
	Abort()
}

// localFileSink is the default shardSink: a per-shard tmp file atomically
// renamed into place on Finalize. Reproduces the pre-seam DriveActor behavior.
type localFileSink struct {
	f         *os.File
	finalPath string
	tmpPath   string
	skipFsync bool
}

func (s *localFileSink) Write(p []byte) (int, error) { return s.f.Write(p) }

func (s *localFileSink) Finalize() error {
	if !s.skipFsync {
		if err := directio.Sync(s.f); err != nil {
			_ = s.f.Close()
			_ = os.Remove(s.tmpPath)
			return fmt.Errorf("fsync shard: %w", err)
		}
	}
	if err := s.f.Close(); err != nil {
		_ = os.Remove(s.tmpPath)
		return fmt.Errorf("close shard: %w", err)
	}
	if err := os.Rename(s.tmpPath, s.finalPath); err != nil {
		_ = os.Remove(s.tmpPath)
		return fmt.Errorf("rename shard: %w", err)
	}
	return nil
}

func (s *localFileSink) Abort() {
	_ = s.f.Close()
	_ = os.Remove(s.tmpPath)
}

// openLocalFileSink is the default DriveActor.newSink: it applies the path
// containment guard, creates the shard dir, and opens the tmp file. Runs
// OUTSIDE d.mu (see stateFor) so concurrent registerPut/dropPending don't
// queue behind the mkdir + open syscalls.
func (d *DriveActor) openLocalFileSink(bucket, shardKey string, shardIdx int) (shardSink, error) {
	shardDir := filepath.Join(d.dataDir, bucket, shardKey)
	// Containment guard (mirrors ShardService.ShardPathUnderDataDir): the
	// candidate path and its containment root both derive from bucket, so a
	// bucket of ".." (or one carrying a separator) would move both up together
	// and slip past the Rel check while physically escaping d.dataDir — reject
	// such buckets outright. shardKey is ecObjectShardKey(objectKey, versionID)
	// and getKey does not normalize URL-encoded ".." in the S3 key, so the Rel
	// check keeps the key from escaping {dataDir}/{bucket}. S3 ingress
	// (ValidBucketName) blocks the public vector; this also guards the trusted
	// peer shard-RPC / mover paths that reach the chokepoint directly.
	if !isSafePathSegment(bucket) || !pathUnderRoot(filepath.Join(d.dataDir, bucket), shardDir) {
		return nil, fmt.Errorf("drive actor: shard location escapes data dir (bucket=%q key=%q)", bucket, shardKey)
	}
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("drive actor mkdir: %w", err)
	}
	finalPath := filepath.Join(shardDir, fmt.Sprintf("shard_%d", shardIdx))
	tmpPath := finalPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("drive actor open tmp: %w", err)
	}
	_ = directio.ApplyNoCacheHint(f)
	return &localFileSink{f: f, finalPath: finalPath, tmpPath: tmpPath, skipFsync: d.skipFsync}, nil
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

	n, err := state.sink.Write(chunk.Ciphertext)
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
// lazily opening the shard sink on the first chunk. Returns nil after
// reporting a failure if the PUT was not registered or the sink could
// not be opened.
//
// newSink runs OUTSIDE the mutex so concurrent registerPut/dropPending
// calls from Pipeline.Put goroutines don't queue behind it. For the
// default local-file sink that means the mkdir + open + fcntl syscalls
// stay off the lock — with 32 concurrent warp PUTs, holding the mutex
// across them dominated the mutex profile (94% of contention delay).
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
	newSink := d.newSink
	if newSink == nil {
		newSink = d.openLocalFileSink
	}
	sink, err := newSink(entry.bucket, entry.shardKey, entry.shardIdx)
	if err != nil {
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      err,
		}
		return nil
	}
	s := &shardWriteState{
		sink:     sink,
		bucket:   entry.bucket,
		shardKey: entry.shardKey,
		shardIdx: entry.shardIdx,
	}
	d.mu.Lock()
	d.pending[chunk.PutID] = s
	d.mu.Unlock()
	return s
}

// finalize durably commits the shard through its sink (local: fsync when no
// WAL is wired + close + rename) and reports success. A sink Finalize error is
// already self-cleaned by the sink, so we only drop state + report.
func (d *DriveActor) finalize(chunk EncryptedShardChunk, state *shardWriteState) {
	if err := state.sink.Finalize(); err != nil {
		d.dropPending(chunk.PutID)
		d.commitCh <- ShardWriteResult{
			PutID:    chunk.PutID,
			ShardIdx: chunk.ShardIdx,
			Err:      err,
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

// failChunk aborts the shard sink (local: close + unlink) and reports the error.
func (d *DriveActor) failChunk(chunk EncryptedShardChunk, state *shardWriteState, err error) {
	state.sink.Abort()
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
		state.sink.Abort()
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
