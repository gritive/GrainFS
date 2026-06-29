package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/directio"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// LocalShardStore owns the local-shard concern carved out of ShardService: the
// shard-blob physical I/O, the at-rest seal (DEK-backed GFSENC3), the
// syncDirChain durability state machine (ancestor-fsync dedup) and the
// staging→promote-local path. ShardService keeps it as a `local` field and
// delegates every local-shard method to it; callers continue to see
// ShardService (behavior-preserving facade).
//
// dataDirs is SHARED config: both LocalShardStore and ShardService hold the same
// resolved shard roots (ShardService's quorum-meta + manifest methods still use
// theirs). It is a path base, not mutable state, so duplicating the slice header
// is safe.
type LocalShardStore struct {
	dataDirs  []string
	segEnc    storage.DataEncryptor // chunked EC-shard data-at-rest seam
	dekKeeper *encrypt.DEKKeeper
	clusterID [16]byte // zero sentinel in D-seg-ec-struct; real ID in slice C
	dirCache  sync.Map
	// dirDurable records, per directory path, that the dir's entry in its PARENT
	// has been persisted by a COMPLETED fsync (Stored post-fsync, never before).
	// syncDirChain consults it to skip re-fsyncing ancestor shard directories
	// whose child link a prior completed write already made durable. Same
	// process-local coherence model as dirCache (no external-delete invalidation;
	// shard leaf paths are version/UUID-keyed and never reborn — see syncDirChain).
	dirDurable sync.Map
	// syncFileHook / syncDirHook are nil-default test seams. Production leaves
	// them nil → fsyncFile/fsyncDir call directio.Sync / syncDir directly. Tests
	// set them to assert the durability fsync call ORDER (DBV / locked-order).
	syncFileHook func(*os.File) error
	syncDirHook  func(string) error
	// noRedundancy, when set, reports whether the deployment has no EC parity
	// and no peers (single-node 1+0). In that case a large metadata-only WAL
	// record cannot be recovered via EC reconstruction, so the shard file must
	// be fsynced directly. Read live so a later EC reconfig is reflected. Nil
	// (legacy callers / tests) never forces a direct fsync.
	noRedundancy func() bool
}

// LocalShardStoreOption is a functional option for LocalShardStore. The
// ShardService options that set the moved fields are reproduced as Local*
// equivalents so the DEK setup + nil-panic behavior is identical.
type LocalShardStoreOption func(*LocalShardStore)

// WithLocalShardDEKKeeper wires the generation-aware DEK keeper as the chunked
// EC-shard data-at-rest seam. clusterID MUST be 16 bytes and MUST equal the
// value the put pipeline binds (divergence fails every GET). nil keeper or
// non-16-byte clusterID is a no-op so callers can append the option before the
// keeper is available in narrowly-scoped tests.
func WithLocalShardDEKKeeper(keeper *encrypt.DEKKeeper, clusterID []byte) LocalShardStoreOption {
	return func(l *LocalShardStore) {
		if keeper == nil || len(clusterID) != 16 {
			return
		}
		copy(l.clusterID[:], clusterID)
		l.dekKeeper = keeper
		l.segEnc = storage.NewDEKKeeperAdapter(keeper, l.clusterID[:])
	}
}

// WithLocalNoRedundancy wires a provider reporting whether the deployment has no
// EC redundancy (ParityShards==0, single-node 1+0). When it returns true, a
// large metadata-only shard write is fsynced directly because EC reconstruction
// cannot rebuild a page-cache-lost shard with no parity and no peers. The
// provider is read live, so a later EC reconfig is honored.
func WithLocalNoRedundancy(fn func() bool) LocalShardStoreOption {
	return func(l *LocalShardStore) { l.noRedundancy = fn }
}

// NewLocalShardStore creates a LocalShardStore rooted at the given resolved shard
// directories (already including the trailing "shards" segment; NewShardService
// resolves them). The at-rest sealer (segEnc) is mandatory; absent it panics,
// identical to NewShardService — use WithLocalShardDEKKeeper.
func NewLocalShardStore(dataDirs []string, opts ...LocalShardStoreOption) *LocalShardStore {
	l := &LocalShardStore{dataDirs: dataDirs}
	for _, opt := range opts {
		opt(l)
	}
	l.requireAtRestSealer()
	return l
}

// requireAtRestSealer enforces the mandatory at-rest sealer invariant shared by
// NewLocalShardStore and NewShardService (which builds its LocalShardStore via
// the same path). Kept as the single owner of the panic message so the
// production invariant + its exact text stay identical.
func (l *LocalShardStore) requireAtRestSealer() {
	if l.segEnc == nil {
		panic("cluster.NewShardService: at-rest sealer is mandatory; use WithShardDEKKeeper")
	}
}

// DataDirs returns the active shard data directories.
func (l *LocalShardStore) DataDirs() []string { return l.dataDirs }

// DEKKeeper returns the generation-aware DEK keeper backing the at-rest seal.
func (l *LocalShardStore) DEKKeeper() *encrypt.DEKKeeper { return l.dekKeeper }

// ClusterID returns a copy of the 16-byte cluster identifier bound to the seal.
func (l *LocalShardStore) ClusterID() []byte {
	out := make([]byte, len(l.clusterID))
	copy(out, l.clusterID[:])
	return out
}

// getShardDir resolves the on-disk directory for an object's shard and rejects
// any key whose ".." segments would escape the {dataDir}/{bucket} root. This is
// the single containment chokepoint for every shard path consumer (S3 writes,
// peer shard RPC, record-driven mover/repair, reads) — see ShardPathUnderDataDir.
func (l *LocalShardStore) getShardDir(bucket, key string, shardIdx int) (string, error) {
	targetDir := l.dataDirs[shardIdx%len(l.dataDirs)]
	dir := filepath.Join(targetDir, bucket, key)
	if !l.ShardPathUnderDataDir(bucket, shardIdx, dir) {
		return "", fmt.Errorf("shard path for object key %q escapes the shard root", key)
	}
	return dir, nil
}

func (l *LocalShardStore) getShardPath(bucket, key string, shardIdx int) (string, error) {
	dir, err := l.getShardDir(bucket, key, shardIdx)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx)), nil
}

// ShardPathUnderDataDir reports whether p resolves inside the {dataDir}/{bucket}
// subtree for the given shard index — i.e. no "../" traversal escapes the shard
// root. The scrubber repair read/write paths derive the on-disk location from an
// object key via getShardPath; a key containing enough ".." would otherwise let
// that path resolve outside the shard root. Callers MUST reject when this returns
// false (containment guard previously provided by shardServiceKeyFromPath).
func (l *LocalShardStore) ShardPathUnderDataDir(bucket string, shardIdx int, p string) bool {
	if len(l.dataDirs) == 0 {
		return false
	}
	// The candidate path AND the containment root are both derived from bucket,
	// so a bucket of ".." (or one carrying a separator) would move both up
	// together and slip past the per-bucket Rel check while physically escaping
	// the shard data dir. Require bucket to be a single clean path segment. S3
	// ingress already rejects these (ValidBucketName); this guards the trusted
	// peer shard-RPC / mover paths that reach the chokepoint directly.
	if !isSafePathSegment(bucket) {
		return false
	}
	root := filepath.Join(l.dataDirs[shardIdx%len(l.dataDirs)], bucket)
	rel, err := filepath.Rel(root, filepath.Clean(p))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// WriteLocalShard stores a shard on the local node's disk without involving
// the cluster transport. Used by PutObject when this node is the destination for
// one of an object's shards (self-placement); avoids a loopback RPC.
// The shard is sealed via the DEK keeper (GFSENC3) before writing.
// Writes are crash-safe via tmp + rename for atomic visibility. Durability is
// established at write time (no WAL since S4): small / no-redundancy shards
// fsync the file + parent-dir chain; large redundant shards rely on EC
// reconstruction + the background scrubber.
// New encrypted writes use eccodec's chunked AEAD envelope. Plain writes keep
// the CRC envelope while that compatibility path remains available.
func (l *LocalShardStore) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	return l.writeLocalShard(context.Background(), bucket, key, shardIdx, data)
}

func (l *LocalShardStore) WriteLocalShardContext(ctx context.Context, bucket, key string, shardIdx int, data []byte) error {
	return l.writeLocalShard(ctx, bucket, key, shardIdx, data)
}

// EncodeEncryptedShardBuffer seals data as a GFSENC3 chunked shard using the
// DEK seam, identical to the normal writeLocalShard format. Used by the
// scrubber/EC-repair path (DistributedBackend.WriteShard) so repaired shards
// are DEK-encrypted at rest like normally-written shards. The at-rest sealer
// (segEnc) is mandatory (NewShardService panics if absent), so there is no
// plaintext branch.
func (l *LocalShardStore) EncodeEncryptedShardBuffer(bucket, key string, shardIdx int, data []byte) ([]byte, error) {
	payload, err := eccodec.EncodeEncryptedShardToBuffer(data, l.segEnc, ShardAADFields(bucket, key, shardIdx), eccodec.DefaultEncryptedChunkSize)
	if err != nil {
		return nil, fmt.Errorf("encode encrypted shard: %w", err)
	}
	return payload, nil
}

// writeLocalShard writes shard shardIdx of (bucket,key) to its final on-disk path,
// encrypted with (bucket,key,shardIdx) as AAD.
func (l *LocalShardStore) writeLocalShard(ctx context.Context, bucket, key string, shardIdx int, data []byte) error {
	return l.writeLocalShardAAD(ctx, bucket, key, key, shardIdx, data)
}

// writeLocalShardStaged writes shard shardIdx to the STAGING physical path stagingKey but encrypts it
// with the FINAL logical shard key (finalKey) as AAD, so a post-promote read of finalKey — which
// decrypts with the final-key AAD — succeeds. PR1 segment staging: in-flight segment shards land in a
// per-dataDir staging dir and are promoted (renamed) to finalKey only at commit. stagingKey MUST place
// the shard on the SAME dataDir as finalKey (shardIdx%len(dataDirs)) so the promote rename is atomic.
func (l *LocalShardStore) writeLocalShardStaged(ctx context.Context, bucket, stagingKey, finalKey string, shardIdx int, data []byte) error {
	return l.writeLocalShardAAD(ctx, bucket, stagingKey, finalKey, shardIdx, data)
}

// writeLocalShardAAD is the shared core: the shard FILE is written under pathKey's shard dir, but the
// encryption AAD is derived from aadKey. The two are equal for a normal write and differ for a staged
// write (pathKey = staging path, aadKey = final logical key).
func (l *LocalShardStore) writeLocalShardAAD(ctx context.Context, bucket, key, aadKey string, shardIdx int, data []byte) error {
	return l.writeLocalShardAADStream(ctx, bucket, key, aadKey, shardIdx, bytes.NewReader(data), int64(len(data)))
}

// shardCountingReader counts plaintext bytes consumed by the encoder so a short
// body (a truncated shard) is detected and rejected instead of silently writing a
// short shard. It restores the io.ReadFull guard the buffered readShardPayload
// path gave us before the encode was switched to streaming.
type shardCountingReader struct {
	r io.Reader
	n int64
}

func (c *shardCountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// writeLocalShardAADStream is the streaming core of writeLocalShardAAD: it encodes
// the shard straight from body without first materializing the plaintext as a
// []byte, which the sized streaming shard-write path uses to drop one whole-shard
// buffer. The []byte entrypoint wraps its slice in a bytes.Reader so both paths
// share this code and produce byte-equivalent output. When sizeHint >= 0 the body
// is bounded to sizeHint and the consumed count is verified, so a short or
// oversized body fails loudly (data-loss guard) — the same protection the
// buffered io.ReadFull path provided. The encoded ciphertext is streamed
// straight into the shard temp file (atomicShardFileWrite), so no whole
// encrypted shard is ever materialized as a []byte.
func (l *LocalShardStore) writeLocalShardAADStream(ctx context.Context, bucket, key, aadKey string, shardIdx int, body io.Reader, sizeHint int64) error {
	dir, err := l.getShardDir(bucket, key, shardIdx)
	if err != nil {
		return err
	}
	mkdirStart := time.Now()
	if err := l.ensureShardDir(dir); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalMkdir, mkdirStart, PutTraceStageFields{
			Bytes:            sizeHint,
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		return fmt.Errorf("create shard dir: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalMkdir, mkdirStart, PutTraceStageFields{
		Bytes:            sizeHint,
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	encodeStart := time.Now()
	cr := &shardCountingReader{r: body}
	if sizeHint >= 0 {
		cr.r = io.LimitReader(body, sizeHint)
	}
	// Encode the shard ciphertext straight into the temp file: atomicShardFileWrite
	// runs this callback writing directly to the file's fd, so the GFSENC3 bytes are
	// never materialized as a whole-shard []byte. The short-read guard runs inside
	// the callback (after the encode consumed body) so a body shorter than the
	// committed sizeHint aborts the write — the helper removes the tmp, no truncated
	// shard is published (data-loss guard). The helper emits the
	// EncOpen/EncWrite/EncSync/EncClose/EncRename/DirSync stages and owns the
	// fsync/dir-sync durability; the Encode stage below frames the whole operation
	// for error attribution and overall timing.
	werr := l.atomicShardFileWrite(ctx, dir, path, shardIdx, func(w io.Writer) error {
		if err := eccodec.EncodeEncryptedShard(w, cr, l.segEnc, ShardAADFields(bucket, aadKey, shardIdx), eccodec.DefaultEncryptedChunkSize); err != nil {
			return err
		}
		if sizeHint >= 0 && cr.n != sizeHint {
			return fmt.Errorf("shard %d short read: encoded %d plaintext bytes, expected %d", shardIdx, cr.n, sizeHint)
		}
		return nil
	})
	if werr != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncode, encodeStart, PutTraceStageFields{
			Bytes:            sizeHint,
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            werr.Error(),
		})
		return werr
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncode, encodeStart, PutTraceStageFields{
		Bytes:            sizeHint,
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	return nil
}

// writeLocalSealedShard stores an ALREADY-SEALED shard payload verbatim — no
// re-encryption at the destination. Used by the S2 streaming path: the
// coordinator's CPUPool seals the shard at the source (seal-at-source), so the
// bytes on the wire and on disk are identical GFSENC3 ciphertext. This mirrors
// writeLocalShard's tail (mkdir → tmp+rename → fsync/EC durability) MINUS the
// encode step. Packing is intentionally bypassed: the
// streaming path is the large-object band (shard ≫ pack threshold), so a
// sealed-stream shard always materializes as a shard_N file. `sealed` must be
// EncodeEncryptedShard output for ShardAADFields(bucket,key,shardIdx) (the same
// identity this node will use on read), or the read-side AEAD/AAD check fails.
func (l *LocalShardStore) writeLocalSealedShard(ctx context.Context, bucket, key string, shardIdx int, sealed []byte) error {
	dir, err := l.getShardDir(bucket, key, shardIdx)
	if err != nil {
		return err
	}
	if err := l.ensureShardDir(dir); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	return l.writeEncryptedShardFile(ctx, dir, path, sealed, shardIdx)
}

// fsyncFile fsyncs an open shard tmp file (or the test seam when set).
func (l *LocalShardStore) fsyncFile(f *os.File) error {
	if l.syncFileHook != nil {
		return l.syncFileHook(f)
	}
	return directio.Sync(f)
}

// fsyncDir fsyncs one directory so a rename (the durable link of a shard file
// into the namespace) survives a crash (or the test seam).
func (l *LocalShardStore) fsyncDir(dir string) error {
	if l.syncDirHook != nil {
		return l.syncDirHook(dir)
	}
	return syncDir(dir)
}

// syncDirChain durably LINKS a freshly written shard into the namespace: it
// fsyncs the leaf (its new shard-file entry) and persists each ancestor's entry
// in its parent up to (exclusive) stop. getShardDir builds dir = dataDir/bucket/key
// (version-keyed key dir created per PUT), so fsyncing only the leaf would leave
// the leaf's own entry in its parent non-durable (a crash could lose the whole
// shard dir after the quorum-meta commit). stop is the shard's data dir; the
// bucket dir's own entry in dataDir is out of scope (a pre-existing gap — see
// below — that this function never closed and CreateBucket does not either).
//
// Dedup: the leaf is fsynced every write (a new shard FILE lands there). An
// ancestor only needs fsyncing the first time its child entry is created+
// persisted; once a COMPLETED prior write made that link durable, the chain stays
// durable across later writes (which only add shard FILES to leaves, never
// restructure ancestors). dirDurable(d) records that d's entry in its parent was
// persisted; the mark is Stored only AFTER the persisting fsync(parent(d))
// returns, so any skip is backed by durable on-disk data (race-free under the
// concurrent same-leaf shard writes the errgroup data-shard path issues). A
// newly-created dir is absent from the map (== not durable) → its parent IS
// fsynced, persisting the new entry; so a new sibling is never skipped.
//
// Coherence: dirDurable is process-local with no external-delete invalidation —
// the same model as dirCache. Every shard-leaf path is version/UUID-keyed and the
// delete paths (DeleteLocalShards, orphan RemoveAll) operate on those unique
// leaves, never reborn, so a stale mark is never consulted again.
func (l *LocalShardStore) syncDirChain(leaf, stop string) error {
	stop = filepath.Clean(stop)
	leaf = filepath.Clean(leaf)
	if leaf == stop {
		// Degenerate (unreachable for real shards: dir = dataDir/bucket/key is
		// always strictly below stop = dataDir). Preserve the pre-dedup contract,
		// whose `for d := leaf; d != stop` loop fsynced nothing here.
		return nil
	}
	// The leaf always receives a fresh shard-file entry on this write → fsync its
	// CONTENTS every time, independent of whether its own dir entry is durable.
	if err := l.fsyncDir(leaf); err != nil {
		return err
	}
	for d := leaf; ; {
		if _, durable := l.dirDurable.Load(d); durable {
			return nil // d's entry — and the whole chain above it — already durable
		}
		parent := filepath.Dir(d)
		if parent == stop {
			// d is the shard-tree bucket dir; stop is the data dir. Match the
			// pre-dedup contract exactly: never fsync stop (its bucket-dir entry is
			// a pre-existing, out-of-scope durability gap). Record d so future
			// writes can stop here too.
			l.dirDurable.Store(d, struct{}{})
			return nil
		}
		if parent == d { // reached filesystem root WITHOUT hitting stop
			// stop must be an ancestor of leaf; if not, the caller passed a wrong
			// data-dir root and we'd otherwise silently report success (masking a
			// placement/path bug) — fail loudly instead.
			return fmt.Errorf("syncDirChain: stop %q is not an ancestor of %q", stop, leaf)
		}
		if err := l.fsyncDir(parent); err != nil {
			return err // d NOT marked durable: its entry isn't persisted yet
		}
		l.dirDurable.Store(d, struct{}{})
		d = parent
	}
}

// atomicShardFileWrite writes a shard file with the atomic tmp + rename recipe,
// streaming whatever the writeBody callback produces straight into the temp file.
// The callback writes directly to the *os.File (no intermediate buffer): the
// AEAD encoder already emits in ~1 MiB units (an 8-byte chunk header + one
// chunk-sized sealed body per chunk), so unbuffered writes cost ~2 syscalls per
// MiB — a buffer would only add a full-shard memcpy and a per-write allocation
// without cutting syscalls. The callback is the only producer of bytes; the
// helper counts the ciphertext bytes it wrote (countingWriter) and derives the
// fsync class from that count — identical to the old "caller computes requireFsync
// from len(payload)" contract, because the bytes written ARE the payload. A
// buffered ([]byte) caller passes a one-line w.Write(payload) callback; the sized
// streaming caller runs the encode straight into w, so no whole encrypted shard
// is ever materialized.
//
// Durability — locked order, byte-for-byte preserved:
//
//	write(tmp) → Sync(tmp) → rename → syncDirChain(dir) → return.
//
// Writes go straight to the fd, so there is no buffered tail to flush — a short
// write surfaces as an error from w.Write immediately, not at a deferred flush.
// Any error (open / writeBody / fsync / close / rename) removes the tmp so no
// partial shard is published and no orphan .tmp leaks.
//
// Post-S2 the fsync classes (small / no-redundancy) fsync the shard file + its
// directory CHAIN at write time; large redundant shards (requireFsync=false)
// skip both — EC reconstruction + the scrubber own their durability (S1). Trace
// stages (EncOpen/EncWrite/EncSync/EncClose/EncRename/DirSync) report the
// ciphertext bytes written (cw.n); EncOpen fires before any byte is written so it
// reports 0 — the put-trace sink is a diagnostic-only no-op in production.
func (l *LocalShardStore) atomicShardFileWrite(ctx context.Context, dir, path string, shardIdx int, writeBody func(w io.Writer) error) error {
	tmp := fmt.Sprintf("%s.%d.%d.tmp", path, os.Getpid(), time.Now().UnixNano())
	openStart := time.Now()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncOpen, openStart, PutTraceStageFields{
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		return fmt.Errorf("create tmp shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncOpen, openStart, PutTraceStageFields{
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}

	// Write the encoded ciphertext straight to the fd (no bufio): the AEAD encoder's
	// native ~1 MiB write granularity already keeps syscalls low, and the tmp file
	// has no O_DIRECT alignment constraint. countingWriter accumulates the ciphertext
	// byte count that drives the fsync class below.
	var written int64
	cw := &countingWriter{w: f, n: &written}
	writeStart := time.Now()
	if err := writeBody(cw); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncWrite, writeStart, PutTraceStageFields{
			Bytes:            written,
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		cleanup()
		return fmt.Errorf("write tmp shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncWrite, writeStart, PutTraceStageFields{
		Bytes:            written,
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	// requireFsync is derived from the ciphertext bytes actually written — the same
	// value the callers previously passed as shardWriteRequiresFsync(len(payload)).
	requireFsync := l.shardWriteRequiresFsync(int(written))

	// EncSync fsyncs the shard file only when requireFsync is set: for small
	// shards and for large no-redundancy shards (no parity to reconstruct from).
	// It is skipped for large redundant shards, which rely on EC reconstruction
	// + the scrubber (S1 — no WAL record, no fsync).
	encSyncStart := time.Now()
	if requireFsync {
		if err := l.fsyncFile(f); err != nil {
			ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncSync, encSyncStart, PutTraceStageFields{
				Bytes:            written,
				ShardIndex:       shardIdx,
				ShardTargetClass: "local",
				Error:            err.Error(),
			})
			cleanup()
			return fmt.Errorf("fsync tmp shard: %w", err)
		}
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncSync, encSyncStart, PutTraceStageFields{
		Bytes:            written,
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	closeStart := time.Now()
	if err := f.Close(); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncClose, closeStart, PutTraceStageFields{
			Bytes:            written,
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncClose, closeStart, PutTraceStageFields{
		Bytes:            written,
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	renameStart := time.Now()
	if err := os.Rename(tmp, path); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncRename, renameStart, PutTraceStageFields{
			Bytes:            written,
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		_ = os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncRename, renameStart, PutTraceStageFields{
		Bytes:            written,
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	// D2 (DBV): after the rename, fsync the shard's directory CHAIN (leaf shard
	// dir + each newly-created ancestor up to the data dir) so the durable link
	// of the shard file into the namespace cannot be lost on crash. Gated on
	// requireFsync — large redundant shards (S1) own durability via EC
	// reconstruction and skip both the file and dir fsync (a vanished dir there
	// is just a "missing shard" rebuilt lazily). Locked order:
	// write(tmp) → Sync(tmp) → rename → syncDirChain(dir) → return.
	if requireFsync {
		dirSyncStart := time.Now()
		stop := l.dataDirs[shardIdx%len(l.dataDirs)]
		if err := l.syncDirChain(dir, stop); err != nil {
			ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalDirSync, dirSyncStart, PutTraceStageFields{
				Bytes:            written,
				ShardIndex:       shardIdx,
				ShardTargetClass: "local",
				Error:            err.Error(),
			})
			return fmt.Errorf("fsync shard dir chain: %w", err)
		}
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalDirSync, dirSyncStart, PutTraceStageFields{
			Bytes:            written,
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
		})
	}

	return nil
}

// writeEncryptedShardFile materializes a pre-encoded (chunked AEAD) shard
// payload to disk via atomicShardFileWrite. The whole payload is already sealed
// GFSENC3 (seal-at-source / EC-repair), so the callback is a single
// w.Write(payload); the fsync class is derived from the bytes written, identical
// to the old caller-passed requireFsync (== shardWriteRequiresFsync(len(payload))).
func (l *LocalShardStore) writeEncryptedShardFile(ctx context.Context, dir, path string, payload []byte, shardIdx int) error {
	return l.atomicShardFileWrite(ctx, dir, path, shardIdx, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
}

// shardWriteRequiresFsync reports whether a freshly written shard file (and its
// parent directory chain) must be fsynced for durability, by shard class.
// Post-S2 the data WAL is no longer written on the shard PUT path — durability
// is established at write time:
//
//   - Small (< largeShardFsyncThreshold) OR large with NO EC redundancy
//     (ParityShards==0, i.e. noRedundancy()): fsync the shard file + parent dir
//     chain (there is no parity to reconstruct from, or the shard is too small
//     to be worth an EC stripe) → returns true.
//   - Large WITH EC redundancy (ParityShards>0, i.e. !noRedundancy): EC
//     reconstruction + the background scrubber (S0/S1) own durability; the file
//     is NOT fsynced → returns false. A nil noRedundancy provider counts as
//     redundant (matches production wiring; nil only occurs in tests).
//
// S4 removed the data WAL entirely, so there is no replay path to special-case.
func (l *LocalShardStore) shardWriteRequiresFsync(payloadLen int) bool {
	large := payloadLen >= largeShardFsyncThreshold
	if large && (l.noRedundancy == nil || !l.noRedundancy()) {
		return false // large + redundant: EC durability, no fsync (S1)
	}
	return true // small, or large + no-redundancy: write-time fsync
}

// WriteLocalShardStream stores a shard from body without buffering plaintext.
func (l *LocalShardStore) WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error {
	return l.WriteLocalShardStreamContext(context.Background(), bucket, key, shardIdx, body)
}

func (l *LocalShardStore) WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error {
	return l.writeLocalShardStreamContext(ctx, bucket, key, shardIdx, body, -1)
}

func (l *LocalShardStore) WriteLocalShardStreamSizedContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error {
	return l.writeLocalShardStreamContext(ctx, bucket, key, shardIdx, body, streamSize)
}

// WriteLocalShardStreamStagedContext is the streaming counterpart of
// writeLocalShardStaged: it buffers the shard body (bounded) and writes it to the
// STAGING physical path stagingKey while sealing with the FINAL logical key
// (finalKey) as AAD. PR1 segment staging; used by the local endpoint and the
// native shard-write receiver when a staging redirect is present.
func (l *LocalShardStore) WriteLocalShardStreamStagedContext(ctx context.Context, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader) error {
	rawCap := maxRawShardPayload(false)
	data, err := readShardPayload(body, rawCap, -1, false)
	if err != nil {
		return err
	}
	return l.writeLocalShardStaged(ctx, bucket, stagingKey, finalKey, shardIdx, data)
}

func (l *LocalShardStore) WriteLocalShardStreamStagedSizedContext(ctx context.Context, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader, streamSize int64) error {
	if streamSize < 0 {
		return l.WriteLocalShardStreamStagedContext(ctx, bucket, stagingKey, finalKey, shardIdx, body)
	}
	return l.writeLocalShardAADStream(ctx, bucket, stagingKey, finalKey, shardIdx, body, streamSize)
}

func (l *LocalShardStore) writeLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error {
	rawCap := maxRawShardPayload(false)
	// Sized path: the shard length is known, so stream the body straight into the
	// encoder (writeLocalShardAADStream, which bounds + count-verifies it against
	// streamSize) instead of buffering the whole plaintext shard as a []byte. This
	// drops one whole-shard buffer on every sized EC shard write (large PUT,
	// multipart, COPY). writeLocalShard == writeLocalShardAAD(key, key).
	if streamSize >= 0 {
		if streamSize > rawCap {
			return fmt.Errorf("shard payload too large: %d", streamSize)
		}
		return l.writeLocalShardAADStream(ctx, bucket, key, key, shardIdx, body, streamSize)
	}
	// Unsized path: the length is unknown, so buffer the body bounded by a 64 MiB
	// memory cap (the overflow check needs the full byte count) and route through
	// writeLocalShard. Durability no longer involves a WAL (S4): writeLocalShard
	// fsyncs small / no-redundancy shards directly and relies on EC for large
	// redundant shards.
	data, err := readShardPayload(body, rawCap, streamSize, false)
	if err != nil {
		return err
	}
	return l.writeLocalShard(ctx, bucket, key, shardIdx, data)
}

// PromoteLocalStagedShards renames a segment's staged shard dirs (written under stagingKey by
// writeLocalShardStaged) to their final path (finalKey), one intra-dataDir atomic dir-rename per
// dataDir. stagingKey and finalKey map to the SAME dataDir per shard index (both via getShardDir), so
// each rename stays within one device. Idempotent: a missing staging dir (already promoted / none on
// this dataDir) is skipped, and a pre-existing destination (concurrent completer / retry) is treated as
// done. The shard files keep their final-key AAD (set at staged-write time), so post-promote reads
// decrypt correctly. PR1 segment staging.
func (l *LocalShardStore) PromoteLocalStagedShards(bucket, stagingKey, finalKey string) error {
	// promotedAny guards against committing a manifest that references absent
	// shards: every node in a segment's placement holds at least one shard, so a
	// promote that renames nothing AND finds nothing already at the final path
	// means the shards are gone (never written / over-eager cleanup race) — fail
	// rather than silently report success.
	promotedAny := false
	// promote accepts dst as durable: it persists the final-path dir entries up to
	// the dataDir BEFORE the manifest commit (data-before-meta), so a post-commit
	// crash cannot lose the rename and strand an acknowledged object with no final
	// shards. Called on EVERY promoted path — including when dst was already present
	// (rename race / idempotent retry / concurrent completer): an earlier mover may
	// have created dst but crashed/failed before ITS fsync, so this caller must not
	// proceed to commit until it has personally made the link durable. fsync is
	// idempotent (syncDirChain dedups once this process persisted it), so re-syncing
	// an already-durable dst is cheap.
	promote := func(dst string, d int) error {
		if err := l.syncDirChain(dst, l.dataDirs[d]); err != nil {
			return fmt.Errorf("promote staged segment fsync %s: %w", dst, err)
		}
		promotedAny = true
		return nil
	}
	for d := 0; d < len(l.dataDirs); d++ {
		src, err := l.getShardDir(bucket, stagingKey, d)
		if err != nil {
			return err
		}
		dst, err := l.getShardDir(bucket, finalKey, d)
		if err != nil {
			return err
		}
		if _, statErr := os.Stat(src); statErr != nil {
			if os.IsNotExist(statErr) {
				// No staged dir on this dataDir: either it never held a shard for this
				// blob, or a prior promote already moved it. A present destination means
				// the latter (idempotent retry / concurrent completer) — accept it, but
				// still fsync the final chain before counting it as promoted.
				if _, derr := os.Stat(dst); derr == nil {
					if err := promote(dst, d); err != nil {
						return err
					}
				}
				continue
			}
			return statErr
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("promote staged segment: mkdir final parent: %w", err)
		}
		if err := os.Rename(src, dst); err != nil {
			if _, derr := os.Stat(dst); derr == nil {
				// destination already present (idempotent retry / concurrent completer)
				if err := promote(dst, d); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("promote staged segment rename %s -> %s: %w", src, dst, err)
		}
		if err := promote(dst, d); err != nil {
			return err
		}
	}
	if !promotedAny {
		return fmt.Errorf("promote staged segment: no staged or promoted shards for %q -> %q", stagingKey, finalKey)
	}
	return nil
}

func (l *LocalShardStore) ensureShardDir(dir string) error {
	if _, ok := l.dirCache.Load(dir); ok {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		l.dirCache.Delete(dir)
		l.dirDurable.Delete(dir)
		return err
	}
	l.dirCache.Store(dir, struct{}{})
	return nil
}

// ReadLocalShard fetches a shard from the local node's disk and decrypts it via
// the DEK keeper. Returns an error if the shard appears encrypted but at-rest
// encryption is disabled (downgrade guard).
func (l *LocalShardStore) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	path, err := l.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if l.segEnc == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		info, statErr := f.Stat()
		if statErr != nil {
			_ = f.Close()
			return nil, statErr
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		var decoded bytes.Buffer
		if size := info.Size(); size > 0 {
			maxInt := int(^uint(0) >> 1)
			if size <= int64(maxInt) {
				decoded.Grow(int(size))
			}
		}
		if err := eccodec.DecodeEncryptedShard(&decoded, f, l.segEnc, ShardAADFields(bucket, key, shardIdx)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		return decoded.Bytes(), nil
	}
	_ = f.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data := raw
	if eccodec.IsEncodedShard(raw) {
		data, err = eccodec.DecodeShard(raw)
		if err != nil {
			return nil, err
		}
	}
	if encrypt.IsEncryptedBlob(data) {
		return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
	}
	if encrypt.IsLegacyEncryptedBlob(data) {
		return nil, fmt.Errorf("shard carries an unsupported/old encrypted-blob format (pre-XAES); in-place upgrade unsupported")
	}
	// DEK-only service: every shard is GFSENC3-sealed (handled above). A payload
	// that reaches here carries no envelope — reject plaintext fail-closed.
	return nil, fmt.Errorf("%w: shard carries no GFSENC3 envelope and at-rest encryption is DEK-only (plaintext rejected)", eccodec.ErrShardCorrupt)
}

// OpenLocalShard opens a local shard as plaintext. New chunked encrypted shards
// are decrypted chunk-by-chunk; compatibility formats fall back to ReadLocalShard.
func (l *LocalShardStore) OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error) {
	path, err := l.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if l.segEnc == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		r, err := eccodec.NewEncryptedShardReader(f, l.segEnc, ShardAADFields(bucket, key, shardIdx))
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
		return &multiReadCloser{Reader: r, close: func() error {
			var closeErr error
			if closer, ok := r.(io.Closer); ok {
				closeErr = closer.Close()
			}
			if err := f.Close(); closeErr == nil {
				closeErr = err
			}
			return closeErr
		}}, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		if err := rejectLegacyEncodedShardBlob(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		// GFSCRC1-wrapped shard: may be an encrypted single-blob (XAES) that
		// cannot be streamed without decrypting. Delegate to ReadLocalShard to
		// buffer-decrypt; reject plain shards as corruption.
		_ = f.Close()
		data, err := l.ReadLocalShard(bucket, key, shardIdx)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	_ = f.Close()
	data, err := l.ReadLocalShard(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (l *LocalShardStore) ReadLocalShardAt(bucket, key string, shardIdx int, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative shard offset %d", offset)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	path, err := l.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if l.segEnc == nil {
			return 0, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		n, err := eccodec.ReadEncryptedShardRangeAt(f, l.segEnc, ShardAADFields(bucket, key, shardIdx), offset, buf)
		if err != nil {
			return n, fmt.Errorf("decrypt shard range: %w", err)
		}
		return n, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		if err := rejectLegacyEncodedShardBlob(f); err != nil {
			_ = f.Close()
			return 0, err
		}
		// GFSCRC1-wrapped shard: may be an encrypted single-blob (XAES) that
		// cannot be range-read without decrypting. Fall through to OpenLocalShard.
		_ = f.Close()
	} else {
		_ = f.Close()
	}
	r, err := l.OpenLocalShard(bucket, key, shardIdx)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, r, offset); err != nil {
			return 0, err
		}
	}
	return io.ReadFull(r, buf)
}

func (l *LocalShardStore) OpenLocalShardRange(bucket, key string, shardIdx int, offset int64, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, fmt.Errorf("negative shard offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("negative shard length %d", length)
	}
	path, err := l.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if l.segEnc == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		r, err := eccodec.NewEncryptedShardRangeReader(f, l.segEnc, ShardAADFields(bucket, key, shardIdx), offset, length)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard range: %w", err)
		}
		closeFn := f.Close
		if closer, ok := r.(io.Closer); ok {
			closeFn = func() error {
				rerr := closer.Close()
				ferr := f.Close()
				if rerr != nil {
					return rerr
				}
				return ferr
			}
		}
		return &multiReadCloser{Reader: r, close: closeFn}, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		// DEK-only service: every shard is GFSENC3-sealed, so a GFSCRC1 shard here
		// is plaintext (or legacy). Reject fail-closed rather than leak it.
		_ = f.Close()
		return nil, fmt.Errorf("%w: shard carries no GFSENC3 envelope and at-rest encryption is DEK-only (plaintext rejected)", eccodec.ErrShardCorrupt)
	}
	// A plain/short shard: route through OpenLocalShard for proper decode (rejects
	// plaintext) rather than streaming the raw payload — keeps OpenLocalShardRange
	// consistent with ReadLocalShard/ReadLocalShardAt.
	_ = f.Close()

	r, err := l.OpenLocalShard(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		return &multiReadCloser{Reader: io.LimitReader(r, length), close: r.Close}, nil
	}
	return &multiReadCloser{Reader: &skipThenLimitReader{r: r, skip: offset, limit: length}, close: r.Close}, nil
}

// DeleteLocalShards removes every shard for key on the local node (all indices).
// Each candidate directory is validated against its per-dataDir containment root
// before removal — a key containing ".." segments that would escape {dataDir}/{bucket}
// is rejected (same guard as deleteQuorumMetaLocal). This prevents a malformed key
// from a decoded qmeta blob or trusted RPC from targeting paths outside the shard root.
func (l *LocalShardStore) DeleteLocalShards(bucket, key string) error {
	for i, dataDir := range l.dataDirs {
		dir := filepath.Join(dataDir, bucket, key)
		// Containment check: mirror the ShardPathUnderDataDir guard used by
		// getShardDir. Use i as a representative shard index for this dataDir.
		if !l.ShardPathUnderDataDir(bucket, i, dir) {
			return fmt.Errorf("delete local shards: key %q escapes shard root", key)
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}
