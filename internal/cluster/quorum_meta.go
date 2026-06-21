package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/directio"
)

// quorumMetaSubDir is the per-dataDir subdirectory where per-node quorum object
// metadata is durably stored. Sibling to shard files on the same device so the
// write rides the same I/O path (same spindle/NVMe).
const quorumMetaSubDir = ".quorum_meta"

const quorumMetaVersionsSubDir = ".quorum_meta_versions"

// quorumMetaWriteTimeout bounds the synchronous quorum meta write. A node that
// does not ack within this window is treated as failed; the write succeeds as
// long as a quorum (K data shards) of nodes acked.
const quorumMetaWriteTimeout = 30 * time.Second

// quorumMetaReadTimeout bounds the peer fan-out read in fetchQuorumMetaFromPeers.
// Shorter than the write timeout: reads are latency-sensitive (GET path).
const quorumMetaReadTimeout = 5 * time.Second

// bucketVersioningEnabled reports whether the PUT targets a versioning-enabled
// bucket. It prefers the context flag set by the coordinator (and carried over
// the forward wire), which is authoritative because the per-group commit
// backend cannot read the replicated bucketver state itself. When the flag is
// absent — an in-process DistributedBackend PUT that bypasses the coordinator —
// it falls back to a local versioning read, which is correct in that case
// because the single backend does hold the bucketver state.
func (b *DistributedBackend) bucketVersioningEnabled(ctx context.Context, bucket string) bool {
	if enabled, resolved := bucketVersioningFromContext(ctx); resolved {
		return enabled
	}
	state, err := b.GetBucketVersioning(bucket)
	return err == nil && state == "Enabled"
}

// resolveQuorumMetaEpoch returns the soleauth epoch to fence this quorum-meta
// write with: the context-stamped value (the ORIGINATING node's authoritative
// epoch, set at the S3 edge and carried over the forward wire) when present,
// else the local committed epoch. For a non-forwarded write the two are the
// same node, so this is behavior-neutral; for a forwarded write it makes the
// owner fence against the originator's epoch instead of re-reading its own.
// Error-tolerant: a local read error yields 0 (the fence is off at epoch 0).
func (b *DistributedBackend) resolveQuorumMetaEpoch(ctx context.Context, bucket string) uint32 {
	if e, ok := bucketSoleAuthEpochFromContext(ctx); ok {
		return e
	}
	e, _ := b.GetBucketSoleAuthEpoch(bucket)
	return e
}

func (b *DistributedBackend) writeQuorumMeta(ctx context.Context, cmd PutObjectMetaCmd) error {
	// Internal buckets stay on raft (control-plane; headObjectMeta reads BadgerDB
	// for them). Non-internal user buckets use per-node quorum (data_raft bypass).
	if storage.IsInternalBucket(cmd.Bucket) {
		return b.propose(ctx, CmdPutObjectMeta, cmd)
	}
	// Versioning-enabled buckets need per-version retention: quorum-meta holds a
	// single latest-only record per bucket/key, so without an FSM per-version key
	// (obj:{bucket}/{key}/{versionID}) a GET ?versionId=<old> can never find an
	// overwritten version (404). Persist the per-version metadata via raft first,
	// then continue to the quorum-meta fan-out below for LIST/latest visibility.
	if cmd.VersionID != "" && b.bucketVersioningEnabled(ctx, cmd.Bucket) {
		if err := b.propose(ctx, CmdPutObjectMeta, cmd); err != nil {
			return fmt.Errorf("versioned meta persist: %w", err)
		}
	}
	if b.shardSvc == nil || len(cmd.NodeIDs) == 0 {
		return fmt.Errorf("quorum meta write: no shard service or empty placement")
	}
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	if err != nil {
		return fmt.Errorf("quorum meta write encode: %w", err)
	}
	self := b.currentSelfAddr()
	// Sole-authority epoch for the fence: prefer the context-stamped value (the
	// originating node's authoritative epoch, carried over the forward wire), else
	// the local committed read. Resolved once at the top and threaded to every
	// local leaf + peer send.
	epoch := b.resolveQuorumMetaEpoch(ctx, cmd.Bucket)
	// K-of-N write quorum: ECData nodes must ack. Parity nodes are best-effort.
	// Any node that misses the write can still read via fetchQuorumMetaFromPeers.
	k := int(cmd.ECData)
	if k <= 0 {
		k = 1
	}
	// Per-version blob FIRST, durable and FAIL-CLOSED. The immutable per-version blob
	// in the separate .quorum_meta_versions subtree is the AUTHORITATIVE metadata for a
	// versioned object, so a versioned write is durable only if its per-version blob
	// reaches the K-of-N write quorum. It is written BEFORE the latest-only blob so a
	// per-version failure returns before the latest blob is published — the caller's
	// shard cleanup is then a clean rollback, never a published latest blob left
	// pointing at data the caller is about to delete. Gated on versioning-enabled:
	// non-versioned buckets retain no version history (latest-only blob / off-raft).
	if cmd.VersionID != "" && b.bucketVersioningEnabled(ctx, cmd.Bucket) {
		if verr := b.fanOutPerVersionBlob(ctx, cmd, blob, epoch); verr != nil {
			return verr
		}
	}
	// Latest-only blob (LIST-latest / legacy read fast path), same K-of-N, fail-closed.
	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	latestErr := fanOutQuorumMeta(wctx, cmd.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.writeQuorumMetaLocal(cmd.Bucket, cmd.Key, blob, epoch)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteQuorumMeta(fctx, addr, cmd.Bucket, cmd.Key, blob, epoch)
	})
	if latestErr != nil {
		return latestErr
	}
	return nil
}

// fanOutPerVersionBlob durably writes ONE per-version quorum-meta blob
// (.quorum_meta_versions/{key}/{vid}) to the version's K-of-N placement quorum,
// FAIL-CLOSED. It is the per-version half of writeQuorumMeta (the latest-only blob
// and the raft propose are NOT touched here) and the sole writer of hard-delete
// tombstones (DeleteObjectVersion). The encoded blob and the resolved soleauth
// epoch are passed in so the hot PUT path encodes + resolves the epoch once and
// shares them with the latest-only write.
func (b *DistributedBackend) fanOutPerVersionBlob(ctx context.Context, cmd PutObjectMetaCmd, blob []byte, epoch uint32) error {
	if b.shardSvc == nil || len(cmd.NodeIDs) == 0 {
		return fmt.Errorf("per-version quorum-meta write: no shard service or empty placement")
	}
	self := b.currentSelfAddr()
	k := int(cmd.ECData)
	if k <= 0 {
		k = 1
	}
	verSubpath := path.Join(cmd.Key, cmd.VersionID)
	vctx, vcancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer vcancel()
	if verr := fanOutQuorumMeta(vctx, cmd.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.writeQuorumMetaVersionLocal(cmd.Bucket, verSubpath, blob, epoch)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteQuorumMetaVersion(fctx, addr, cmd.Bucket, verSubpath, blob, epoch)
	}); verr != nil {
		return fmt.Errorf("per-version quorum-meta write %s/%s@%s: %w", cmd.Bucket, cmd.Key, cmd.VersionID, verr)
	}
	return nil
}

// readQuorumMeta reads object metadata from the local quorum store, falling
// back to a peer fan-out when the local file is absent (e.g. parity node that
// missed the K-of-N write). Returns ErrObjectNotFound only when no peer has
// the file; callers then fall through to BadgerDB for pre-Phase-3 objects.
func (b *DistributedBackend) readQuorumMeta(bucket, key string) (*storage.Object, PlacementMeta, error) {
	if b.shardSvc == nil {
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	raw, err := b.readQuorumMetaWinningRaw(bucket, key)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return b.shardSvc.decodeQuorumMetaBlob(raw)
}

// quorumMetaBlobWins reports whether candidate (modA, verA, seqA) beats
// (modB, verB, seqB) in the quorum-meta last-writer-wins comparison. Priority
// order: higher ModTime wins; on an equal ModTime (second granularity — see
// time.Now().Unix() writers) the lexicographically greater VersionID wins; on
// an equal ModTime AND VersionID the higher MetaSeq wins. The VersionID
// tiebreak gives the point-GET merge, the peer fan-out, and scatter-gather LIST
// a single deterministic ordering so they agree on the winner of a same-second
// tie. The MetaSeq tiebreak is the lowest-priority discriminator: genuine
// client writes always differ in ModTime/VersionID, so MetaSeq is only
// consulted when a placement re-write (object relocation) preserves both — it
// is behavior-neutral while every blob carries MetaSeq 0. None of these is a
// recency guarantee at second granularity — only deterministic agreement.
func quorumMetaBlobWins(modA int64, verA string, seqA uint64, modB int64, verB string, seqB uint64) bool {
	if modA != modB {
		return modA > modB
	}
	if verA != verB {
		return verA > verB
	}
	return seqA > seqB
}

// quorumMetaCmdWins reports whether candidate cand strictly beats cur in the
// per-version LWW comparison, with the hard-delete tombstone as the top-priority
// tiebreak: on an otherwise-equal (ModTime, VersionID, MetaSeq) a tombstone
// (IsHardDeleted) beats a non-tombstone, so a hard-deleted version can never lose
// the tie to the stale data blob it replaced (closes the relocation-re-write
// equal-MetaSeq window). For two non-tombstone blobs it is identical to
// quorumMetaBlobWins. Used on the per-version write guard and the read-side
// per-VID dedup, where same-VID tombstone-vs-data comparisons occur.
func quorumMetaCmdWins(cand, cur PutObjectMetaCmd) bool {
	if quorumMetaBlobWins(cand.ModTime, cand.VersionID, cand.MetaSeq, cur.ModTime, cur.VersionID, cur.MetaSeq) {
		return true
	}
	// Not a strict (ModTime,VID,MetaSeq) win. The only remaining way cand wins is
	// the tombstone tiebreak on a FULL tie.
	if cand.ModTime == cur.ModTime && cand.VersionID == cur.VersionID && cand.MetaSeq == cur.MetaSeq {
		return cand.IsHardDeleted && !cur.IsHardDeleted
	}
	return false
}

// readQuorumMetaWinningRaw returns the raw quorum-meta blob that wins the LWW
// comparison for (bucket, key). It is the shared read funnel for both
// readQuorumMeta (decodes to storage.Object) and readQuorumMetaCmd (decodes to
// PutObjectMetaCmd).
//
// Single-generation default (multiGeneration false): local-first fast path —
// return the local blob on hit, fan out to peers only on a local miss. This is
// byte-identical to the legacy readQuorumMeta/readQuorumMetaCmd behavior.
//
// Multi-generation (S7-6): a routed (newest-generation) leader holding a stale
// local copy must not shadow a fresher copy that an add-window split-brain write
// landed in an older generation. So merge the local blob AND the cross-generation
// peer fan-out (fetchQuorumMetaFromPeers already spans every ShardGroups() peer),
// returning the LWW winner. fetchQuorumMetaFromPeers excludes self, so the local
// blob is merged in separately here rather than dropped.
func (b *DistributedBackend) readQuorumMetaWinningRaw(bucket, key string) ([]byte, error) {
	localRaw, localErr := b.shardSvc.readQuorumMetaRaw(bucket, key)
	if localErr != nil && !errors.Is(localErr, storage.ErrObjectNotFound) {
		return nil, localErr
	}

	if !b.multiGeneration.Load() {
		// Local-first fast path (byte-identical to legacy).
		if localErr == nil {
			return localRaw, nil
		}
		raw, ok := b.fetchQuorumMetaFromPeers(bucket, key)
		if !ok {
			return nil, storage.ErrObjectNotFound
		}
		return raw, nil
	}

	// Multi-generation cross-generation LWW merge: combine the local blob with
	// the peer-best blob and pick the winner.
	peerRaw, peerOK := b.fetchQuorumMetaFromPeers(bucket, key)
	switch {
	case localErr == nil && peerOK:
		return b.pickQuorumMetaWinner(localRaw, peerRaw), nil
	case localErr == nil:
		return localRaw, nil
	case peerOK:
		return peerRaw, nil
	default:
		return nil, storage.ErrObjectNotFound
	}
}

// pickQuorumMetaWinner returns whichever of two raw quorum-meta blobs wins the
// LWW comparison (quorumMetaBlobWins). A blob that fails to decode loses to a
// decodable one; if both fail it returns a (the caller already holds both as
// candidate winners, so returning either preserves liveness).
func (b *DistributedBackend) pickQuorumMetaWinner(a, bRaw []byte) []byte {
	cmdA, errA := b.shardSvc.decodeQuorumMetaCmdBlob(a)
	cmdB, errB := b.shardSvc.decodeQuorumMetaCmdBlob(bRaw)
	switch {
	case errA != nil && errB != nil:
		return a
	case errA != nil:
		return bRaw
	case errB != nil:
		return a
	}
	if quorumMetaBlobWins(cmdB.ModTime, cmdB.VersionID, cmdB.MetaSeq, cmdA.ModTime, cmdA.VersionID, cmdA.MetaSeq) {
		return bRaw
	}
	return a
}

// fetchQuorumMetaFromPeers fans out ReadQuorumMeta RPCs to all shard group
// peers concurrently and returns the blob with the highest ModTime (LWW).
// Waits for all peers within quorumMetaReadTimeout so that a concurrent PUT
// race resolves to the latest write rather than the fastest responder.
// Returns (nil, false) when no peer has the file or b.shardGroup is nil.
func (b *DistributedBackend) fetchQuorumMetaFromPeers(bucket, key string) ([]byte, bool) {
	if b.shardSvc == nil || b.shardGroup == nil {
		return nil, false
	}
	// Collect unique peer addresses from all shard groups, excluding self.
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	var peers []string
	for _, g := range b.shardGroup.ShardGroups() {
		for _, p := range g.PeerIDs {
			if !seen[p] {
				seen[p] = true
				peers = append(peers, p)
			}
		}
	}
	if len(peers) == 0 {
		return nil, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()

	type peerResult struct {
		data      []byte
		modTime   int64
		versionID string
		metaSeq   uint64
	}
	ch := make(chan peerResult, len(peers))
	var wg sync.WaitGroup
	for _, p := range peers {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := b.shardSvc.resolvePeerAddress(p)
			if err != nil {
				return
			}
			data, err := b.shardSvc.ReadQuorumMetaRaw(ctx, addr, bucket, key)
			if err != nil || len(data) == 0 {
				return
			}
			// Decode ModTime+VersionID for LWW: pick the blob written latest,
			// breaking same-second ties deterministically by VersionID.
			var (
				modTime   int64
				versionID string
				metaSeq   uint64
			)
			if cmd, decErr := b.shardSvc.decodeQuorumMetaCmdBlob(data); decErr == nil {
				modTime = cmd.ModTime
				versionID = cmd.VersionID
				metaSeq = cmd.MetaSeq
			}
			ch <- peerResult{data: data, modTime: modTime, versionID: versionID, metaSeq: metaSeq}
		}()
	}
	go func() { wg.Wait(); close(ch) }()

	// Collect all peer responses; return the LWW winner (highest ModTime, then
	// highest VersionID on a tie). hasBest guards the zero-ModTime case: two
	// blobs with ModTime=0 must still resolve to a deterministic winner.
	var best peerResult
	hasBest := false
	for r := range ch {
		if !hasBest || quorumMetaBlobWins(r.modTime, r.versionID, r.metaSeq, best.modTime, best.versionID, best.metaSeq) {
			best = r
			hasBest = true
		}
	}
	return best.data, hasBest
}

// fanOutQuorumMeta dispatches to every placement node concurrently and returns
// as soon as K acks arrive. Returns an error only when the quorum becomes
// unreachable (more than N-K failures or context cancellation). Errors are
// propagated to the caller — unlike the Phase 0 shadow, failures here fail
// the PUT.
func fanOutQuorumMeta(ctx context.Context, nodes []string, k int, dispatch func(context.Context, string) error) error {
	if k <= 0 {
		k = 1
	}
	n := len(nodes)
	if n < k {
		return fmt.Errorf("quorum meta: placement nodes %d < quorum size %d", n, k)
	}
	results := make(chan error, n)
	for _, node := range nodes {
		node := node
		go func() { results <- dispatch(ctx, node) }()
	}
	var ok, failed int
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err == nil {
				ok++
				if ok >= k {
					return nil
				}
			} else {
				failed++
				if failed > n-k {
					return fmt.Errorf("quorum meta: %d/%d nodes failed, quorum %d unreachable", failed, n, k)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// writeQuorumMetaLocal durably writes the encoded quorum meta blob for
// (bucket, key) under {dataDirs[0]}/.quorum_meta/{bucket}/{key}. One fsync —
// same durability cost as the shard write it co-locates with.
func (s *ShardService) writeQuorumMetaLocal(bucket, key string, data []byte, admittedEpoch uint32) error {
	if len(s.dataDirs) == 0 {
		return fmt.Errorf("quorum meta: no data dir")
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta: key %q escapes root", key)
	}
	// Fence: hold the bucket RLock across the epoch check + the per-target
	// critical section + the FS op. A concurrent flip (T2) takes the bucket
	// WLock, so it cannot interleave; lock order is bucket-RLock (outer) →
	// target-Mutex (inner), no cycle. Dormant at epoch 0.
	bmu := s.bucketSoleAuthLock(bucket)
	bmu.RLock()
	defer bmu.RUnlock()
	if err := s.rejectStaleSoleAuthEpoch(bucket, admittedEpoch); err != nil {
		return err
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("quorum meta mkdir: %w", err)
	}
	// Atomic publish: write to a unique temp file in the same directory, fsync,
	// then rename over the target. An in-place O_TRUNC write exposes a window where
	// the file is 0 bytes (truncated, data not yet written); a concurrent reader of
	// the same key — a same-key overwrite racing a GET/HEAD, or a lingering
	// best-effort quorum write still in flight after fanOutQuorumMeta returned on
	// the k-th ack — would read that empty file and report the object as missing.
	// rename is atomic on a POSIX filesystem, so a reader sees either the previous
	// complete blob or the new one, never a torn one.
	tmp, err := os.CreateTemp(dir, ".qmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("quorum meta tmp create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("quorum meta write: %w", err)
	}
	if err := directio.Sync(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("quorum meta fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("quorum meta tmp close: %w", err)
	}
	// Per-target lock: serializes the (guard-read + rename) critical section so
	// two concurrent writers for the same target cannot both observe the old blob,
	// both conclude their candidate wins, and then race on the rename — which would
	// allow a lower-priority blob to land last and clobber the true LWW winner.
	// Temp create/write/fsync/close above are outside the lock (unique temp name;
	// no contention there). The existing defer os.Remove(tmpName) still fires on
	// the guard-skip path because it was registered before the lock is taken.
	mu := s.quorumMetaTargetLock(target)
	mu.Lock()
	defer mu.Unlock()
	// Write-time LWW guard: a blind-writer (e.g. leaderless backfill) must not
	// clobber a newer on-disk blob. Absent file → no-op (the common case).
	// Use strict "existing beats candidate" (not "candidate does not beat existing")
	// so that read-modify-write mutations (ACL/tags) with equal ModTime/MetaSeq
	// are never suppressed — only a strictly-newer on-disk blob causes a skip.
	if existing, rerr := os.ReadFile(target); rerr == nil {
		if cand, derr := s.decodeQuorumMetaCmdBlob(data); derr == nil {
			if cur, derr2 := s.decodeQuorumMetaCmdBlob(existing); derr2 == nil {
				if quorumMetaBlobWins(cur.ModTime, cur.VersionID, cur.MetaSeq, cand.ModTime, cand.VersionID, cand.MetaSeq) {
					return nil // existing strictly wins — keep it, skip the rename
				}
			}
		}
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("quorum meta rename: %w", err)
	}
	return nil
}

// writeQuorumMetaVersionLocalCore is the lock-free, epoch-free, guard-free FS
// core for per-version blob writes: mkdir + atomic temp+fsync+rename. It assumes
// the caller has already validated the target path and holds whatever lock is
// appropriate (bucket RLock for the normal guarded path; bucket WLock for the
// force-locked restore path).
func (s *ShardService) writeQuorumMetaVersionLocalCore(target string, data []byte) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("quorum meta version mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".qmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("quorum meta version tmp create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("quorum meta version write: %w", err)
	}
	if err := directio.Sync(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("quorum meta version fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("quorum meta version tmp close: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("quorum meta version rename: %w", err)
	}
	return nil
}

// writeQuorumMetaVersionLocal durably writes an immutable per-version quorum-meta
// blob under {dataDirs[0]}/.quorum_meta_versions/{bucket}/{versionSubpath}, where
// versionSubpath is path.Join(key, versionID). It mirrors writeQuorumMetaLocal
// (path-traversal guard + atomic temp+fsync+rename) but uses the separate
// per-version subtree, so {key} is always a directory (holding {vid} files) and
// never collides with the latest-only leaf file in .quorum_meta.
func (s *ShardService) writeQuorumMetaVersionLocal(bucket, versionSubpath string, data []byte, admittedEpoch uint32) error {
	if len(s.dataDirs) == 0 {
		return fmt.Errorf("quorum meta version: no data dir")
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir)
	target := filepath.Join(root, bucket, versionSubpath)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta version: path %q escapes root", versionSubpath)
	}
	// Fence: bucket RLock wraps the epoch check + per-target Mutex + FS op
	// (see writeQuorumMetaLocal). Dormant at epoch 0.
	bmu := s.bucketSoleAuthLock(bucket)
	bmu.RLock()
	defer bmu.RUnlock()
	if err := s.rejectStaleSoleAuthEpoch(bucket, admittedEpoch); err != nil {
		return err
	}
	// Per-target lock: serializes the (guard-read + rename) critical section.
	// Mirrors writeQuorumMetaLocal — see that function for the full rationale.
	mu := s.quorumMetaTargetLock(target)
	mu.Lock()
	defer mu.Unlock()
	// Write-time LWW guard: a blind-writer (e.g. leaderless backfill) must not
	// clobber a newer on-disk blob. Absent file → no-op (the common case).
	// decodeQuorumMetaCmdBlob is a method on *ShardService (the receiver `s` here).
	if existing, rerr := os.ReadFile(target); rerr == nil {
		if cand, derr := s.decodeQuorumMetaCmdBlob(data); derr == nil {
			if cur, derr2 := s.decodeQuorumMetaCmdBlob(existing); derr2 == nil {
				if !quorumMetaCmdWins(cand, cur) {
					return nil // existing wins (or ties) — keep it, skip the rename
				}
			}
		}
	}
	return s.writeQuorumMetaVersionLocalCore(target, data)
}

// readQuorumMetaVersionsLocal returns the decoded per-version blobs for one key
// from .quorum_meta_versions/{bucket}/{key}/{vid}. Absent dir → empty, no error.
func (s *ShardService) readQuorumMetaVersionsLocal(bucket, key string) ([]PutObjectMetaCmd, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir)
	dir := filepath.Join(root, bucket, key)
	if rel, err := filepath.Rel(root, dir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("quorum meta versions: key %q escapes root", key)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]PutObjectMetaCmd, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || isQuorumMetaTempName(e.Name()) {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			continue // tolerate a transient unreadable blob
		}
		cmd, derr := s.decodeQuorumMetaCmdBlob(data)
		if derr != nil {
			continue
		}
		out = append(out, cmd)
	}
	return out, nil
}

// readQuorumMetaVersionsRawLocal reads the RAW per-version blob bytes for
// (bucket, key) WITHOUT decoding — the fail-closed input for the read1
// decode-strict reader. Unlike readQuorumMetaVersionsLocal it returns an error on
// an os.ReadFile failure (a present-but-unreadable blob under sole authority must
// fail closed, not be silently skipped); decoding (and its strictness) is the
// caller's job. Absent dir → (nil, nil). Temp files + the key-escape guard are
// handled identically to readQuorumMetaVersionsLocal.
func (s *ShardService) readQuorumMetaVersionsRawLocal(bucket, key string) ([][]byte, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir)
	dir := filepath.Join(root, bucket, key)
	if rel, err := filepath.Rel(root, dir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("quorum meta versions: key %q escapes root", key)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([][]byte, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || isQuorumMetaTempName(e.Name()) {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			return nil, fmt.Errorf("read per-version blob %s/%s/%s: %w", bucket, key, e.Name(), rerr)
		}
		out = append(out, data)
	}
	return out, nil
}

// ReadQuorumMetaVersionsRaw fans the per-key version list to a remote node and
// returns the RAW blob bytes (no decode) so the caller can decode-strict. Mirrors
// ReadQuorumMetaVersions but does NOT decode/drop — a corrupt blob is served as-is
// (the decode-strictness lives in the read1 reader, not here or on the peer).
func (s *ShardService) ReadQuorumMetaVersionsRaw(ctx context.Context, addr, bucket, key string) ([][]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta versions raw: no transport")
	}
	envb := buildShardEnvelope("ReadQuorumMetaVersionsRaw", bucket, key, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("read quorum meta versions raw from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, err
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote quorum meta versions raw error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil // empty payload = key has no per-version blobs on this node
	}
	blobs, uerr := unpackBlobList(data)
	if uerr != nil {
		return nil, uerr
	}
	return blobs, nil
}

// ReadQuorumMetaVersions fans the per-key version list to a remote placement node.
func (s *ShardService) ReadQuorumMetaVersions(ctx context.Context, addr, bucket, key string) ([]PutObjectMetaCmd, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta versions: no transport")
	}
	envb := buildShardEnvelope("ReadQuorumMetaVersions", bucket, key, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("read quorum meta versions from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, err
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote quorum meta versions error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil // empty payload = key has no per-version blobs on this node
	}
	blobs, uerr := unpackBlobList(data) // NOTE: two-return (quorum_meta.go ~886)
	if uerr != nil {
		return nil, uerr
	}
	out := make([]PutObjectMetaCmd, 0, len(blobs))
	for _, blob := range blobs {
		if cmd, derr := s.decodeQuorumMetaCmdBlob(blob); derr == nil {
			out = append(out, cmd)
		}
	}
	return out, nil
}

// readQuorumMetaVersions unions a key's per-version blobs across ALL placement
// groups (every generation) by fanning ReadQuorumMetaVersions to ShardGroups()
// peers + self, deduped by VersionID. Unconditional all-groups fan-out (mirrors
// fetchQuorumMetaFromPeers) — NOT the multiGeneration-gated fast path.
func (b *DistributedBackend) readQuorumMetaVersions(bucket, key string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	byVID := map[string]PutObjectMetaCmd{}
	// keep the higher-MetaSeq blob for a same-VID replica (mirrors quorumMetaBlobWins's
	// MetaSeq tiebreak so a relocation re-write wins deterministically).
	put := func(c PutObjectMetaCmd) {
		if ex, ok := byVID[c.VersionID]; !ok || c.MetaSeq >= ex.MetaSeq {
			byVID[c.VersionID] = c
		}
	}
	// self
	if local, err := b.shardSvc.readQuorumMetaVersionsLocal(bucket, key); err == nil {
		for _, c := range local {
			put(c)
		}
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	if b.shardGroup != nil {
		for _, g := range b.shardGroup.ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := b.shardSvc.resolvePeerAddress(p)
				if aerr != nil {
					continue
				}
				remote, rerr := b.shardSvc.ReadQuorumMetaVersions(ctx, addr, bucket, key)
				if rerr != nil {
					continue // partial tolerated (see spec predicate boundary)
				}
				for _, c := range remote {
					put(c)
				}
			}
		}
	}
	out := make([]PutObjectMetaCmd, 0, len(byVID))
	for _, c := range byVID {
		out = append(out, c)
	}
	return out, nil
}

// listObjectsPerVersion derives latest-per-key from per-version blobs across ALL
// generation groups (unconditional ShardGroups fan-out, like readQuorumMetaVersions),
// excluding keys whose global-max version is a delete marker. Returns sorted-by-key
// cmds, same shape/contract as scatterGatherList (tombstone-excluded, key-sorted),
// so the three LIST methods stay unchanged downstream. Orphan per-version blobs (a
// node unreachable at hard-delete that rejoins later) could show a rare phantom;
// reconciliation (scrubber removes blobs with no FSM record) is a separate slice.
func (b *DistributedBackend) listObjectsPerVersion(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	byKey := map[string]PutObjectMetaCmd{}
	// Inline max-vid + same-VID MetaSeq>= tiebreak (mirrors readQuorumMetaVersions'
	// put), NOT deriveLatestVersion (which lacks the MetaSeq tiebreak).
	put := func(c PutObjectMetaCmd) {
		if ex, ok := byKey[c.Key]; !ok || c.VersionID > ex.VersionID ||
			(c.VersionID == ex.VersionID && c.MetaSeq >= ex.MetaSeq) {
			byKey[c.Key] = c
		}
	}
	if local, err := b.shardSvc.ScanQuorumMetaVersionsBucket(bucket, prefix); err == nil {
		for _, c := range local {
			put(c)
		}
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	rctx, cancel := context.WithTimeout(ctx, quorumMetaReadTimeout)
	defer cancel()
	if b.shardGroup != nil {
		for _, g := range b.shardGroup.ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := b.shardSvc.resolvePeerAddress(p)
				if aerr != nil {
					continue
				}
				remote, rerr := b.shardSvc.ScanQuorumMetaVersions(rctx, addr, bucket, prefix)
				if rerr != nil {
					continue // partial-tolerant
				}
				for _, c := range remote {
					put(c)
				}
			}
		}
	}
	out := make([]PutObjectMetaCmd, 0, len(byKey))
	for _, c := range byKey {
		if c.IsHardDeleted {
			// The per-key-max version was hard-deleted; the per-key-max scan
			// collapsed the predecessor away, so re-derive the new latest from the
			// full per-version set for this key (cluster-wide, tombstones/markers
			// excluded). Omit the key entirely if nothing live remains.
			if live, ok := b.latestLiveForKey(bucket, c.Key); ok {
				out = append(out, live)
			}
			continue
		}
		if c.IsDeleteMarker {
			continue // global-max is a marker → key deleted, omit from LIST
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// scanQuorumMetaVersionsClusterAll unions EVERY per-version quorum-meta blob in
// the bucket across ALL placement groups (every generation), deduped by
// (Key, VersionID) with a MetaSeq tiebreak. Unlike readQuorumMetaVersions
// (single-key, dedup-by-VID) it is bucket-wide and all-version (no max-per-key
// collapse). Unlike listObjectsPerVersion it is FAIL-CLOSED: a local strict-scan
// error, a peer-address resolution error, or a peer RPC error is returned (NOT
// skipped) — under sole authority a silently-truncated set is data loss. When
// shardGroup == nil (single-node) the result is the local STRICT scan.
func (b *DistributedBackend) scanQuorumMetaVersionsClusterAll(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	type vkey struct{ key, vid string }
	byKey := map[vkey]PutObjectMetaCmd{}
	// keep the higher-MetaSeq blob for a same-(Key,VID) replica (mirrors the
	// MetaSeq tiebreak in readQuorumMetaVersions so a relocation re-write wins).
	put := func(c PutObjectMetaCmd) {
		k := vkey{c.Key, c.VersionID}
		if ex, ok := byKey[k]; !ok || c.MetaSeq >= ex.MetaSeq {
			byKey[k] = c
		}
	}
	// self (STRICT, fail-closed)
	local, lerr := b.shardSvc.scanQuorumMetaVersionsBucketAllStrict(bucket, prefix)
	if lerr != nil {
		return nil, lerr
	}
	for _, c := range local {
		put(c)
	}
	if b.shardGroup == nil {
		out := make([]PutObjectMetaCmd, 0, len(byKey))
		for _, c := range byKey {
			out = append(out, c)
		}
		return out, nil
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	for _, g := range b.shardGroup.ShardGroups() {
		for _, p := range g.PeerIDs {
			if seen[p] {
				continue
			}
			seen[p] = true
			addr, aerr := b.shardSvc.resolvePeerAddress(p)
			if aerr != nil {
				return nil, aerr // fail-closed: cannot enumerate a peer → abort
			}
			remote, rerr := b.shardSvc.ScanQuorumMetaVersionsAll(ctx, addr, bucket, prefix)
			if rerr != nil {
				return nil, rerr // fail-closed: a partial set is silent data loss
			}
			for _, c := range remote {
				put(c)
			}
		}
	}
	out := make([]PutObjectMetaCmd, 0, len(byKey))
	for _, c := range byKey {
		out = append(out, c)
	}
	return out, nil
}

// readQuorumMetaVersion returns the single per-version blob for (bucket, key,
// versionID), found via the all-groups fan-out (NOT local-only) so a reachable
// replica is located even when the local node isn't a placement node. Returns
// (cmd, true, nil) on a hit, (zero, false, nil) on a miss.
func (b *DistributedBackend) readQuorumMetaVersion(bucket, key, versionID string) (PutObjectMetaCmd, bool, error) {
	cmds, err := b.readQuorumMetaVersions(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, false, err
	}
	for _, c := range cmds {
		if c.VersionID == versionID {
			return c, true, nil
		}
	}
	return PutObjectMetaCmd{}, false, nil
}

// readQuorumMetaVersionsDecodeStrict is the read1 soleauth=on per-key reader:
// DECODE-strict but availability-TOLERANT. A blob that fails to decode anywhere it
// is SERVED (self, or any reachable peer) fails the whole read closed — so a
// corrupt latest (max-VID) blob can NEVER be silently dropped and resurrect an
// older live version (the read1 resurrection window). An unreachable / un-upgraded
// / disk-erroring peer is TOLERATED (skipped) — it resurrects nothing (its blobs
// are simply absent from the candidate set; the residual under-replication-AND-
// unreachable window is bounded by the minReader=2 read floor). Self is always
// read strict (local read failure → fail closed).
//
// Distinct from the tolerant readQuorumMetaVersions, which silently drops per-blob
// decode failures and is correct for the off-path / non-sole-authority consumers.
func (b *DistributedBackend) readQuorumMetaVersionsDecodeStrict(bucket, key string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	var rawBlobs [][]byte
	// self — strict local read (a present-but-unreadable blob fails closed;
	// decode happens once, strictly, in the loop below).
	selfRaw, err := b.shardSvc.readQuorumMetaVersionsRawLocal(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("decode-strict version read %s/%s: %w", bucket, key, err)
	}
	rawBlobs = append(rawBlobs, selfRaw...)
	// peers — all-groups fan-out; tolerate an unreachable peer (a corrupt blob is
	// caught wherever it IS served, below).
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	if b.shardGroup != nil {
		for _, g := range b.shardGroup.ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := b.shardSvc.resolvePeerAddress(p)
				if aerr != nil {
					continue // availability-tolerant
				}
				peerRaw, rerr := b.shardSvc.ReadQuorumMetaVersionsRaw(ctx, addr, bucket, key)
				if rerr != nil {
					continue // unreachable / un-upgraded / peer read error → tolerate
				}
				rawBlobs = append(rawBlobs, peerRaw...)
			}
		}
	}
	// strict decode + dedup (keep the higher-MetaSeq blob for a same-VID replica,
	// mirroring readQuorumMetaVersions).
	byVID := map[string]PutObjectMetaCmd{}
	for _, blob := range rawBlobs {
		cmd, derr := b.shardSvc.decodeQuorumMetaCmdBlob(blob)
		if derr != nil {
			// A served blob we cannot decode: its VID is unknown, so we cannot rule
			// out that it WAS the authoritative latest. Fail closed.
			return nil, fmt.Errorf("decode-strict version read %s/%s: %w", bucket, key, derr)
		}
		if ex, ok := byVID[cmd.VersionID]; !ok || cmd.MetaSeq >= ex.MetaSeq {
			byVID[cmd.VersionID] = cmd
		}
	}
	out := make([]PutObjectMetaCmd, 0, len(byVID))
	for _, c := range byVID {
		out = append(out, c)
	}
	return out, nil
}

// readQuorumMetaVersionDecodeStrict is the specific-version twin of
// readQuorumMetaVersion built on the decode-strict reader: a corrupt SIBLING
// version under the same key fails the read closed (the version set is
// untrustworthy if any blob under the key is undecodable).
func (b *DistributedBackend) readQuorumMetaVersionDecodeStrict(bucket, key, versionID string) (PutObjectMetaCmd, bool, error) {
	cmds, err := b.readQuorumMetaVersionsDecodeStrict(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, false, err
	}
	for _, c := range cmds {
		if c.VersionID == versionID {
			return c, true, nil
		}
	}
	return PutObjectMetaCmd{}, false, nil
}

// deriveLatestVersion returns the max-VersionID blob and whether the object's
// latest state is live (false = no versions OR latest is a delete marker).
// Hard-delete tombstones (IsHardDeleted) are skipped as if the version were not
// present, so a hard-delete of the current latest version correctly falls through
// to the live predecessor instead of reporting the whole key as gone.
func deriveLatestVersion(cmds []PutObjectMetaCmd) (PutObjectMetaCmd, bool) {
	var latest PutObjectMetaCmd
	found := false
	for _, c := range cmds {
		if c.IsHardDeleted {
			continue // hard-delete tombstone: version is gone, never the latest
		}
		if !found || c.VersionID > latest.VersionID {
			latest = c
			found = true
		}
	}
	if !found || latest.IsDeleteMarker {
		return PutObjectMetaCmd{}, false
	}
	return latest, true
}

// dropHardDeletedVersions returns cmds with hard-delete tombstones removed. Apply
// at the read / list / scan CONSUMERS so a hard-deleted version is fully excluded
// (never resurrected). It is correct to drop after the per-VID dedup because a
// tombstone is written with MetaSeq = existing+1, so it always wins the same-VID
// dedup over the data blob it replaced (and the per-version write guard keeps only
// the tombstone on disk). The low-level scanners deliberately KEEP tombstones so
// the orphan walker can still reconcile/GC them.
func dropHardDeletedVersions(cmds []PutObjectMetaCmd) []PutObjectMetaCmd {
	out := make([]PutObjectMetaCmd, 0, len(cmds))
	for _, c := range cmds {
		if c.IsHardDeleted {
			continue
		}
		out = append(out, c)
	}
	return out
}

// latestLiveForKey resolves the live latest version for a single key from the full
// cluster-wide per-version set (tombstones and delete markers excluded via
// deriveLatestVersion). Used by listObjectsPerVersion when a key's per-key-max scan
// collapsed to a hard-delete tombstone: the predecessor was collapsed away, so the
// new latest must be re-derived from all versions of that key.
func (b *DistributedBackend) latestLiveForKey(bucket, key string) (PutObjectMetaCmd, bool) {
	cmds, err := b.readQuorumMetaVersions(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, false
	}
	return deriveLatestVersion(cmds)
}

// isQuorumMetaTempName reports whether a directory entry is an in-flight
// atomic-publish temp file (os.CreateTemp(dir, ".qmeta-*.tmp") above). Store
// walkers MUST skip these: the temp lives in the same directory as its rename
// target and contains a complete, decodable meta blob, so a walker that treats
// every file as {bucket}/{key} would fabricate an object keyed by the temp
// name (e.g. ".qmeta-123.tmp" with shard key ".qmeta-123.tmp/segments/<id>")
// whenever a scan races an in-flight write — phantom "missing local shard"
// reports and doomed repairs.
func isQuorumMetaTempName(name string) bool {
	return strings.HasPrefix(name, ".qmeta-") && strings.HasSuffix(name, ".tmp")
}

// readQuorumMetaRaw reads the raw quorum meta blob for (bucket, key) from the
// local filesystem. Returns (nil, ErrObjectNotFound) when the file is absent.
func (s *ShardService) readQuorumMetaRaw(bucket, key string) ([]byte, error) {
	if len(s.dataDirs) == 0 {
		return nil, storage.ErrObjectNotFound
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("quorum meta read raw: key %q escapes root", key)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("quorum meta read raw: %w", err)
	}
	return data, nil
}

// decodeQuorumMetaBlob decodes a raw quorum meta blob into storage.Object and
// PlacementMeta. Used by both the local-read and peer-fallback paths.
func (s *ShardService) decodeQuorumMetaBlob(data []byte) (*storage.Object, PlacementMeta, error) {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta decode command: %w", err)
	}
	if cmd.Type != CmdPutObjectMeta {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta: unexpected command type %d", cmd.Type)
	}
	putCmd, err := decodePutObjectMetaCmd(cmd.Data)
	if err != nil {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta decode put cmd: %w", err)
	}
	obj, placement := objectAndPlacementFromCmd(putCmd)
	return obj, placement, nil
}

// objectAndPlacementFromCmd builds a storage.Object and PlacementMeta from a
// decoded PutObjectMetaCmd. Factored out of decodeQuorumMetaBlob so the
// per-version read hooks (headObjectMeta/headObjectMetaV) build identical
// objects/placement (layout dispatch + EC reads behave the same).
func objectAndPlacementFromCmd(putCmd PutObjectMetaCmd) (*storage.Object, PlacementMeta) {
	m := buildPutObjectMeta(putCmd)
	obj := &storage.Object{
		Key:              m.Key,
		Size:             m.Size,
		ContentType:      m.ContentType,
		ETag:             m.ETag,
		LastModified:     m.LastModified,
		VersionID:        putCmd.VersionID,
		ACL:              m.ACL,
		UserMetadata:     cloneStringMap(m.UserMetadata),
		SSEAlgorithm:     m.SSEAlgorithm,
		PlacementGroupID: m.PlacementGroupID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		Segments:         append([]storage.SegmentRef(nil), m.Segments...),
		Coalesced:        coalescedRefsToStorage(m.Coalesced),
		IsAppendable:     m.IsAppendable,
		Parts:            m.Parts,
		Tags:             append([]storage.Tag(nil), m.Tags...),
		// S4-4c: carry the delete-marker flag so versioned reads
		// (GetObjectVersion/HeadObjectVersion) fold a quorum-meta delete marker
		// to 405 MethodNotAllowed instead of trying to read its (absent) body.
		IsDeleteMarker: putCmd.IsDeleteMarker,
	}
	placement := PlacementMeta{
		VersionID:        putCmd.VersionID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		PlacementGroupID: m.PlacementGroupID,
	}
	return obj, placement
}

// readQuorumMetaLocalDecoded reads and decodes the quorum meta blob for
// (bucket, key) from the local store. Returns storage.ErrObjectNotFound if the
// file does not exist.
func (s *ShardService) readQuorumMetaLocalDecoded(bucket, key string) (*storage.Object, PlacementMeta, error) {
	data, err := s.readQuorumMetaRaw(bucket, key)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return s.decodeQuorumMetaBlob(data)
}

// ScanQuorumMetaBucket returns all PutObjectMetaCmd entries (including
// IsDeleteMarker tombstones) stored locally for bucket. prefix is an
// optional key-prefix filter (empty string = return all). Unreadable entries
// are silently skipped. Callers decide whether to filter tombstones.
func (s *ShardService) ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	bucketRoot := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketRoot); os.IsNotExist(err) {
		return nil, nil
	}
	var results []PutObjectMetaCmd
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if isQuorumMetaTempName(d.Name()) {
			return nil // in-flight atomic-publish temp, not a stored key
		}
		key, rerr := filepath.Rel(bucketRoot, path)
		if rerr != nil {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		cmd, qerr := s.readQuorumMetaRawCmd(bucket, key)
		if qerr != nil {
			return nil
		}
		results = append(results, cmd)
		return nil
	})
	return results, err
}

// ScanQuorumMetaVersionsBucket walks .quorum_meta_versions/{bucket}/, decodes
// EVERY version blob, groups by the decoded cmd.Key (authoritative — keys contain
// '/', so a dir can be both a key-leaf and an intermediate dir, e.g. a/b and
// a/b/c.txt; dir structure can't be trusted), and returns the max-VersionID blob
// per key (markers included; the coordinator merge decides exclusion). Cost is
// O(total versions on this node). ADDITIVE: it does NOT replace
// ScanQuorumMetaBucket, whose latest-only consumers (scatterGatherList /
// ScanObjectMetaEntries facts, the ScanQuorumMeta RPC fan-in, scrubbable.go scrub
// records) must keep the latest-only tree — do not reroute them through this.
func (s *ShardService) ScanQuorumMetaVersionsBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	bucketRoot := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir, bucket)
	byKey := map[string]PutObjectMetaCmd{}
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			if os.IsNotExist(werr) {
				return nil
			}
			return werr
		}
		if d.IsDir() || isQuorumMetaTempName(d.Name()) {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil // tolerate a transient unreadable blob
		}
		cmd, derr := s.decodeQuorumMetaCmdBlob(data)
		if derr != nil {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(cmd.Key, prefix) {
			return nil
		}
		if ex, ok := byKey[cmd.Key]; !ok || cmd.VersionID > ex.VersionID ||
			(cmd.VersionID == ex.VersionID && cmd.MetaSeq >= ex.MetaSeq) {
			byKey[cmd.Key] = cmd
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]PutObjectMetaCmd, 0, len(byKey))
	for _, c := range byKey {
		out = append(out, c)
	}
	return out, nil
}

// ScanQuorumMetaVersionsBucketAll walks + decodes + prefix-filters IDENTICALLY to
// ScanQuorumMetaVersionsBucket but is an ADDITIVE all-version enumerator — it
// returns EVERY version blob (cost O(total versions on this node)) instead of the
// per-key max. Consumed by S4c-b snapshot absent-blob purge + S4c-c flag-on LIST
// (NOT yet wired). Do NOT reroute the max-per-key consumers (listObjectsPerVersion)
// through it.
func (s *ShardService) ScanQuorumMetaVersionsBucketAll(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	bucketRoot := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir, bucket)
	out := []PutObjectMetaCmd{}
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			if os.IsNotExist(werr) {
				return nil
			}
			return werr
		}
		if d.IsDir() || isQuorumMetaTempName(d.Name()) {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil // tolerate a transient unreadable blob
		}
		cmd, derr := s.decodeQuorumMetaCmdBlob(data)
		if derr != nil {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(cmd.Key, prefix) {
			return nil
		}
		out = append(out, cmd)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// scanQuorumMetaVersionsBucketAllStrict is the FAIL-CLOSED twin of
// ScanQuorumMetaVersionsBucketAll: it walks every per-version blob in bucket
// and returns an error on the first unreadable or undecodable blob instead of
// silently skipping it. Consumed by the soleauth-on snapshot capture path
// where a skipped-then-recovered blob would be captured-absent then purged.
func (s *ShardService) scanQuorumMetaVersionsBucketAllStrict(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	bucketRoot := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir, bucket)
	out := []PutObjectMetaCmd{}
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			if os.IsNotExist(werr) {
				return nil
			}
			return werr
		}
		if d.IsDir() || isQuorumMetaTempName(d.Name()) {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return fmt.Errorf("strict version scan: read %s: %w", path, rerr)
		}
		cmd, derr := s.decodeQuorumMetaCmdBlob(data)
		if derr != nil {
			return fmt.Errorf("strict version scan: decode %s: %w", path, derr)
		}
		if prefix != "" && !strings.HasPrefix(cmd.Key, prefix) {
			return nil
		}
		out = append(out, cmd)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ScanQuorumMetaVersions fans the per-version bucket walk to a remote node and
// returns its per-key max-VersionID PutObjectMetaCmds (markers included).
func (s *ShardService) ScanQuorumMetaVersions(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("scan quorum meta versions: no transport")
	}
	envb := buildShardEnvelope("ScanQuorumMetaVersions", bucket, prefix, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("scan quorum meta versions from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("unmarshal scan quorum meta versions response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote scan quorum meta versions error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil
	}
	blobs, uerr := unpackBlobList(data) // NOTE: two-return
	if uerr != nil {
		return nil, fmt.Errorf("unpack scan quorum meta versions response: %w", uerr)
	}
	out := make([]PutObjectMetaCmd, 0, len(blobs))
	for _, blob := range blobs {
		if cmd, derr := s.decodeQuorumMetaCmdBlob(blob); derr == nil {
			out = append(out, cmd)
		}
	}
	return out, nil
}

// ScanQuorumMetaVersionsAll queries one peer for EVERY per-version quorum-meta
// blob under the bucket (no max-per-key collapse), via the ScanQuorumMetaVersionsAll
// RPC. Mirrors ScanQuorumMetaVersions but returns all versions. FAIL-CLOSED: a peer
// "Error" reply — including an un-upgraded peer that doesn't know the msgType — is
// fatal; consumers of the all-version enumeration must NOT degrade to a partial
// result (a missed orphan blob / version would be unsafe), unlike the partial-
// tolerant max-per-key read.
func (s *ShardService) ScanQuorumMetaVersionsAll(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("scan quorum meta versions all: no transport")
	}
	envb := buildShardEnvelope("ScanQuorumMetaVersionsAll", bucket, prefix, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("scan quorum meta versions all from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("unmarshal scan quorum meta versions all response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote scan quorum meta versions all error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil
	}
	blobs, uerr := unpackBlobList(data)
	if uerr != nil {
		return nil, fmt.Errorf("unpack scan quorum meta versions all response: %w", uerr)
	}
	out := make([]PutObjectMetaCmd, 0, len(blobs))
	for _, blob := range blobs {
		// FAIL-CLOSED end-to-end (codex code-gate [P1]): a per-entry decode
		// failure (in-transit corruption, decode-version skew) must NOT be
		// silently skipped — that would return a partial authoritative version
		// list, omitting versions from a soleauth-on listing. (The per-key-max
		// tolerant clients keep skipping by design; this all-version path is the
		// sole-authority enumerator and must fail closed.)
		cmd, derr := s.decodeQuorumMetaCmdBlob(blob)
		if derr != nil {
			return nil, fmt.Errorf("decode scan quorum meta versions all response from %s: %w", addr, derr)
		}
		out = append(out, cmd)
	}
	return out, nil
}

// deleteQuorumMetaLocal removes the local quorum meta file for (bucket, key).
// Called by deleteObjectWithMarker after the raft CmdDeleteObject commit so
// subsequent reads fall through to BadgerDB and find the delete marker.
// Errors are silently ignored: the raft marker is the source of truth; a
// stale quorum meta file is handled on the next read.
func (s *ShardService) deleteQuorumMetaLocal(bucket, key string, admittedEpoch uint32) error {
	if len(s.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta delete: key %q escapes root", key)
	}
	// Fence: bucket RLock wraps the epoch check + the FS remove (see
	// writeQuorumMetaLocal). Dormant at epoch 0.
	bmu := s.bucketSoleAuthLock(bucket)
	bmu.RLock()
	defer bmu.RUnlock()
	if err := s.rejectStaleSoleAuthEpoch(bucket, admittedEpoch); err != nil {
		return err
	}
	err = os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("quorum meta delete: %w", err)
	}
	return nil
}

// deleteQuorumMetaVersionLocalCore is the lock-free, epoch-free FS core for
// per-version blob removal. Absent file is not an error (idempotent). The caller
// is responsible for holding the appropriate lock before calling.
func (s *ShardService) deleteQuorumMetaVersionLocalCore(target string) error {
	err := os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("quorum meta version delete: %w", err)
	}
	return nil
}

// deleteQuorumMetaVersionLocal removes the local per-version blob for
// (bucket, key, versionID) under .quorum_meta_versions/{bucket}/{key}/{vid}.
// Absent file is not an error (idempotent). Mirrors deleteQuorumMetaLocal.
func (s *ShardService) deleteQuorumMetaVersionLocal(bucket, key, versionID string, admittedEpoch uint32) error {
	if len(s.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir)
	target := filepath.Join(root, bucket, key, versionID)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta version delete: path %q/%q escapes root", key, versionID)
	}
	// Fence: bucket RLock wraps the epoch check + the FS remove (see
	// writeQuorumMetaLocal). Dormant at epoch 0.
	bmu := s.bucketSoleAuthLock(bucket)
	bmu.RLock()
	defer bmu.RUnlock()
	if err := s.rejectStaleSoleAuthEpoch(bucket, admittedEpoch); err != nil {
		return err
	}
	return s.deleteQuorumMetaVersionLocalCore(target)
}

// DeleteQuorumMetaVersion removes a per-version blob on a remote placement node.
// Mirrors DeleteQuorumMeta; the receiver runs deleteQuorumMetaVersionLocal.
// versionSubpath rides the envelope key field as path.Join(key, versionID).
func (s *ShardService) DeleteQuorumMetaVersion(ctx context.Context, addr, bucket, key, versionID string, admittedEpoch uint32) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta version: no transport")
	}
	envb := buildShardEnvelope("DeleteQuorumMetaVersion", bucket, path.Join(key, versionID), 0, nil, admittedEpoch)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("delete quorum meta version on %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta version delete response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote quorum meta version delete error from %s", addr)
	}
	return nil
}

// deleteQuorumMetaVersionQuorum deletes a version's blob on every placement node.
// FAIL-CLOSED (unlike deleteQuorumMetaQuorum): returns the first error so a
// lingering blob on a missed node cannot resurface the deleted version via
// derive-by-scan.
func (b *DistributedBackend) deleteQuorumMetaVersionQuorum(ctx context.Context, bucket, key, versionID string, nodeIDs []string) error {
	if b.shardSvc == nil {
		return nil
	}
	self := b.currentSelfAddr()
	epoch, _ := b.GetBucketSoleAuthEpoch(bucket)
	dctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	var firstErr error
	for _, node := range nodeIDs {
		var err error
		if node == self {
			err = b.shardSvc.deleteQuorumMetaVersionLocal(bucket, key, versionID, epoch)
		} else if addr, rerr := b.shardSvc.resolvePeerAddress(node); rerr == nil {
			err = b.shardSvc.DeleteQuorumMetaVersion(dctx, addr, bucket, key, versionID, epoch)
		} else {
			err = rerr
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// decodeQuorumMetaCmdBlob decodes a raw quorum meta blob to a PutObjectMetaCmd.
func (s *ShardService) decodeQuorumMetaCmdBlob(data []byte) (PutObjectMetaCmd, error) {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("quorum meta decode raw: %w", err)
	}
	if cmd.Type != CmdPutObjectMeta {
		return PutObjectMetaCmd{}, fmt.Errorf("quorum meta read raw: unexpected command type %d", cmd.Type)
	}
	return decodePutObjectMetaCmd(cmd.Data)
}

// readQuorumMetaRawCmd reads and decodes the PutObjectMetaCmd from the local
// quorum meta store. Returns storage.ErrObjectNotFound if the file is absent.
// Use DistributedBackend.readQuorumMetaCmd when peer fallback is needed.
func (s *ShardService) readQuorumMetaRawCmd(bucket, key string) (PutObjectMetaCmd, error) {
	data, err := s.readQuorumMetaRaw(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return s.decodeQuorumMetaCmdBlob(data)
}

// ReadQuorumMetaRaw fetches the raw quorum meta blob from a remote peer via
// the shard transport. Returns (nil, nil) when the peer has no file for the
// object (not-found is not an error — caller treats nil data as a miss).
func (s *ShardService) ReadQuorumMetaRaw(ctx context.Context, addr, bucket, key string) ([]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta read: no transport")
	}
	envb := buildShardEnvelope("ReadQuorumMeta", bucket, key, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("read quorum meta from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("unmarshal quorum meta read response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote quorum meta read error from %s", addr)
	}
	return data, nil
}

// readQuorumMetaCmd is the DistributedBackend-level read for PutObjectMetaCmd,
// with peer fan-out fallback when the local quorum meta file is absent.
// Used by SetObjectACLPropose, SetObjectTagsPropose, and AppendObject migration.
func (b *DistributedBackend) readQuorumMetaCmd(bucket, key string) (PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return PutObjectMetaCmd{}, storage.ErrObjectNotFound
	}
	raw, err := b.readQuorumMetaWinningRaw(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return b.shardSvc.decodeQuorumMetaCmdBlob(raw)
}

// WriteQuorumMeta sends the quorum meta blob to a remote placement node via the
// shard transport (mirrors WriteShadowMeta but routes to the primary handler).
func (s *ShardService) WriteQuorumMeta(ctx context.Context, addr, bucket, key string, data []byte, admittedEpoch uint32) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta: no transport")
	}
	envb := buildShardEnvelope("WriteQuorumMeta", bucket, key, 0, data, admittedEpoch)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("write quorum meta to %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote quorum meta error from %s", addr)
	}
	return nil
}

// WriteQuorumMetaVersion sends an immutable per-version quorum-meta blob to a
// remote placement node. versionSubpath is path.Join(key, versionID); it rides
// the existing shard envelope's key field (no schema change) and the receiver
// writes it under the .quorum_meta_versions subtree.
func (s *ShardService) WriteQuorumMetaVersion(ctx context.Context, addr, bucket, versionSubpath string, data []byte, admittedEpoch uint32) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta version: no transport")
	}
	envb := buildShardEnvelope("WriteQuorumMetaVersion", bucket, versionSubpath, 0, data, admittedEpoch)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("write quorum meta version to %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta version response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote quorum meta version error from %s", addr)
	}
	return nil
}

// DeleteQuorumMeta removes the quorum-meta replica for (bucket, key) on a remote
// placement node. Mirrors WriteQuorumMeta; the receiver runs deleteQuorumMetaLocal.
func (s *ShardService) DeleteQuorumMeta(ctx context.Context, addr, bucket, key string, admittedEpoch uint32) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta: no transport")
	}
	envb := buildShardEnvelope("DeleteQuorumMeta", bucket, key, 0, nil, admittedEpoch)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("delete quorum meta on %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta delete response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote quorum meta delete error from %s", addr)
	}
	return nil
}

// deleteQuorumMetaQuorum removes the quorum-meta replica for (bucket, key) on
// EVERY placement node (self locally, peers via the DeleteQuorumMeta RPC).
// Best-effort: a failed peer delete is logged-by-omission and tolerated — the
// scrubber's orphan sweep and the next write reconcile a residual replica, and
// the BadgerDB record (read fallback) is authoritative either way. nodeIDs is
// the placement set captured from the object's quorum-meta before migration; an
// empty set falls back to a local-only delete (object was BadgerDB-only).
func (b *DistributedBackend) deleteQuorumMetaQuorum(ctx context.Context, bucket, key string, nodeIDs []string) {
	if b.shardSvc == nil {
		return
	}
	epoch, _ := b.GetBucketSoleAuthEpoch(bucket)
	if len(nodeIDs) == 0 {
		_ = b.shardSvc.deleteQuorumMetaLocal(bucket, key, epoch)
		return
	}
	self := b.currentSelfAddr()
	dctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	for _, node := range nodeIDs {
		if node == self {
			_ = b.shardSvc.deleteQuorumMetaLocal(bucket, key, epoch)
			continue
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			continue
		}
		_ = b.shardSvc.DeleteQuorumMeta(dctx, addr, bucket, key, epoch)
	}
}

// IterQuorumMetaECShardTargets walks all quorum meta files under
// {dataDirs[0]}/.quorum_meta/ and emits ECShardScanTarget entries for every
// object whose segments or coalesced refs carry EC placement. Used by
// ShardPlacementMonitor.Scan to cover Phase 3 objects that bypass BadgerDB.
//
// fn returning a non-nil error stops iteration.
func (s *ShardService) IterQuorumMetaECShardTargets(fn func(ECShardScanTarget) error) error {
	if len(s.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	// Walk: root/{bucket}/{key} — exactly two levels deep.
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			return nil
		}
		if isQuorumMetaTempName(d.Name()) {
			return nil // in-flight atomic-publish temp, not a stored key
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		parts := strings.SplitN(rel, string(filepath.Separator), 2)
		if len(parts) != 2 {
			return nil // not bucket/key shape
		}
		bucket, key := parts[0], parts[1]
		obj, _, qerr := s.readQuorumMetaLocalDecoded(bucket, key)
		if qerr != nil {
			return nil // corrupt or not found; skip
		}
		// Build ECShardScanTarget entries mirroring buildECShardTargets in
		// shard_placement.go, but sourced from the quorum meta store.
		for i := range obj.Segments {
			seg := obj.Segments[i]
			if seg.ECData == 0 || len(seg.NodeIDs) == 0 {
				continue
			}
			if !validateECRefPlacement(seg.ECData, seg.ECParity, seg.NodeIDs) {
				continue
			}
			if ferr := fn(ECShardScanTarget{
				Kind:      ECShardSegment,
				Bucket:    bucket,
				ObjectKey: key,
				VersionID: obj.VersionID,
				ShardKey:  key + "/segments/" + seg.BlobID,
				Placement: PlacementRecord{
					Nodes:       seg.NodeIDs,
					K:           int(seg.ECData),
					M:           int(seg.ECParity),
					StripeBytes: int(seg.StripeBytes),
				},
			}); ferr != nil {
				return ferr
			}
		}
		for i := range obj.Coalesced {
			cs := obj.Coalesced[i]
			if cs.ECData == 0 || len(cs.NodeIDs) == 0 {
				continue
			}
			if !validateECRefPlacement(cs.ECData, cs.ECParity, cs.NodeIDs) {
				continue
			}
			if ferr := fn(ECShardScanTarget{
				Kind:      ECShardCoalesced,
				Bucket:    bucket,
				ObjectKey: key,
				VersionID: obj.VersionID,
				ShardKey:  cs.ShardKey,
				Placement: PlacementRecord{
					Nodes:       cs.NodeIDs,
					K:           int(cs.ECData),
					M:           int(cs.ECParity),
					StripeBytes: int(cs.StripeBytes),
				},
			}); ferr != nil {
				return ferr
			}
		}
		// Non-segmented / non-coalesced EC objects (single-blob phase-3 objects).
		if len(obj.Segments) == 0 && len(obj.Coalesced) == 0 && obj.ECData > 0 && len(obj.NodeIDs) > 0 {
			if ferr := fn(ECShardScanTarget{
				Kind:             ECShardObjectVersion,
				Bucket:           bucket,
				ObjectKey:        key,
				VersionID:        obj.VersionID,
				ECData:           obj.ECData,
				ECParity:         obj.ECParity,
				NodeIDs:          obj.NodeIDs,
				PlacementGroupID: obj.PlacementGroupID,
			}); ferr != nil {
				return ferr
			}
		}
		return nil
	})
}

// packBlobList encodes a slice of blobs as a concatenated length-prefixed stream.
// Each blob is preceded by a 4-byte big-endian length. Used for ScanQuorumMeta RPC.
func packBlobList(blobs [][]byte) []byte {
	var total int
	for _, b := range blobs {
		total += 4 + len(b)
	}
	out := make([]byte, 0, total)
	for _, b := range blobs {
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
		out = append(out, hdr[:]...)
		out = append(out, b...)
	}
	return out
}

// unpackBlobList decodes a packBlobList-encoded byte slice.
func unpackBlobList(data []byte) ([][]byte, error) {
	var out [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("unpack blob list: truncated length prefix")
		}
		n := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < n {
			return nil, fmt.Errorf("unpack blob list: truncated blob (%d bytes, want %d)", len(data), n)
		}
		out = append(out, data[:n])
		data = data[n:]
	}
	return out, nil
}

// ScanQuorumMeta fans out a ScanQuorumMeta RPC to a remote node and returns
// all PutObjectMetaCmds (including tombstones) for the given bucket and prefix.
func (s *ShardService) ScanQuorumMeta(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("scan quorum meta: no transport")
	}
	envb := buildShardEnvelope("ScanQuorumMeta", bucket, prefix, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("scan quorum meta from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("unmarshal scan quorum meta response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote scan quorum meta error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil
	}
	blobs, err := unpackBlobList(data)
	if err != nil {
		return nil, fmt.Errorf("unpack scan quorum meta response: %w", err)
	}
	var cmds []PutObjectMetaCmd
	for _, blob := range blobs {
		cmd, qerr := s.decodeQuorumMetaCmdBlob(blob)
		if qerr == nil {
			cmds = append(cmds, cmd)
		}
	}
	return cmds, nil
}

// scatterGatherList fans out ScanQuorumMeta calls to all shard group peers
// (including self), applies per-key LWW (max ModTime wins), filters
// IsDeleteMarker tombstones, and returns the surviving entries sorted by key.
// ScanObjectMetaEntries scatter-gathers the live (tombstone-filtered) object
// metadata for bucket under prefix and returns each as an ObjectIndexEntry
// carrying the EC placement fields (PlacementGroupID, NodeIDs, ECData, ECParity)
// that ClassifyObjectLayout needs. S4-4d uses it to rebuild admin volume replica
// facts from quorum meta now that the object index is gone.
func (b *DistributedBackend) ScanObjectMetaEntries(ctx context.Context, bucket, prefix string) ([]ObjectIndexEntry, error) {
	cmds, err := b.scatterGatherList(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	entries := make([]ObjectIndexEntry, 0, len(cmds))
	for _, cmd := range cmds {
		entries = append(entries, ObjectIndexEntry{
			Bucket:           cmd.Bucket,
			Key:              cmd.Key,
			VersionID:        cmd.VersionID,
			PlacementGroupID: cmd.PlacementGroupID,
			Size:             cmd.Size,
			ContentType:      cmd.ContentType,
			ETag:             cmd.ETag,
			ModTime:          cmd.ModTime,
			ECData:           cmd.ECData,
			ECParity:         cmd.ECParity,
			NodeIDs:          cmd.NodeIDs,
			IsDeleteMarker:   cmd.IsDeleteMarker,
		})
	}
	return entries, nil
}

func (b *DistributedBackend) scatterGatherList(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{}
	var peerIDs []string
	if b.shardGroup != nil {
		for _, g := range b.shardGroup.ShardGroups() {
			for _, p := range g.PeerIDs {
				if !seen[p] {
					seen[p] = true
					peerIDs = append(peerIDs, p)
				}
			}
		}
	}
	if !seen[self] {
		peerIDs = append(peerIDs, self)
	}
	if len(peerIDs) == 0 {
		entries, err := b.shardSvc.ScanQuorumMetaBucket(bucket, prefix)
		if err != nil {
			return nil, err
		}
		return filterAndSortEntries(entries), nil
	}

	type nodeResult struct {
		entries []PutObjectMetaCmd
	}
	rctx, cancel := context.WithTimeout(ctx, quorumMetaReadTimeout)
	defer cancel()
	ch := make(chan nodeResult, len(peerIDs))
	for _, p := range peerIDs {
		p := p
		go func() {
			if p == self {
				entries, _ := b.shardSvc.ScanQuorumMetaBucket(bucket, prefix)
				ch <- nodeResult{entries: entries}
				return
			}
			addr, aerr := b.shardSvc.resolvePeerAddress(p)
			if aerr != nil {
				ch <- nodeResult{}
				return
			}
			entries, _ := b.shardSvc.ScanQuorumMeta(rctx, addr, bucket, prefix)
			ch <- nodeResult{entries: entries}
		}()
	}

	lww := map[string]PutObjectMetaCmd{}
	for range peerIDs {
		r := <-ch
		for _, e := range r.entries {
			if cur, ok := lww[e.Key]; !ok || quorumMetaBlobWins(e.ModTime, e.VersionID, e.MetaSeq, cur.ModTime, cur.VersionID, cur.MetaSeq) {
				lww[e.Key] = e
			}
		}
	}

	all := make([]PutObjectMetaCmd, 0, len(lww))
	for _, e := range lww {
		all = append(all, e)
	}
	return filterAndSortEntries(all), nil
}

// filterAndSortEntries removes tombstones and sorts by key.
func filterAndSortEntries(entries []PutObjectMetaCmd) []PutObjectMetaCmd {
	out := entries[:0]
	for _, e := range entries {
		if !e.IsDeleteMarker {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
