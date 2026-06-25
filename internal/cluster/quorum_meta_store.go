package cluster

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/storage"
)

// QuorumMetaStore owns the quorum-meta ORCHESTRATION concern carved out of
// DistributedBackend: the K-of-N fan-out write (per-version-blob-before-latest),
// the LWW read merge (local-first fast path / multi-generation cross-generation
// merge), the certainty-aware reclaim read, and the cluster-wide LIST/scan
// scatter-gather. DistributedBackend keeps it as a `qms` field and delegates
// every one of these methods to it; external callers continue to call
// b.writeQuorumMeta / b.readQuorumMetaCmd / ... unchanged (behavior-preserving
// facade), mirroring the LocalShardStore extraction (PR1).
//
// The store touches no DistributedBackend state directly. Its collaborators are
// injected as narrow adapter interfaces (localQuorumMetaStore / quorumMetaPeerRPC)
// plus three tiny accessors (groups / selfAddr / multiGen) and a 1-method
// versioning reader. Today *ShardService satisfies both adapters and the
// DistributedBackend satisfies versioningSource; a future focused
// LocalQuorumMetaStore (PR2) can replace the local adapter without touching this
// orchestration — "the interface is the test surface".
type QuorumMetaStore struct {
	// local is a LIVE accessor for the local-filesystem quorum-meta primitive set
	// (read/write/scan/decode against this node's disk). Like groups, it is an
	// accessor (not a captured value), so a test that reassigns b.shardSvc after
	// construction is read live — no rebuild machinery needed. It returns a TRUE-nil
	// interface when shardSvc is nil (not a non-nil interface wrapping a nil
	// pointer), so the `s.local() == nil` graceful-degrade guards keep firing.
	local func() localQuorumMetaStore
	// peer is a LIVE accessor for the addr-taking peer-RPC quorum-meta set. Same
	// *ShardService as local in production; same true-nil guard.
	peer func() quorumMetaPeerRPC
	// groups is a LIVE accessor for the shard-group topology source. It MUST be an
	// accessor (not a captured value): production wires SetShardGroupSource AFTER
	// SetShardService (boot_phases_storage_runtime.go), so a value captured at
	// qms-build time would be nil forever. The orchestration reads it live and
	// nil-guards it (legacy single-group / solo path) exactly as the inline code did.
	groups func() ShardGroupSource
	// versioning is the narrow control-plane reader used only by
	// bucketVersioningEnabled. A 1-method interface (not *DistributedBackend
	// wholesale) so the qms→versioning→DistributedBackend edge cannot re-enter the
	// store. GetBucketVersioning reads meta-raft bucket config; it never calls back
	// into the quorum-meta store.
	versioning versioningSource
	// selfAddr returns this node's address. currentSelfAddr is a DistributedBackend
	// method (it reads the topology snapshot), NOT a ShardService one, so it is
	// injected as a closure rather than via the peer adapter.
	selfAddr func() string
	// multiGen points at DistributedBackend.multiGeneration (an atomic.Bool). It is
	// a POINTER so a later SetMultiGeneration is observed live; the atomic value is
	// never copied.
	multiGen *atomic.Bool
}

// localQuorumMetaStore is the local-filesystem quorum-meta primitive set the
// orchestration depends on. LocalQuorumMetaStore satisfies it. Every method the moved
// orchestration calls on the local store is enumerated here — a missing method
// fails the var-_ compile guard below.
type localQuorumMetaStore interface {
	writeQuorumMetaLocal(bucket, key string, data []byte) error
	writeQuorumMetaLocalWithResult(bucket, key string, data []byte) (quorumMetaLocalWriteResult, error)
	writeQuorumMetaVersionLocal(bucket, versionSubpath string, data []byte) error
	readQuorumMetaRaw(bucket, key string) ([]byte, error)
	readQuorumMetaVersionsLocal(bucket, key string) ([]PutObjectMetaCmd, error)
	readQuorumMetaVersionsRawLocal(bucket, key string) ([][]byte, error)
	ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error)
	scanQuorumMetaBucketStrict(bucket string) ([]PutObjectMetaCmd, error)
	ScanQuorumMetaVersionsBucket(bucket, prefix string) ([]PutObjectMetaCmd, error)
	scanQuorumMetaVersionsBucketAllStrict(bucket, prefix string) ([]PutObjectMetaCmd, error)
	deleteQuorumMetaLocal(bucket, key string) error
	rollbackQuorumMetaLocalIfMatch(bucket, key string, expected []byte, previous []byte, hadPrevious bool) error
	decodeQuorumMetaBlob(data []byte) (*storage.Object, PlacementMeta, error)
	decodeQuorumMetaCmdBlob(data []byte) (PutObjectMetaCmd, error)
}

// quorumMetaPeerRPC is the addr-taking peer-RPC quorum-meta primitive set the
// orchestration fans out to. *ShardService satisfies it today. resolvePeerAddress
// maps a placement node ID to a dial address; the rest are the shard-transport
// quorum-meta RPCs.
type quorumMetaPeerRPC interface {
	resolvePeerAddress(peer string) (string, error)
	WriteQuorumMeta(ctx context.Context, addr, bucket, key string, data []byte) error
	WriteQuorumMetaVersion(ctx context.Context, addr, bucket, versionSubpath string, data []byte) error
	ReadQuorumMetaRaw(ctx context.Context, addr, bucket, key string) ([]byte, error)
	ReadQuorumMetaVersions(ctx context.Context, addr, bucket, key string) ([]PutObjectMetaCmd, error)
	ReadQuorumMetaVersionsRaw(ctx context.Context, addr, bucket, key string) ([][]byte, error)
	ScanQuorumMeta(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error)
	ScanQuorumMetaVersions(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error)
	ScanQuorumMetaVersionsAll(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error)
	DeleteQuorumMeta(ctx context.Context, addr, bucket, key string) error
}

// versioningSource is the narrow control-plane reader used by
// bucketVersioningEnabled. DistributedBackend satisfies it. Keeping it to one
// method prevents the store from holding *DistributedBackend wholesale (which
// would re-introduce the god-struct coupling the extraction removes).
type versioningSource interface {
	GetBucketVersioning(bucket string) (string, error)
}

// Compile guards: the production collaborators satisfy the adapter interfaces.
var (
	_ localQuorumMetaStore = (*LocalQuorumMetaStore)(nil)
	_ quorumMetaPeerRPC    = (*ShardService)(nil)
	_ versioningSource     = (*DistributedBackend)(nil)
)

// buildQuorumMetaStore wires an orchestration store whose adapter fields are all
// LIVE accessors closing over b: local/peer/groups read b.shardSvc / b.shardGroup
// every call, selfAddr is the backend's bound method, multiGen points at the
// backend's atomic.Bool (never copied), and versioning is the backend (narrow
// 1-method use). Because every field reads through b, the store is built ONCE in
// SetShardService and never needs rebuilding even when a test reassigns b.shardSvc
// after construction. The local/peer accessors return a LITERAL nil interface when
// shardSvc is nil (not a non-nil interface wrapping a nil *ShardService), so the
// `s.local() == nil` graceful-degrade guards keep firing exactly as the inline
// `b.shardSvc == nil` checks did.
func (b *DistributedBackend) buildQuorumMetaStore() *QuorumMetaStore {
	return &QuorumMetaStore{
		local: func() localQuorumMetaStore {
			if b.shardSvc == nil || b.shardSvc.qmeta == nil {
				return nil
			}
			return b.shardSvc.qmeta
		},
		peer: func() quorumMetaPeerRPC {
			if b.shardSvc == nil {
				return nil
			}
			return b.shardSvc
		},
		groups:     func() ShardGroupSource { return b.shardGroup },
		versioning: b,
		selfAddr:   b.currentSelfAddr,
		multiGen:   &b.multiGeneration,
	}
}

// qmsOrBuild returns the cached orchestration store, or builds a fresh one when
// b.qms is nil. Production sets b.qms once in SetShardService and hits the cache;
// struct-literal test backends that skip SetShardService get a freshly built store
// (its live accessors read the struct's shardSvc/shardGroup directly, so no
// staleness — that is why no builtFrom identity check is needed). The fresh store
// is stateless and never written back to b.qms, so it cannot race the concurrent
// fan-out goroutines the facade methods spawn.
func (b *DistributedBackend) qmsOrBuild() *QuorumMetaStore {
	if b.qms != nil {
		return b.qms
	}
	return b.buildQuorumMetaStore()
}

// bucketVersioningEnabled reports whether the PUT targets a versioning-enabled
// bucket. It prefers the context flag set by the coordinator (and carried over
// the forward wire), which is authoritative because the per-group commit
// backend cannot read the replicated bucketver state itself. When the flag is
// absent — an in-process DistributedBackend PUT that bypasses the coordinator —
// it falls back to a local versioning read, which is correct in that case
// because the single backend does hold the bucketver state.
func (s *QuorumMetaStore) bucketVersioningEnabled(ctx context.Context, bucket string) bool {
	if enabled, resolved := bucketVersioningFromContext(ctx); resolved {
		return enabled
	}
	state, err := s.versioning.GetBucketVersioning(bucket)
	return err == nil && state == "Enabled"
}

func (s *QuorumMetaStore) writeQuorumMeta(ctx context.Context, cmd PutObjectMetaCmd) error {
	// Blob-primary (raft-free): for versioning-enabled buckets the per-version blob
	// (written below via fanOutPerVersionBlob) is the BLOB AUTHORITY for object
	// metadata — there is no raft propose for object metadata. Reads, LIST, the orphan GCs,
	// and DEK rewrap all derive from the per-version blobs; the latest-only blob is
	// the LIST-latest fast path. The conditional-PUT (ExpectedETag) CAS that used to
	// be enforced inside the propose is dropped for versioned objects: the only
	// caller that sets ExpectedETag is object relocation, which already relies on the
	// blob LWW (preserve-old-ModTime), not the FSM CAS. Internal buckets keep their
	// propose above.
	if s.local() == nil || len(cmd.NodeIDs) == 0 {
		return fmt.Errorf("quorum meta write: no shard service or empty placement")
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	if err != nil {
		return fmt.Errorf("quorum meta write encode: %w", err)
	}
	self := s.selfAddr()
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
	if cmd.VersionID != "" && s.bucketVersioningEnabled(ctx, cmd.Bucket) {
		if verr := s.fanOutPerVersionBlob(ctx, cmd, blob); verr != nil {
			return verr
		}
	}
	// Latest-only blob (LIST-latest / legacy read fast path), same K-of-N, fail-closed.
	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	writeLocal := func() error { return s.local().writeQuorumMetaLocal(cmd.Bucket, cmd.Key, blob) }
	localWrite := quorumMetaLocalWriteResult{}
	writeLocalCAS := func() error {
		result, err := s.local().writeQuorumMetaLocalWithResult(cmd.Bucket, cmd.Key, blob)
		localWrite = result
		return err
	}
	cleanupLocal := func() error {
		if !localWrite.applied {
			return nil
		}
		return s.local().rollbackQuorumMetaLocalIfMatch(cmd.Bucket, cmd.Key, blob, localWrite.previous, localWrite.hadPrevious)
	}
	writePeer := func(fctx context.Context, node string) error {
		addr, rerr := s.peer().resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return s.peer().WriteQuorumMeta(fctx, addr, cmd.Bucket, cmd.Key, blob)
	}
	var latestErr error
	if cmd.MetaSeqCAS {
		// CAS write (append/coalesce): the next same-owner RMW reads this blob
		// owner-local-first, so the owner-local copy MUST be durable before this
		// returns (BUG-2). Owner-local-first guarantees that while preserving the
		// exact N-K peer failure budget. LWW writers keep the plain fan-out below.
		latestErr = fanOutQuorumMetaOwnerLocalFirst(wctx, cmd.NodeIDs, self, k, writeLocalCAS, cleanupLocal, writePeer)
	} else {
		latestErr = fanOutQuorumMeta(wctx, cmd.NodeIDs, k, func(fctx context.Context, node string) error {
			if node == self {
				return writeLocal()
			}
			return writePeer(fctx, node)
		})
	}
	if latestErr != nil {
		return latestErr
	}
	return nil
}

// fanOutPerVersionBlob durably writes ONE per-version quorum-meta blob
// (.quorum_meta_versions/{key}/{vid}) to the version's K-of-N placement quorum,
// FAIL-CLOSED. It is the per-version half of writeQuorumMeta (the latest-only blob
// and the raft propose are NOT touched here) and the sole writer of hard-delete
// tombstones (DeleteObjectVersion). The encoded blob is passed in so the hot PUT
// path encodes once and shares it with the latest-only write.
func (s *QuorumMetaStore) fanOutPerVersionBlob(ctx context.Context, cmd PutObjectMetaCmd, blob []byte) error {
	if s.local() == nil || len(cmd.NodeIDs) == 0 {
		return fmt.Errorf("per-version quorum-meta write: no shard service or empty placement")
	}
	self := s.selfAddr()
	k := int(cmd.ECData)
	if k <= 0 {
		k = 1
	}
	verSubpath := path.Join(cmd.Key, cmd.VersionID)
	vctx, vcancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer vcancel()
	if verr := fanOutQuorumMeta(vctx, cmd.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return s.local().writeQuorumMetaVersionLocal(cmd.Bucket, verSubpath, blob)
		}
		addr, rerr := s.peer().resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return s.peer().WriteQuorumMetaVersion(fctx, addr, cmd.Bucket, verSubpath, blob)
	}); verr != nil {
		return fmt.Errorf("per-version quorum-meta write %s/%s@%s: %w", cmd.Bucket, cmd.Key, cmd.VersionID, verr)
	}
	return nil
}

// readQuorumMeta reads object metadata from the local quorum store, falling
// back to a peer fan-out when the local file is absent (e.g. parity node that
// missed the K-of-N write). Returns ErrObjectNotFound only when no peer has
// the file; callers then fall through to BadgerDB for pre-Phase-3 objects.
func (s *QuorumMetaStore) readQuorumMeta(bucket, key string) (*storage.Object, PlacementMeta, error) {
	if s.local() == nil {
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	raw, err := s.readQuorumMetaWinningRaw(bucket, key)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return s.local().decodeQuorumMetaBlob(raw)
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
func (s *QuorumMetaStore) readQuorumMetaWinningRaw(bucket, key string) ([]byte, error) {
	localRaw, localErr := s.local().readQuorumMetaRaw(bucket, key)
	if localErr != nil && !errors.Is(localErr, storage.ErrObjectNotFound) {
		return nil, localErr
	}

	if !s.multiGen.Load() {
		// Local-first fast path (byte-identical to legacy).
		if localErr == nil {
			return localRaw, nil
		}
		raw, ok := s.fetchQuorumMetaFromPeers(bucket, key)
		if !ok {
			return nil, storage.ErrObjectNotFound
		}
		return raw, nil
	}

	// Multi-generation cross-generation LWW merge: combine the local blob with
	// the peer-best blob and pick the winner.
	peerRaw, peerOK := s.fetchQuorumMetaFromPeers(bucket, key)
	switch {
	case localErr == nil && peerOK:
		return s.pickQuorumMetaWinner(localRaw, peerRaw), nil
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
func (s *QuorumMetaStore) pickQuorumMetaWinner(a, bRaw []byte) []byte {
	cmdA, errA := s.local().decodeQuorumMetaCmdBlob(a)
	cmdB, errB := s.local().decodeQuorumMetaCmdBlob(bRaw)
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
// Returns (nil, false) when no peer has the file or the shard-group source is nil.
func (s *QuorumMetaStore) fetchQuorumMetaFromPeers(bucket, key string) ([]byte, bool) {
	if s.local() == nil || s.groups() == nil {
		return nil, false
	}
	// Collect unique peer addresses from all shard groups, excluding self.
	self := s.selfAddr()
	seen := map[string]bool{self: true}
	var peers []string
	for _, g := range s.groups().ShardGroups() {
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
			addr, err := s.peer().resolvePeerAddress(p)
			if err != nil {
				return
			}
			data, err := s.peer().ReadQuorumMetaRaw(ctx, addr, bucket, key)
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
			if cmd, decErr := s.local().decodeQuorumMetaCmdBlob(data); decErr == nil {
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

// readQuorumMetaForReclaim is the certainty-aware sibling of readQuorumMeta for the
// DESTRUCTIVE orphan-reclaim path. Unlike readQuorumMeta (which maps an exhausted
// peer fan-out to ErrObjectNotFound even when a metadata-holding peer was merely
// unreachable), this distinguishes "proven absent" from "couldn't reach a peer":
//
//   - (obj, found=true,  certain=true)  the blob exists; obj is the LWW winner.
//   - (nil, found=false, certain=true)  proven absent (local not-found AND every
//     contacted peer responded a definitive not-found) → orphan-eligible.
//   - (nil, _,           certain=false) UNCERTAIN (local read error, OR any peer
//     unreachable/errored while the blob was absent locally) → caller MUST keep.
//
// The peer read RPC returns OK+empty for a definitive not-found and an error for a
// read/transport failure, so the client can tell them apart (handleQuorumMetaRead).
func (s *QuorumMetaStore) readQuorumMetaForReclaim(bucket, key string) (*storage.Object, bool, bool) {
	if s.local() == nil {
		return nil, false, false // can't judge → keep
	}
	localRaw, localErr := s.local().readQuorumMetaRaw(bucket, key)
	localHasData := localErr == nil && len(localRaw) > 0
	localUncertain := localErr != nil && !errors.Is(localErr, storage.ErrObjectNotFound)

	bestRaw, bestMod, bestVer, bestSeq, hasBest := []byte(nil), int64(0), "", uint64(0), false
	consider := func(raw []byte) {
		if len(raw) == 0 {
			return
		}
		var mod int64
		var ver string
		var seq uint64
		if cmd, derr := s.local().decodeQuorumMetaCmdBlob(raw); derr == nil {
			mod, ver, seq = cmd.ModTime, cmd.VersionID, cmd.MetaSeq
		}
		if !hasBest || quorumMetaBlobWins(mod, ver, seq, bestMod, bestVer, bestSeq) {
			bestRaw, bestMod, bestVer, bestSeq, hasBest = raw, mod, ver, seq, true
		}
	}
	if localHasData {
		consider(localRaw)
	}

	outcomes := s.collectReclaimPeerOutcomes(bucket, key, consider)
	found, certain := reclaimCertainty(localHasData, localUncertain, outcomes)
	if !found || !certain {
		return nil, found, certain
	}
	obj, _, derr := s.local().decodeQuorumMetaBlob(bestRaw)
	if derr != nil {
		return nil, false, false // can't decode the live blob → uncertain → keep
	}
	return obj, true, true
}

// collectReclaimPeerOutcomes fans a quorum-meta read out to every unique peer
// across all shard groups (excluding self) and returns one peerReadOutcome per
// contacted peer, feeding any returned blob to consider() for LWW winner selection.
// It mirrors fetchQuorumMetaFromPeers' peer enumeration but PRESERVES the
// not-found-vs-error distinction the reclaim decision needs. A nil shardGroup is the
// legacy single-group / solo path (no peers): it returns no outcomes, so the local
// read is authoritative (reclaimCertainty treats an empty set as complete).
func (s *QuorumMetaStore) collectReclaimPeerOutcomes(bucket, key string, consider func([]byte)) []peerReadOutcome {
	if s.local() == nil || s.groups() == nil {
		return nil
	}
	self := s.selfAddr()
	seen := map[string]bool{self: true}
	var peers []string
	for _, g := range s.groups().ShardGroups() {
		for _, p := range g.PeerIDs {
			if !seen[p] {
				seen[p] = true
				peers = append(peers, p)
			}
		}
	}
	if len(peers) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	type res struct {
		outcome peerReadOutcome
		data    []byte
	}
	ch := make(chan res, len(peers))
	var wg sync.WaitGroup
	for _, p := range peers {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := s.peer().resolvePeerAddress(p)
			if err != nil {
				ch <- res{outcome: peerErrored}
				return
			}
			data, err := s.peer().ReadQuorumMetaRaw(ctx, addr, bucket, key)
			switch {
			case err != nil:
				ch <- res{outcome: peerErrored}
			case len(data) == 0:
				ch <- res{outcome: peerNotFound}
			default:
				ch <- res{outcome: peerHasData, data: data}
			}
		}()
	}
	go func() { wg.Wait(); close(ch) }()
	outcomes := make([]peerReadOutcome, 0, len(peers))
	for r := range ch {
		if r.outcome == peerHasData {
			consider(r.data)
		}
		outcomes = append(outcomes, r.outcome)
	}
	return outcomes
}

func (s *QuorumMetaStore) readQuorumMetaVersions(bucket, key string) ([]PutObjectMetaCmd, error) {
	if s.local() == nil {
		return nil, nil
	}
	byVID := map[string]PutObjectMetaCmd{}
	// Same-VID dedup by the full LWW comparator (quorumMetaCmdWins): higher
	// ModTime wins, then higher VersionID, then higher MetaSeq, with the
	// hard-delete tombstone as the top tiebreak. Deterministic regardless of
	// fan-out iteration order (the old "MetaSeq >= keeps last-iterated" tiebreak
	// was non-deterministic for same-MetaSeq replicas).
	put := func(c PutObjectMetaCmd) {
		if ex, ok := byVID[c.VersionID]; !ok || quorumMetaCmdWins(c, ex) {
			byVID[c.VersionID] = c
		}
	}
	// self
	if local, err := s.local().readQuorumMetaVersionsLocal(bucket, key); err == nil {
		for _, c := range local {
			put(c)
		}
	}
	self := s.selfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	if s.groups() != nil {
		for _, g := range s.groups().ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := s.peer().resolvePeerAddress(p)
				if aerr != nil {
					continue
				}
				remote, rerr := s.peer().ReadQuorumMetaVersions(ctx, addr, bucket, key)
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
func (s *QuorumMetaStore) listObjectsPerVersion(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.local() == nil {
		return nil, nil
	}
	byKey := map[string]PutObjectMetaCmd{}
	// Latest by the full ModTime-primary LWW comparator (quorumMetaCmdWins: higher
	// ModTime; tie → higher VID; tie → higher MetaSeq; tombstone tier) so it matches
	// deriveLatestVersion and is deterministic regardless of fan-out iteration order
	// (mirrors readQuorumMetaVersions).
	put := func(c PutObjectMetaCmd) {
		if ex, ok := byKey[c.Key]; !ok || quorumMetaCmdWins(c, ex) {
			byKey[c.Key] = c
		}
	}
	if local, err := s.local().ScanQuorumMetaVersionsBucket(bucket, prefix); err == nil {
		for _, c := range local {
			put(c)
		}
	}
	self := s.selfAddr()
	seen := map[string]bool{self: true}
	rctx, cancel := context.WithTimeout(ctx, quorumMetaReadTimeout)
	defer cancel()
	if s.groups() != nil {
		for _, g := range s.groups().ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := s.peer().resolvePeerAddress(p)
				if aerr != nil {
					continue
				}
				remote, rerr := s.peer().ScanQuorumMetaVersions(rctx, addr, bucket, prefix)
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
			if live, ok := s.latestLiveForKey(bucket, c.Key); ok {
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
// skipped) — under blob authority a silently-truncated set is data loss. When
// shardGroup == nil (single-node) the result is the local STRICT scan.
func (s *QuorumMetaStore) scanQuorumMetaVersionsClusterAll(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.local() == nil {
		return nil, nil
	}
	type vkey struct{ key, vid string }
	byKey := map[vkey]PutObjectMetaCmd{}
	// Same-(Key,VID) dedup by the full LWW comparator (quorumMetaCmdWins) — higher
	// ModTime/VID/MetaSeq with the tombstone tiebreak — so it is deterministic
	// regardless of fan-out iteration order (mirrors readQuorumMetaVersions).
	put := func(c PutObjectMetaCmd) {
		k := vkey{c.Key, c.VersionID}
		if ex, ok := byKey[k]; !ok || quorumMetaCmdWins(c, ex) {
			byKey[k] = c
		}
	}
	// self (STRICT, fail-closed)
	local, lerr := s.local().scanQuorumMetaVersionsBucketAllStrict(bucket, prefix)
	if lerr != nil {
		return nil, lerr
	}
	for _, c := range local {
		put(c)
	}
	if s.groups() == nil {
		out := make([]PutObjectMetaCmd, 0, len(byKey))
		for _, c := range byKey {
			out = append(out, c)
		}
		return out, nil
	}
	self := s.selfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	for _, g := range s.groups().ShardGroups() {
		for _, p := range g.PeerIDs {
			if seen[p] {
				continue
			}
			seen[p] = true
			addr, aerr := s.peer().resolvePeerAddress(p)
			if aerr != nil {
				return nil, aerr // fail-closed: cannot enumerate a peer → abort
			}
			remote, rerr := s.peer().ScanQuorumMetaVersionsAll(ctx, addr, bucket, prefix)
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

// scanQuorumMetaClusterAll is the latest-only-tree twin of
// scanQuorumMetaVersionsClusterAll: a cluster-wide enumeration of the latest-only
// blob per key (.quorum_meta/{bucket}/{key}) across self + every peer, one entry
// per key (LWW winner kept). Used by the DEK-rewrap enumerator so a non-versioned
// object's segment shards on a PARITY node that missed the K-of-N latest-only write
// are still covered (via a peer's blob that lists the full placement).
//
// Fail-closed at the TRANSPORT level: an unreachable/erroring peer aborts the scan
// (a partial set would leave shards un-rewrapped → undecryptable after a future KEK
// prune). Self uses the strict (decode-fail-closed) local scan. CAVEAT: the peer
// path reuses the non-strict ScanQuorumMeta RPC, which silently DROPS an undecodable
// blob (it does not abort) — strictly weaker than the per-version twin's strict
// ScanQuorumMetaVersionsAll. Bounded: a key is dropped only if its blob is corrupt
// on EVERY replica (a single good replica masks it via the LWW union), and such an
// object is already unreadable. FOLLOW-UP before DEK-gen prune is enabled (S7): add
// a strict latest-only peer RPC mirroring handleScanQuorumMetaVersionsAll.
func (s *QuorumMetaStore) scanQuorumMetaClusterAll(bucket string) ([]PutObjectMetaCmd, error) {
	if s.local() == nil {
		return nil, nil
	}
	byKey := map[string]PutObjectMetaCmd{}
	put := func(c PutObjectMetaCmd) {
		if ex, ok := byKey[c.Key]; !ok || quorumMetaCmdWins(c, ex) {
			byKey[c.Key] = c
		}
	}
	local, lerr := s.local().scanQuorumMetaBucketStrict(bucket)
	if lerr != nil {
		return nil, lerr
	}
	for _, c := range local {
		put(c)
	}
	if s.groups() != nil {
		self := s.selfAddr()
		seen := map[string]bool{self: true}
		ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
		defer cancel()
		for _, g := range s.groups().ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := s.peer().resolvePeerAddress(p)
				if aerr != nil {
					return nil, aerr // fail-closed
				}
				remote, rerr := s.peer().ScanQuorumMeta(ctx, addr, bucket, "")
				if rerr != nil {
					return nil, rerr // fail-closed: a partial set leaves shards un-rewrapped
				}
				for _, c := range remote {
					put(c)
				}
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
func (s *QuorumMetaStore) readQuorumMetaVersion(bucket, key, versionID string) (PutObjectMetaCmd, bool, error) {
	cmds, err := s.readQuorumMetaVersions(bucket, key)
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

// readQuorumMetaVersionsDecodeStrict is the read1 blob-authoritative per-key reader:
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
// decode failures and is correct for the off-path / non-blob-authority consumers.
func (s *QuorumMetaStore) readQuorumMetaVersionsDecodeStrict(bucket, key string) ([]PutObjectMetaCmd, error) {
	if s.local() == nil {
		return nil, nil
	}
	var rawBlobs [][]byte
	// self — strict local read (a present-but-unreadable blob fails closed;
	// decode happens once, strictly, in the loop below).
	selfRaw, err := s.local().readQuorumMetaVersionsRawLocal(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("decode-strict version read %s/%s: %w", bucket, key, err)
	}
	rawBlobs = append(rawBlobs, selfRaw...)
	// peers — all-groups fan-out; tolerate an unreachable peer (a corrupt blob is
	// caught wherever it IS served, below).
	self := s.selfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	if s.groups() != nil {
		for _, g := range s.groups().ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := s.peer().resolvePeerAddress(p)
				if aerr != nil {
					continue // availability-tolerant
				}
				peerRaw, rerr := s.peer().ReadQuorumMetaVersionsRaw(ctx, addr, bucket, key)
				if rerr != nil {
					continue // unreachable / un-upgraded / peer read error → tolerate
				}
				rawBlobs = append(rawBlobs, peerRaw...)
			}
		}
	}
	// strict decode + dedup. Same-VID replicas dedup by the full LWW comparator
	// (quorumMetaCmdWins) so the winner is deterministic regardless of fan-out
	// order, mirroring readQuorumMetaVersions.
	byVID := map[string]PutObjectMetaCmd{}
	for _, blob := range rawBlobs {
		cmd, derr := s.local().decodeQuorumMetaCmdBlob(blob)
		if derr != nil {
			// A served blob we cannot decode: its VID is unknown, so we cannot rule
			// out that it WAS the authoritative latest. Fail closed.
			return nil, fmt.Errorf("decode-strict version read %s/%s: %w", bucket, key, derr)
		}
		if ex, ok := byVID[cmd.VersionID]; !ok || quorumMetaCmdWins(cmd, ex) {
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
func (s *QuorumMetaStore) readQuorumMetaVersionDecodeStrict(bucket, key, versionID string) (PutObjectMetaCmd, bool, error) {
	cmds, err := s.readQuorumMetaVersionsDecodeStrict(bucket, key)
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

// latestLiveForKey resolves the live latest version for a single key from the full
// cluster-wide per-version set (tombstones and delete markers excluded via
// deriveLatestVersion). Used by listObjectsPerVersion when a key's per-key-max scan
// collapsed to a hard-delete tombstone: the predecessor was collapsed away, so the
// new latest must be re-derived from all versions of that key.
func (s *QuorumMetaStore) latestLiveForKey(bucket, key string) (PutObjectMetaCmd, bool) {
	cmds, err := s.readQuorumMetaVersions(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, false
	}
	return deriveLatestVersion(cmds)
}

func (s *QuorumMetaStore) deleteQuorumMetaQuorum(ctx context.Context, bucket, key string, nodeIDs []string) error {
	if s.local() == nil {
		return nil
	}
	self := s.selfAddr()
	dctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	var firstErr error
	for _, node := range nodeIDs {
		var err error
		if node == self {
			err = s.local().deleteQuorumMetaLocal(bucket, key)
		} else if addr, rerr := s.peer().resolvePeerAddress(node); rerr == nil {
			err = s.peer().DeleteQuorumMeta(dctx, addr, bucket, key)
		} else {
			err = rerr
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// readQuorumMetaCmd is the store-level read for PutObjectMetaCmd, with peer
// fan-out fallback when the local quorum meta file is absent. Used by
// SetObjectACLPropose, SetObjectTagsPropose, and AppendObject migration.
func (s *QuorumMetaStore) readQuorumMetaCmd(bucket, key string) (PutObjectMetaCmd, error) {
	if s.local() == nil {
		return PutObjectMetaCmd{}, storage.ErrObjectNotFound
	}
	raw, err := s.readQuorumMetaWinningRaw(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return s.local().decodeQuorumMetaCmdBlob(raw)
}

func (s *QuorumMetaStore) ScanObjectMetaEntries(ctx context.Context, bucket, prefix string) ([]ObjectIndexEntry, error) {
	cmds, err := s.scatterGatherList(ctx, bucket, prefix)
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

func (s *QuorumMetaStore) scatterGatherList(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.local() == nil {
		return nil, nil
	}
	self := s.selfAddr()
	seen := map[string]bool{}
	var peerIDs []string
	if s.groups() != nil {
		for _, g := range s.groups().ShardGroups() {
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
		entries, err := s.local().ScanQuorumMetaBucket(bucket, prefix)
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
				entries, _ := s.local().ScanQuorumMetaBucket(bucket, prefix)
				ch <- nodeResult{entries: entries}
				return
			}
			addr, aerr := s.peer().resolvePeerAddress(p)
			if aerr != nil {
				ch <- nodeResult{}
				return
			}
			entries, _ := s.peer().ScanQuorumMeta(rctx, addr, bucket, prefix)
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
