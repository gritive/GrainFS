package cluster

import (
	"bytes"
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
	"syscall"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/directio"
)

// Control-plane vs data-plane metadata boundary (load-bearing, intentional):
//
//   - DATA-PLANE (object metadata): every per-object record — regular/chunked/
//     multipart/appendable/coalesced PUT, tags, ACL, quarantine, delete — is
//     written ONLY to the off-raft quorum-meta blob co-located with its EC shards
//     (K-of-N quorum, LWW-merged on read; this file). There is NO raft propose for
//     object metadata, and there is no SINGLE shared writer beyond the blob RMW.
//     Reads, LIST, the orphan/segment GCs, the EC scrub/placement monitor, and DEK
//     rewrap all derive from the quorum-meta blobs.
//
//   - CONTROL-PLANE: bucket existence/policy/versioning, bucket→group assignment,
//     placement generations, IAM, DEK/KEK, JWT keys, invites, peer registry,
//     protocol credentials, lifecycle, config — all replicated via the meta-raft
//     FSM (BadgerDB). The FSM holds ZERO per-object records.
//
// Consequence: the FSM keyspace's obj:/lat: (ObjectMetaKey/ObjectMetaKeyV/
// LatestKey) tiers are NOT a second writer of object metadata; the writer-less FSM
// object-read scaffolding was removed (greenfield: no residual obj:/lat: records,
// per the s4c_cutover greenfield decision). "Unify Raft vs Quorum" is therefore a
// non-goal — the two stores never hold the same data.

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
// bucketVersioningEnabled delegates to the extracted QuorumMetaStore (see
// quorum_meta_store.go). DistributedBackend stays a behavior-preserving facade.
func (b *DistributedBackend) bucketVersioningEnabled(ctx context.Context, bucket string) bool {
	return b.qmsOrBuild().bucketVersioningEnabled(ctx, bucket)
}

// writeQuorumMeta delegates to the extracted QuorumMetaStore.
func (b *DistributedBackend) writeQuorumMeta(ctx context.Context, cmd PutObjectMetaCmd) error {
	return b.qmsOrBuild().writeQuorumMeta(ctx, cmd)
}

// fanOutPerVersionBlob delegates to the extracted QuorumMetaStore.
func (b *DistributedBackend) fanOutPerVersionBlob(ctx context.Context, cmd PutObjectMetaCmd, blob []byte) error {
	return b.qmsOrBuild().fanOutPerVersionBlob(ctx, cmd, blob)
}

// readQuorumMeta delegates to the extracted QuorumMetaStore.
func (b *DistributedBackend) readQuorumMeta(bucket, key string) (*storage.Object, PlacementMeta, error) {
	return b.qmsOrBuild().readQuorumMeta(bucket, key)
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
	if modA != modB || verA != verB {
		return latestWins(modA, verA, modB, verB)
	}
	return seqA > seqB
}

// latestWins is the 2-tier ModTime-primary core shared by quorumMetaBlobWins
// (which adds a MetaSeq tier) and the storage.ObjectVersion latest-deciding sites
// that carry no MetaSeq (reconcileVersionIsLatest / sortObjectVersions / the leaf
// version sort). It reports whether (modA, vidA) beats (modB, vidB): higher
// ModTime wins; on an equal ModTime (second granularity) the lexicographically
// greater VersionID wins. VersionIDs are unique per version, so distinct versions
// always order strictly — a valid sort.Slice less function.
func latestWins(modA int64, vidA string, modB int64, vidB string) bool {
	if modA != modB {
		return modA > modB
	}
	return vidA > vidB
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

// errQuorumMetaCASReject is returned by the write-time guards when a CAS
// candidate (MetaSeqCAS) loses the +1 base-match check: a stalled owner read
// base=N and wrote N+1, but a newer writer already advanced existing to N+1, so
// the late write's base no longer matches. It is DISTINGUISHABLE from a benign
// LWW skip (which returns nil): the append/coalesce coordinator must see this
// error to re-read the base and retry — it must NOT mistake the rejected write
// for a successful no-op (which would silently drop the appended data).
var errQuorumMetaCASReject = errors.New("quorum meta: CAS base mismatch")

// quorumMetaCASRejectWireCode is the stable, distinguishable error-body string a
// remote quorum-meta WRITE handler emits when its local write returns
// errQuorumMetaCASReject. The RPC error channel is free-text (errorResponse →
// err.Error()), so the CAS-reject signal must travel as a fixed sentinel string
// that the client maps back to errQuorumMetaCASReject — not as the free-text
// message (which could change) and not collide with any other handler error.
//
// BUG-1 fix: without this, a REMOTE replica's CAS reject reaches the client as a
// generic "remote quorum meta error", so fanOutQuorumMeta never counts it as
// errQuorumMetaCASReject and the append/coalesce RMW retry/SlowDown logic is
// bypassed on a multi-node cluster.
const quorumMetaCASRejectWireCode = "QUORUM_META_CAS_REJECT"

// quorumMetaWriteErrorBody maps a local quorum-meta write error to the error-body
// string sent over the RPC error channel. errQuorumMetaCASReject becomes the
// stable wire code; every other error keeps its free-text message.
func quorumMetaWriteErrorBody(err error) string {
	if errors.Is(err, errQuorumMetaCASReject) {
		return quorumMetaCASRejectWireCode
	}
	return err.Error()
}

// quorumMetaWriteRPCError maps a remote quorum-meta WRITE reply back to a local
// error. An "Error" reply whose body is the CAS-reject wire code is reconstituted
// as errQuorumMetaCASReject (errors.Is-able), so fanOutQuorumMeta counts a remote
// reject identically to a local one. Any other "Error" reply stays a generic
// remote error annotated with addr + the reported body.
func quorumMetaWriteRPCError(addr string, body []byte) error {
	if string(body) == quorumMetaCASRejectWireCode {
		return errQuorumMetaCASReject
	}
	if len(body) == 0 {
		return fmt.Errorf("remote quorum meta error from %s", addr)
	}
	return fmt.Errorf("remote quorum meta error from %s: %s", addr, string(body))
}

// quorumMetaWriteVerdict is the tri-state result of the write-time accept guard.
type quorumMetaWriteVerdict int

const (
	// quorumMetaWriteApply: rename the candidate over the existing blob.
	quorumMetaWriteApply quorumMetaWriteVerdict = iota
	// quorumMetaWriteSkip: keep the existing blob, skip the rename, return nil
	// (a benign LWW loss, or an idempotent CAS re-delivery — both are no-ops, NOT
	// errors).
	quorumMetaWriteSkip
	// quorumMetaWriteRejectCAS: a CAS candidate lost the +1 base race — return the
	// DISTINGUISHABLE errQuorumMetaCASReject so the coordinator re-reads and retries.
	quorumMetaWriteRejectCAS
)

// decideQuorumMetaWrite is the SINGLE decision point for whether a candidate
// quorum-meta blob may overwrite the existing one. Mutable-accumulating writes
// (append/coalesce) set MetaSeqCAS and require strict +1 monotonicity over the
// existing blob's MetaSeq — a failover-safe compare-and-swap fence (spec §7-A/B):
// an absent existing is MetaSeq 0, so the first CAS write must carry MetaSeq==1.
//
// CAS idempotency is NOT decided here (this sees only the decoded cmds, which
// cannot tell "the exact same candidate already landed" from "a different write
// that happens to share MetaSeq+VersionID"). The byte-identical re-delivery skip
// is applied at the call site, BEFORE this guard, via quorumMetaBlobIsIdempotentReplay.
//
// Every non-CAS write uses unchanged LWW (quorumMetaCmdWins). Both write-time
// guards (writeQuorumMetaLocal / writeQuorumMetaVersionLocal) route through here;
// it is the §7-F single-truth-point.
func decideQuorumMetaWrite(existing, cand PutObjectMetaCmd) quorumMetaWriteVerdict {
	if cand.MetaSeqCAS {
		// Placement fence: reject a CAS write whose PlacementGroupID no longer
		// matches the existing blob's (cross-placement rebalance safety). An absent
		// existing (PlacementGroupID=="") passes — first write / new object is
		// unaffected. The LWW branch is unchanged.
		placementOK := existing.PlacementGroupID == "" || cand.PlacementGroupID == existing.PlacementGroupID
		if existing.MetaSeq+1 == cand.MetaSeq && placementOK {
			return quorumMetaWriteApply
		}
		return quorumMetaWriteRejectCAS
	}
	if quorumMetaCmdWins(cand, existing) {
		return quorumMetaWriteApply
	}
	return quorumMetaWriteSkip
}

// quorumMetaBlobIsIdempotentReplay reports whether a CAS candidate is a
// byte-identical re-delivery of the blob already on disk. A K-of-N CAS fan-out
// can deliver the SAME candidate to one node more than once — directly (a
// placement set that resolves multiple node IDs to the same physical node, e.g.
// EC over a single host) or via retry. The exact same write landing twice on a
// node must be a no-op SKIP, NOT a CAS reject (rejecting it would spuriously fail
// the whole RMW). Byte-equality is the only safe discriminator: a different write
// that happens to share (MetaSeq, VersionID) has different content, so it is NOT
// idempotent and correctly falls through to the +1 CAS guard (→ reject). Only
// MetaSeqCAS candidates qualify (LWW writes already no-op on a tie).
func quorumMetaBlobIsIdempotentReplay(existing, candidate []byte, candIsCAS bool) bool {
	return candIsCAS && bytes.Equal(existing, candidate)
}

// readQuorumMetaWinningRaw, pickQuorumMetaWinner and fetchQuorumMetaFromPeers moved
// to QuorumMetaStore (quorum_meta_store.go) in the extraction. They are internal to
// the read orchestration — only QuorumMetaStore calls them, on itself — so no
// DistributedBackend facade delegate is needed (and a zero-caller delegate would be
// dead code). The DistributedBackend-level read entrypoints (readQuorumMeta /
// readQuorumMetaCmd / readQuorumMetaForReclaim) remain as facades below.

// peerReadOutcome is one peer's response to a reclaim quorum-meta read.
type peerReadOutcome int

const (
	peerErrored  peerReadOutcome = iota // unreachable / read error → UNCERTAIN
	peerNotFound                        // OK with empty payload → definitively absent on that peer
	peerHasData                         // OK with the blob present
)

// reclaimCertainty decides, for a DESTRUCTIVE orphan reclaim, whether a quorum-meta
// blob was seen (found) and whether the responding set is COMPLETE (certain). The
// two are INDEPENDENT: a reclaim makes a NEGATIVE claim ("this object/version is
// absent from the live metadata"), valid only against the complete authoritative
// set, so ANY errored/unreachable peer forces certain=false even when another peer
// returned a (possibly stale) manifest — otherwise a newer manifest on the
// unreachable peer could be missed and live data deleted. Caller contract: reclaim
// ONLY when certain && !found; on certain && found, judge liveness from the winning
// blob; on !certain, KEEP.
func reclaimCertainty(localHasData, localUncertain bool, peers []peerReadOutcome) (found, certain bool) {
	if localUncertain {
		return false, false // local read errored (non-NotFound) → can't judge → keep
	}
	found = localHasData
	anyErr := false
	for _, p := range peers {
		switch p {
		case peerHasData:
			found = true
		case peerErrored:
			anyErr = true
		}
	}
	// certain ⟺ complete responding set: local resolved AND every contacted peer
	// answered (data or definitive not-found), none errored. Solo / legacy
	// single-group (no peers contacted) is trivially complete.
	return found, !anyErr
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
func (b *DistributedBackend) readQuorumMetaForReclaim(bucket, key string) (*storage.Object, bool, bool) {
	return b.qmsOrBuild().readQuorumMetaForReclaim(bucket, key)
}

// fanOutQuorumMeta dispatches to every placement node concurrently and returns
// as soon as K acks arrive. Returns an error only when the quorum becomes
// unreachable (more than N-K failures or context cancellation). Errors are
// propagated to the caller: a failed quorum-meta write fails the PUT.
//
// errQuorumMetaCASReject handling: a CAS reject is ONE replica's vote, NOT a
// global short-circuit. With ordinary K-of-N replica skew (some replicas at
// MetaSeq S, some at S+1 after a prior successful write), the NEXT CAS write of
// S+2 is ACCEPTED by the up-to-date replicas and REJECTED by the laggards — the
// votes differ across replicas, so short-circuiting on the first reject would
// surface a spurious reject even when K replicas accept (and a local-first retry
// could then false-Noop without ever reaching K — the partial-publish bug). So a
// CAS reject counts toward the N-K failure budget like any other failed vote; K
// nil-acks still win. Only when K is UNREACHABLE do we surface the DISTINGUISHABLE
// errQuorumMetaCASReject (if any reject was observed) so the append/coalesce RMW
// re-reads the base and retries — preserving the §7-F single-truth contract for
// the genuine base-advanced-everywhere case (all replicas reject → K unreachable
// → surface the sentinel). A non-CAS-only quorum failure keeps the generic error.
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
	var ok, failed, casRejects int
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err == nil {
				ok++
				if ok >= k {
					return nil
				}
			} else {
				if errors.Is(err, errQuorumMetaCASReject) {
					casRejects++
				}
				failed++
				if failed > n-k {
					// K is now unreachable. If ANY vote was a CAS reject, surface the
					// DISTINGUISHABLE sentinel so the caller re-reads the base and
					// retries (the genuine base-advanced case). Otherwise it is a plain
					// availability failure.
					if casRejects > 0 {
						return errQuorumMetaCASReject
					}
					return fmt.Errorf("quorum meta: %d/%d nodes failed, quorum %d unreachable", failed, n, k)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// fanOutQuorumMetaOwnerLocalFirst is the owner-local-first variant of
// fanOutQuorumMeta used by the CAS read-modify-write writers (append/coalesce).
//
// BUG-2 fix (owner-local base freshness): plain fanOutQuorumMeta returns on the
// k-th ack, which may NOT include the owner-local write when k peer acks arrive
// first. The CAS RMW reads its base owner-local-first (readQuorumMetaWinningRaw),
// so if a same-owner write returns before its OWN local copy is durable, the next
// RMW on that owner reads a STALE local base, computes a MetaSeq the peers already
// advanced past, and CAS-rejects forever (false offset mismatch / livelock / lost
// update). Under a single stable leader the owner is the authoritative reader of
// its own copy, so its local write MUST be durable before the RMW returns.
//
// Fix: when self is a placement node, write the owner-local copy before peer
// publish and require it to be durable before any return so a subsequent
// owner-local-first base read sees it. Do NOT overlap peer publishes with the
// owner-local CAS write in this one-phase protocol: writeLocal includes the CAS
// accept/reject decision plus file and directory fsync. If it rejects or fails
// after peers have accepted the candidate, the caller has no compare-and-rollback
// peer primitive to remove only that candidate without deleting a newer or
// previously committed blob. The local write counts for EVERY self entry in the
// placement set (a placement set can list self more than once — e.g. EC over a
// single host, where the byte-identical CAS re-delivery is idempotent), exactly as
// plain fanOutQuorumMeta acked once per self goroutine. The remaining k-selfCount
// acks come from the true peers, preserving the N-K peer failure budget unchanged:
// (peerCount)-(k-selfCount) = n-k. A CAS reject on the owner-local write is
// surfaced immediately (the owner's own base advanced — the genuine retry signal).
// When self is NOT a placement node the behavior is identical to plain
// fanOutQuorumMeta over all nodes. If the owner-local write succeeds but peer
// fan-out cannot reach quorum, cleanupLocal is called before returning the
// original fan-out error. The cleanup must be compare-and-delete: a failed write
// must not remove a newer local write or a previously committed idempotent replay.
func fanOutQuorumMetaOwnerLocalFirst(
	ctx context.Context,
	nodes []string,
	self string,
	k int,
	writeLocal func() error,
	cleanupLocal func() error,
	writePeer func(context.Context, string) error,
) error {
	if k <= 0 {
		k = 1
	}
	// Partition placement into the owner-local entries (all self occurrences) and
	// the true peers. selfCount mirrors the number of acks the old all-node fan-out
	// produced for self (one per goroutine), since the local write is idempotent.
	selfCount := 0
	peers := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node == self {
			selfCount++
			continue
		}
		peers = append(peers, node)
	}
	if selfCount == 0 {
		// Owner is not a placement node — no local-first guarantee to make; fall
		// back to the plain all-node fan-out so the peer dispatch is unchanged.
		return fanOutQuorumMeta(ctx, nodes, k, func(fctx context.Context, node string) error {
			return writePeer(fctx, node)
		})
	}
	// Owner-local write first, SYNCHRONOUSLY (once; idempotent for duplicate self
	// entries). A CAS reject here is the owner's own base advancing — surface it so
	// the RMW re-reads and retries.
	remaining := k - selfCount
	if remaining > len(peers) {
		return fmt.Errorf("quorum meta: placement nodes %d < quorum size %d", len(nodes), k)
	}
	if err := writeLocal(); err != nil {
		return err
	}
	// The local write satisfies selfCount of the k required acks. Require the rest
	// from the true peers; if the local acks already meet the quorum we are done.
	if remaining <= 0 {
		return nil
	}
	err := fanOutQuorumMeta(ctx, peers, remaining, writePeer)
	if err == nil {
		return nil
	}
	if cleanupLocal != nil {
		if cerr := cleanupLocal(); cerr != nil {
			return errors.Join(err, fmt.Errorf("owner-local quorum-meta cleanup after fan-out failure: %w", cerr))
		}
	}
	return err
}

// writeQuorumMetaLocal durably writes the encoded quorum meta blob for
// (bucket, key) under {dataDirs[0]}/.quorum_meta/{bucket}/{key}.
func (m *LocalQuorumMetaStore) writeQuorumMetaLocal(bucket, key string, data []byte) error {
	_, err := m.writeQuorumMetaLocalWithResult(bucket, key, data)
	return err
}

type quorumMetaLocalWriteResult struct {
	applied     bool
	hadPrevious bool
	previous    []byte
}

// writeQuorumMetaLocalWithResult mirrors writeQuorumMetaLocal and also reports
// whether this call actually renamed the candidate over the target. A nil error
// with applied=false means the write was a no-op: byte-identical CAS replay,
// CAS/LWW guard skip, or another non-publish path.
func (m *LocalQuorumMetaStore) writeQuorumMetaLocalWithResult(bucket, key string, data []byte) (quorumMetaLocalWriteResult, error) {
	if len(m.dataDirs) == 0 {
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta: no data dir")
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta: key %q escapes root", key)
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta mkdir: %w", err)
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
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta tmp create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta write: %w", err)
	}
	if err := directio.Sync(tmp); err != nil {
		_ = tmp.Close()
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta tmp close: %w", err)
	}
	// Per-target lock: serializes the (guard-read + rename) critical section so
	// two concurrent writers for the same target cannot both observe the old blob,
	// both conclude their candidate wins, and then race on the rename — which would
	// allow a lower-priority blob to land last and clobber the true LWW winner.
	// Temp create/write/fsync/close above are outside the lock (unique temp name;
	// no contention there). The existing defer os.Remove(tmpName) still fires on
	// the guard-skip path because it was registered before the lock is taken.
	unlock := m.quorumMetaTargetLock(target)
	defer unlock()
	// Write-time accept guard: a blind-writer (e.g. leaderless backfill) must not
	// clobber a newer on-disk blob. Absent file → no-op accept (the common case).
	// The single discriminator quorumMetaWriteAccepts decides: CAS candidates
	// (append/coalesce) require existing.MetaSeq+1 == cand.MetaSeq, everything else
	// uses LWW (read-modify-write ACL/tags bump MetaSeq, so they strictly win).
	// A CAS reject surfaces a DISTINGUISHABLE error so the coordinator retries; an
	// LWW loss stays a silent no-op skip. Decode failures of the existing blob keep
	// the legacy behavior: fall through and rename (do not regress the corruption
	// path).
	result := quorumMetaLocalWriteResult{}
	if existing, rerr := os.ReadFile(target); rerr == nil {
		result.hadPrevious = true
		result.previous = append([]byte(nil), existing...)
		if cand, derr := m.decodeQuorumMetaCmdBlob(data); derr == nil {
			// Byte-identical CAS re-delivery (same write to the same node twice) is a
			// no-op, NOT a reject — checked before the +1 guard.
			if quorumMetaBlobIsIdempotentReplay(existing, data, cand.MetaSeqCAS) {
				return quorumMetaLocalWriteResult{}, nil
			}
			if cur, derr2 := m.decodeQuorumMetaCmdBlob(existing); derr2 == nil {
				switch decideQuorumMetaWrite(cur, cand) {
				case quorumMetaWriteRejectCAS:
					return quorumMetaLocalWriteResult{}, errQuorumMetaCASReject // CAS base mismatch — caller retries
				case quorumMetaWriteSkip:
					return quorumMetaLocalWriteResult{}, nil // LWW loss — keep existing, skip the rename
				}
			}
		}
	}
	if err := os.Rename(tmpName, target); err != nil {
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta rename: %w", err)
	}
	if err := syncDirChainNoDedup(dir, root, m.fsyncDir); err != nil {
		return quorumMetaLocalWriteResult{}, fmt.Errorf("quorum meta dir fsync: %w", err)
	}
	result.applied = true
	return result, nil
}

// rollbackQuorumMetaLocalIfMatch removes or restores the latest-only quorum-meta
// blob only if the on-disk bytes still equal expected. It rolls back an
// owner-local CAS publish after peer quorum failure without deleting a newer local
// write or discarding the previously committed local blob that the failed publish
// overwrote.
func (m *LocalQuorumMetaStore) rollbackQuorumMetaLocalIfMatch(bucket, key string, expected []byte, previous []byte, hadPrevious bool) error {
	if len(m.dataDirs) == 0 {
		return fmt.Errorf("quorum meta rollback-if-match: no data dir")
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta rollback-if-match: key %q escapes root", key)
	}
	unlock := m.quorumMetaTargetLock(target)
	defer unlock()
	cur, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("quorum meta rollback-if-match read: %w", err)
	}
	if !bytes.Equal(cur, expected) {
		return nil
	}
	if !hadPrevious {
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("quorum meta rollback-if-match remove: %w", err)
		}
		return nil
	}
	return m.writeQuorumMetaFileAtomic(target, previous, "quorum meta rollback", root)
}

func (m *LocalQuorumMetaStore) writeQuorumMetaFileAtomic(target string, data []byte, label string, syncStop string) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%s mkdir: %w", label, err)
	}
	tmp, err := os.CreateTemp(dir, ".qmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("%s tmp create: %w", label, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("%s write: %w", label, err)
	}
	if err := directio.Sync(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("%s fsync: %w", label, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%s tmp close: %w", label, err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("%s rename: %w", label, err)
	}
	if err := syncDirChainNoDedup(dir, syncStop, m.fsyncDir); err != nil {
		return fmt.Errorf("%s dir fsync: %w", label, err)
	}
	return nil
}

// writeQuorumMetaVersionLocalCore is the lock-free, guard-free FS core for
// per-version blob writes: mkdir + atomic temp+fsync+rename. It assumes the
// caller has already validated the target path and holds the per-target lock
// (the LWW-guard critical section in writeQuorumMetaVersionLocal).
func (m *LocalQuorumMetaStore) writeQuorumMetaVersionLocalCore(target string, data []byte, syncStop string) error {
	return m.writeQuorumMetaFileAtomic(target, data, "quorum meta version", syncStop)
}

// writeQuorumMetaVersionLocal durably writes an immutable per-version quorum-meta
// blob under {dataDirs[0]}/.quorum_meta_versions/{bucket}/{versionSubpath}, where
// versionSubpath is path.Join(key, versionID). It mirrors writeQuorumMetaLocal
// (path-traversal guard + atomic temp+fsync+rename) but uses the separate
// per-version subtree, so {key} is always a directory (holding {vid} files) and
// never collides with the latest-only leaf file in .quorum_meta.
func (m *LocalQuorumMetaStore) writeQuorumMetaVersionLocal(bucket, versionSubpath string, data []byte) error {
	if len(m.dataDirs) == 0 {
		return fmt.Errorf("quorum meta version: no data dir")
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir)
	target := filepath.Join(root, bucket, versionSubpath)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta version: path %q escapes root", versionSubpath)
	}
	// Per-target lock: serializes the (guard-read + rename) critical section.
	// Mirrors writeQuorumMetaLocal — see that function for the full rationale.
	unlock := m.quorumMetaTargetLock(target)
	defer unlock()
	// Write-time accept guard: a blind-writer (e.g. leaderless backfill) must not
	// clobber a newer on-disk blob. Absent file → no-op accept (the common case).
	// Routes through the single discriminator quorumMetaWriteAccepts: CAS
	// candidates (append/coalesce) require existing.MetaSeq+1 == cand.MetaSeq and a
	// reject surfaces the DISTINGUISHABLE error so the coordinator retries; LWW
	// losses stay a silent no-op skip (byte-identical to the prior quorumMetaCmdWins
	// guard). decodeQuorumMetaCmdBlob is a method on *ShardService (receiver `s`);
	// a decode failure of either blob keeps the legacy behavior (fall through and
	// rename — do not regress the corruption path).
	if existing, rerr := os.ReadFile(target); rerr == nil {
		if cand, derr := m.decodeQuorumMetaCmdBlob(data); derr == nil {
			// Byte-identical CAS re-delivery (same write to the same node twice) is a
			// no-op, NOT a reject — checked before the +1 guard.
			if quorumMetaBlobIsIdempotentReplay(existing, data, cand.MetaSeqCAS) {
				return nil
			}
			if cur, derr2 := m.decodeQuorumMetaCmdBlob(existing); derr2 == nil {
				switch decideQuorumMetaWrite(cur, cand) {
				case quorumMetaWriteRejectCAS:
					return errQuorumMetaCASReject // CAS base mismatch — caller retries
				case quorumMetaWriteSkip:
					return nil // LWW loss — keep existing, skip the rename
				}
			}
		}
	}
	return m.writeQuorumMetaVersionLocalCore(target, data, root)
}

// readQuorumMetaVersionsLocal returns the decoded per-version blobs for one key
// from .quorum_meta_versions/{bucket}/{key}/{vid}. Absent dir → empty, no error.
func (m *LocalQuorumMetaStore) readQuorumMetaVersionsLocal(bucket, key string) ([]PutObjectMetaCmd, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir)
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
		cmd, derr := m.decodeQuorumMetaCmdBlob(data)
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
// an os.ReadFile failure (a present-but-unreadable blob under blob authority must
// fail closed, not be silently skipped); decoding (and its strictness) is the
// caller's job. Absent dir → (nil, nil). Temp files + the key-escape guard are
// handled identically to readQuorumMetaVersionsLocal.
func (m *LocalQuorumMetaStore) readQuorumMetaVersionsRawLocal(bucket, key string) ([][]byte, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir)
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
	envb := buildShardEnvelope("ReadQuorumMetaVersionsRaw", bucket, key, 0, nil)
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
	envb := buildShardEnvelope("ReadQuorumMetaVersions", bucket, key, 0, nil)
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
	return b.qmsOrBuild().readQuorumMetaVersions(bucket, key)
}

// listObjectsPerVersion delegates to the extracted QuorumMetaStore.
func (b *DistributedBackend) listObjectsPerVersion(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return b.qmsOrBuild().listObjectsPerVersion(ctx, bucket, prefix)
}

// scanQuorumMetaVersionsClusterAll unions EVERY per-version quorum-meta blob in
// the bucket across ALL placement groups (every generation), deduped by
// (Key, VersionID) with a MetaSeq tiebreak. Unlike readQuorumMetaVersions
// (single-key, dedup-by-VID) it is bucket-wide and all-version (no max-per-key
// collapse). Unlike listObjectsPerVersion it is FAIL-CLOSED: a local strict-scan
// error, a peer-address resolution error, or a peer RPC error is returned (NOT
// skipped) — under blob authority a silently-truncated set is data loss. When
// shardGroup == nil (single-node) the result is the local STRICT scan.
func (b *DistributedBackend) scanQuorumMetaVersionsClusterAll(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return b.qmsOrBuild().scanQuorumMetaVersionsClusterAll(bucket, prefix)
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
func (b *DistributedBackend) scanQuorumMetaClusterAll(bucket string) ([]PutObjectMetaCmd, error) {
	return b.qmsOrBuild().scanQuorumMetaClusterAll(bucket)
}

// readQuorumMetaVersion returns the single per-version blob for (bucket, key,
// versionID), found via the all-groups fan-out (NOT local-only) so a reachable
// replica is located even when the local node isn't a placement node. Returns
// (cmd, true, nil) on a hit, (zero, false, nil) on a miss.
func (b *DistributedBackend) readQuorumMetaVersion(bucket, key, versionID string) (PutObjectMetaCmd, bool, error) {
	return b.qmsOrBuild().readQuorumMetaVersion(bucket, key, versionID)
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
func (b *DistributedBackend) readQuorumMetaVersionsDecodeStrict(bucket, key string) ([]PutObjectMetaCmd, error) {
	return b.qmsOrBuild().readQuorumMetaVersionsDecodeStrict(bucket, key)
}

// readQuorumMetaVersionDecodeStrict is the specific-version twin of
// readQuorumMetaVersion built on the decode-strict reader: a corrupt SIBLING
// version under the same key fails the read closed (the version set is
// untrustworthy if any blob under the key is undecodable).
func (b *DistributedBackend) readQuorumMetaVersionDecodeStrict(bucket, key, versionID string) (PutObjectMetaCmd, bool, error) {
	return b.qmsOrBuild().readQuorumMetaVersionDecodeStrict(bucket, key, versionID)
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
		// ModTime-primary: the last-COMPLETED write is latest (quorumMetaCmdWins =
		// higher ModTime; tie → higher VersionID; tie → higher MetaSeq), NOT the
		// max create-time VersionID. A multipart created before but completed after a
		// same-key PUT (lower VID, higher ModTime) is correctly latest.
		if !found || quorumMetaCmdWins(c, latest) {
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

// latestLiveForKey moved to QuorumMetaStore (quorum_meta_store.go): only
// listObjectsPerVersion (now on the store) calls it, on itself, so the
// DistributedBackend facade delegate would be dead code and was omitted.

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
func (m *LocalQuorumMetaStore) readQuorumMetaRaw(bucket, key string) ([]byte, error) {
	if len(m.dataDirs) == 0 {
		return nil, storage.ErrObjectNotFound
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("quorum meta read raw: key %q escapes root", key)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		// A path-shape collision between `key` and the on-disk layout is a definitive
		// not-found, not a read fault — both directions:
		//   ENOTDIR: an ANCESTOR of `key` is a FILE (reading "k/coalesced" when the blob
		//     for "k" is a file at .quorum_meta/{bucket}/k).
		//   EISDIR:  `key` ITSELF is a DIRECTORY (reading "k" when "k/sub" objects exist,
		//     so .quorum_meta/{bucket}/k is a dir, not a blob file).
		// In both cases no blob can exist at `key`, so map to ErrObjectNotFound — this
		// lets the orphan-reclaim certainty read treat it as proven-absent instead of
		// uncertain (which would wrongly KEEP a coalesced orphan forever).
		if os.IsNotExist(err) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EISDIR) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("quorum meta read raw: %w", err)
	}
	return data, nil
}

// decodeQuorumMetaBlob decodes a raw quorum meta blob into storage.Object and
// PlacementMeta. Used by both the local-read and peer-fallback paths.
func (m *LocalQuorumMetaStore) decodeQuorumMetaBlob(data []byte) (*storage.Object, PlacementMeta, error) {
	putCmd, err := decodeQuorumMetaBlob(data)
	if err != nil {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta decode command: %w", err)
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
func (m *LocalQuorumMetaStore) readQuorumMetaLocalDecoded(bucket, key string) (*storage.Object, PlacementMeta, error) {
	data, err := m.readQuorumMetaRaw(bucket, key)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return m.decodeQuorumMetaBlob(data)
}

// ScanQuorumMetaBucket returns all PutObjectMetaCmd entries (including
// IsDeleteMarker tombstones) stored locally for bucket. prefix is an
// optional key-prefix filter (empty string = return all). Unreadable entries
// are silently skipped. Callers decide whether to filter tombstones.
func (m *LocalQuorumMetaStore) ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
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
		data, qerr := os.ReadFile(path)
		if qerr != nil {
			return nil
		}
		cmd, qerr := m.decodeQuorumMetaCmdBlob(data)
		if qerr != nil {
			return nil
		}
		results = append(results, cmd)
		return nil
	})
	return results, err
}

// ScanQuorumMetaBucketPage returns one latest-only quorum-meta page from the
// local store. The walk is lexicographic, so it can stop after maxKeys live
// entries plus one truncation probe instead of reading the whole prefix.
func (m *LocalQuorumMetaStore) ScanQuorumMetaBucketPage(bucket, prefix, marker string, maxKeys int) ([]PutObjectMetaCmd, bool, error) {
	if len(m.dataDirs) == 0 {
		return nil, false, nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
	bucketRoot := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketRoot); os.IsNotExist(err) {
		return nil, false, nil
	}
	var (
		results   []PutObjectMetaCmd
		truncated bool
	)
	if maxKeys > 0 {
		results = make([]PutObjectMetaCmd, 0, maxKeys)
	}
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if isQuorumMetaTempName(d.Name()) {
			return nil
		}
		key, rerr := filepath.Rel(bucketRoot, path)
		if rerr != nil {
			return nil
		}
		key = filepath.ToSlash(key)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		if marker != "" && key <= marker {
			return nil
		}
		data, qerr := os.ReadFile(path)
		if qerr != nil {
			return nil
		}
		cmd, qerr := m.decodeQuorumMetaCmdBlob(data)
		if qerr != nil {
			return nil
		}
		if cmd.IsDeleteMarker || cmd.IsHardDeleted {
			return nil
		}
		if len(results) >= maxKeys {
			truncated = true
			return filepath.SkipAll
		}
		results = append(results, cmd)
		return nil
	})
	return results, truncated, err
}

// scanQuorumMetaBucketStrict is the FAIL-CLOSED twin of ScanQuorumMetaBucket: a
// latest-only blob that cannot be read/decoded returns an error (rather than being
// silently skipped) so the segment-GC known-set is never silently incomplete — a
// dropped object would orphan-delete its live segments. Used by the non-versioned
// GC known-set builder (listNonVersionedBucketObjectsForGC). No prefix filter (the
// GC walks the whole bucket).
func (m *LocalQuorumMetaStore) scanQuorumMetaBucketStrict(bucket string) ([]PutObjectMetaCmd, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
	bucketRoot := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketRoot); os.IsNotExist(err) {
		return nil, nil
	}
	var results []PutObjectMetaCmd
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr // fail-closed: a walk error must not silently shrink the known-set
		}
		if d.IsDir() {
			return nil
		}
		if isQuorumMetaTempName(d.Name()) {
			return nil // in-flight atomic-publish temp, not a stored key
		}
		key, rerr := filepath.Rel(bucketRoot, path)
		if rerr != nil {
			return rerr
		}
		cmd, qerr := m.readQuorumMetaRawCmd(bucket, key)
		if qerr != nil {
			return fmt.Errorf("gc known-set: read latest blob %s/%s: %w", bucket, key, qerr)
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
func (m *LocalQuorumMetaStore) ScanQuorumMetaVersionsBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	bucketRoot := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir, bucket)
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
		cmd, derr := m.decodeQuorumMetaCmdBlob(data)
		if derr != nil {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(cmd.Key, prefix) {
			return nil
		}
		// Latest stays vid-primary (cross-vid max-VID); same-vid replicas dedup by
		// the full LWW comparator (quorumMetaCmdWins) for fan-out-order independence.
		if ex, ok := byKey[cmd.Key]; !ok || cmd.VersionID > ex.VersionID ||
			(cmd.VersionID == ex.VersionID && quorumMetaCmdWins(cmd, ex)) {
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
func (m *LocalQuorumMetaStore) ScanQuorumMetaVersionsBucketAll(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	bucketRoot := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir, bucket)
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
		cmd, derr := m.decodeQuorumMetaCmdBlob(data)
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
// silently skipping it. Consumed by the blob-authoritative snapshot capture path
// where a skipped-then-recovered blob would be captured-absent then purged.
func (m *LocalQuorumMetaStore) scanQuorumMetaVersionsBucketAllStrict(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(m.dataDirs) == 0 {
		return nil, nil
	}
	bucketRoot := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir, bucket)
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
		cmd, derr := m.decodeQuorumMetaCmdBlob(data)
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
	envb := buildShardEnvelope("ScanQuorumMetaVersions", bucket, prefix, 0, nil)
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
	envb := buildShardEnvelope("ScanQuorumMetaVersionsAll", bucket, prefix, 0, nil)
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
		// list, omitting versions from a blob-authoritative listing. (The per-key-max
		// tolerant clients keep skipping by design; this all-version path is the
		// blob-authority enumerator and must fail closed.)
		cmd, derr := s.decodeQuorumMetaCmdBlob(blob)
		if derr != nil {
			return nil, fmt.Errorf("decode scan quorum meta versions all response from %s: %w", addr, derr)
		}
		out = append(out, cmd)
	}
	return out, nil
}

// deleteQuorumMetaVersionLocalCore is the lock-free, epoch-free FS core for
// per-version blob removal. Absent file is not an error (idempotent). The caller
// is responsible for holding the appropriate lock before calling.
func (m *LocalQuorumMetaStore) deleteQuorumMetaVersionLocalCore(target string) error {
	err := os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("quorum meta version delete: %w", err)
	}
	return nil
}

// deleteQuorumMetaVersionLocal removes the local per-version blob for
// (bucket, key, versionID) under .quorum_meta_versions/{bucket}/{key}/{vid}.
// Absent file is not an error (idempotent). Mirrors deleteQuorumMetaLocal.
func (m *LocalQuorumMetaStore) deleteQuorumMetaVersionLocal(bucket, key, versionID string) error {
	if len(m.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaVersionsSubDir)
	target := filepath.Join(root, bucket, key, versionID)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta version delete: path %q/%q escapes root", key, versionID)
	}
	return m.deleteQuorumMetaVersionLocalCore(target)
}

// DeleteQuorumMetaVersion removes a per-version blob on a remote placement node.
// Mirrors DeleteQuorumMeta; the receiver runs deleteQuorumMetaVersionLocal.
// versionSubpath rides the envelope key field as path.Join(key, versionID).
func (s *ShardService) DeleteQuorumMetaVersion(ctx context.Context, addr, bucket, key, versionID string) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta version: no transport")
	}
	envb := buildShardEnvelope("DeleteQuorumMetaVersion", bucket, path.Join(key, versionID), 0, nil)
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

// deleteQuorumMetaLocal removes the local latest-only quorum-meta blob for
// (bucket, key) under {dataDirs[0]}/.quorum_meta/{bucket}/{key}.
// Absent file is not an error (idempotent). Mirrors deleteQuorumMetaVersionLocal.
func (m *LocalQuorumMetaStore) deleteQuorumMetaLocal(bucket, key string) error {
	if len(m.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta delete: key %q escapes root", key)
	}
	return m.deleteQuorumMetaVersionLocalCore(target)
}

// DeleteQuorumMeta removes the latest-only quorum-meta blob on a remote
// placement node. Mirrors DeleteQuorumMetaVersion; the receiver runs
// deleteQuorumMetaLocal.
func (s *ShardService) DeleteQuorumMeta(ctx context.Context, addr, bucket, key string) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta delete: no transport")
	}
	envb := buildShardEnvelope("DeleteQuorumMeta", bucket, key, 0, nil)
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

// deleteQuorumMetaQuorum deletes the latest-only quorum-meta blob on every
// placement node. FAIL-CLOSED: returns the first error encountered so a blob
// left on a missed node cannot resurface the object via derive-by-scan.
func (b *DistributedBackend) deleteQuorumMetaQuorum(ctx context.Context, bucket, key string, nodeIDs []string) error {
	return b.qmsOrBuild().deleteQuorumMetaQuorum(ctx, bucket, key, nodeIDs)
}

// deleteShardsQuorum deletes EC shards on every placement node synchronously
// and fail-closed (returns the first error). Unlike deleteShardsAsync, errors
// are NOT silenced — the caller must confirm shards are gone before removing
// the qmeta blob to avoid stranded shards with no placement record.
// self → DeleteLocalShards; peers → DeleteShards RPC.
func (b *DistributedBackend) deleteShardsQuorum(ctx context.Context, bucket, shardKey string, placement []string) error {
	if b.shardSvc == nil {
		return nil
	}
	self := b.currentSelfAddr()
	dctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	var firstErr error
	for _, node := range placement {
		var err error
		if node == self {
			err = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		} else {
			err = b.shardSvc.DeleteShards(dctx, node, bucket, shardKey)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// decodeQuorumMetaCmdBlob decodes a raw quorum meta blob to a PutObjectMetaCmd.
// The blob is a bare PutObjectMetaCmd FlatBuffer (no raft Command envelope).
func (m *LocalQuorumMetaStore) decodeQuorumMetaCmdBlob(data []byte) (PutObjectMetaCmd, error) {
	cmd, err := decodeQuorumMetaBlob(data)
	if err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("quorum meta decode raw: %w", err)
	}
	return cmd, nil
}

// readQuorumMetaRawCmd reads and decodes the PutObjectMetaCmd from the local
// quorum meta store. Returns storage.ErrObjectNotFound if the file is absent.
// Use DistributedBackend.readQuorumMetaCmd when peer fallback is needed.
func (m *LocalQuorumMetaStore) readQuorumMetaRawCmd(bucket, key string) (PutObjectMetaCmd, error) {
	data, err := m.readQuorumMetaRaw(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return m.decodeQuorumMetaCmdBlob(data)
}

// ReadQuorumMetaRaw fetches the raw quorum meta blob from a remote peer via
// the shard transport. Returns (nil, nil) when the peer has no file for the
// object (not-found is not an error — caller treats nil data as a miss).
func (s *ShardService) ReadQuorumMetaRaw(ctx context.Context, addr, bucket, key string) ([]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta read: no transport")
	}
	envb := buildShardEnvelope("ReadQuorumMeta", bucket, key, 0, nil)
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

// ReadQuorumMetaRawBatch fetches raw quorum meta blobs for several keys from one
// remote peer. Missing keys are omitted from the returned map.
func (s *ShardService) ReadQuorumMetaRawBatch(ctx context.Context, addr, bucket string, keys []string) (map[string][]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta batch read: no transport")
	}
	envb := buildShardEnvelope("ReadQuorumMetaBatch", bucket, "", 0, packStringList(keys))
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("batch read quorum meta from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("unmarshal quorum meta batch read response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote quorum meta batch read error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil
	}
	blobs, err := unpackKeyBlobMap(data)
	if err != nil {
		return nil, fmt.Errorf("unpack quorum meta batch read response: %w", err)
	}
	return blobs, nil
}

// readQuorumMetaCmd is the DistributedBackend-level read for PutObjectMetaCmd,
// with peer fan-out fallback when the local quorum meta file is absent.
// Used by SetObjectACLPropose, SetObjectTagsPropose, and AppendObject migration.
func (b *DistributedBackend) readQuorumMetaCmd(bucket, key string) (PutObjectMetaCmd, error) {
	return b.qmsOrBuild().readQuorumMetaCmd(bucket, key)
}

// WriteQuorumMeta sends the quorum meta blob to a remote placement node via the
// shard transport (thin mirror of WriteShard, routed to the quorum-meta handler).
func (s *ShardService) WriteQuorumMeta(ctx context.Context, addr, bucket, key string, data []byte) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta: no transport")
	}
	envb := buildShardEnvelope("WriteQuorumMeta", bucket, key, 0, data)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("write quorum meta to %s: %w", addr, err)
	}
	rpcType, body, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta response: %w", err)
	}
	if rpcType == "Error" {
		// Preserve the CAS-reject signal across the RPC boundary (BUG-1): a remote
		// replica's CAS reject must surface as errQuorumMetaCASReject so the
		// fanOut caller counts it like a local reject.
		return quorumMetaWriteRPCError(addr, body)
	}
	return nil
}

// WriteQuorumMetaVersion sends an immutable per-version quorum-meta blob to a
// remote placement node. versionSubpath is path.Join(key, versionID); it rides
// the existing shard envelope's key field (no schema change) and the receiver
// writes it under the .quorum_meta_versions subtree.
func (s *ShardService) WriteQuorumMetaVersion(ctx context.Context, addr, bucket, versionSubpath string, data []byte) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta version: no transport")
	}
	envb := buildShardEnvelope("WriteQuorumMetaVersion", bucket, versionSubpath, 0, data)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("write quorum meta version to %s: %w", addr, err)
	}
	rpcType, body, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta version response: %w", err)
	}
	if rpcType == "Error" {
		// Preserve the CAS-reject signal across the RPC boundary (BUG-1), mirroring
		// WriteQuorumMeta. The per-version writer is LWW today, but a CAS reject must
		// never be flattened to a generic error on this path either.
		return quorumMetaWriteRPCError(addr, body)
	}
	return nil
}

// IterQuorumMetaECShardTargets walks all quorum meta files under
// {dataDirs[0]}/.quorum_meta/ and emits ECShardScanTarget entries for every
// object whose segments or coalesced refs carry EC placement. Used by
// ShardPlacementMonitor.Scan to cover Phase 3 objects that bypass BadgerDB.
//
// fn returning a non-nil error stops iteration.
func (m *LocalQuorumMetaStore) IterQuorumMetaECShardTargets(fn func(ECShardScanTarget) error) error {
	if len(m.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaSubDir)
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
		obj, _, qerr := m.readQuorumMetaLocalDecoded(bucket, key)
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

func packStringList(items []string) []byte {
	blobs := make([][]byte, 0, len(items))
	for _, item := range items {
		blobs = append(blobs, []byte(item))
	}
	return packBlobList(blobs)
}

func unpackStringList(data []byte) ([]string, error) {
	blobs, err := unpackBlobList(data)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(blobs))
	for _, blob := range blobs {
		out = append(out, string(blob))
	}
	return out, nil
}

func packKeyBlobMap(blobs map[string][]byte) []byte {
	pairs := make([][]byte, 0, len(blobs)*2)
	for key, blob := range blobs {
		pairs = append(pairs, []byte(key), blob)
	}
	return packBlobList(pairs)
}

func unpackKeyBlobMap(data []byte) (map[string][]byte, error) {
	pairs, err := unpackBlobList(data)
	if err != nil {
		return nil, err
	}
	if len(pairs)%2 != 0 {
		return nil, fmt.Errorf("unpack key blob map: odd pair count")
	}
	out := make(map[string][]byte, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out[string(pairs[i])] = append([]byte(nil), pairs[i+1]...)
	}
	return out, nil
}

func packScanQuorumMetaPageArgs(marker string, maxKeys int) []byte {
	if maxKeys < 0 {
		maxKeys = 0
	}
	data := make([]byte, 4+len(marker))
	binary.BigEndian.PutUint32(data[:4], uint32(maxKeys))
	copy(data[4:], marker)
	return data
}

func unpackScanQuorumMetaPageArgs(data []byte) (string, int, error) {
	if len(data) < 4 {
		return "", 0, fmt.Errorf("scan quorum meta page args: truncated")
	}
	maxKeys := int(binary.BigEndian.Uint32(data[:4]))
	return string(data[4:]), maxKeys, nil
}

// ScanQuorumMeta fans out a ScanQuorumMeta RPC to a remote node and returns
// all PutObjectMetaCmds (including tombstones) for the given bucket and prefix.
func (s *ShardService) ScanQuorumMeta(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("scan quorum meta: no transport")
	}
	envb := buildShardEnvelope("ScanQuorumMeta", bucket, prefix, 0, nil)
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

// ScanQuorumMetaPage returns one lexicographic page of live quorum-meta entries
// from a remote node.
func (s *ShardService) ScanQuorumMetaPage(ctx context.Context, addr, bucket, prefix, marker string, maxKeys int) ([]PutObjectMetaCmd, bool, error) {
	if s.transport == nil {
		return nil, false, fmt.Errorf("scan quorum meta page: no transport")
	}
	if maxKeys < 0 {
		maxKeys = 0
	}
	envb := buildShardEnvelope("ScanQuorumMetaPage", bucket, prefix, 0, packScanQuorumMetaPageArgs(marker, maxKeys+1))
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, false, fmt.Errorf("scan quorum meta page from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, false, fmt.Errorf("unmarshal scan quorum meta page response: %w", err)
	}
	if rpcType == "Error" {
		return nil, false, fmt.Errorf("remote scan quorum meta page error from %s", addr)
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	blobs, err := unpackBlobList(data)
	if err != nil {
		return nil, false, fmt.Errorf("unpack scan quorum meta page response: %w", err)
	}
	truncated := len(blobs) > maxKeys
	if truncated {
		blobs = blobs[:maxKeys]
	}
	cmds := make([]PutObjectMetaCmd, 0, len(blobs))
	for _, blob := range blobs {
		cmd, qerr := s.decodeQuorumMetaCmdBlob(blob)
		if qerr == nil {
			cmds = append(cmds, cmd)
		}
	}
	return cmds, truncated, nil
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
	return b.qmsOrBuild().ScanObjectMetaEntries(ctx, bucket, prefix)
}

func (b *DistributedBackend) scatterGatherList(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return b.qmsOrBuild().scatterGatherList(ctx, bucket, prefix)
}

// filterAndSortEntries removes tombstones and sorts by key.
func filterAndSortEntries(entries []PutObjectMetaCmd) []PutObjectMetaCmd {
	out := entries[:0]
	for _, e := range entries {
		// Drop both tombstone kinds: IsDeleteMarker (non-versioned/Suspended soft
		// delete) and IsHardDeleted (defense-in-depth — the latest-only tree carries
		// no hard-delete tombstone today, but a future writer must never surface one
		// as a live LIST entry).
		if e.IsDeleteMarker || e.IsHardDeleted {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func removeBucketChild(root, bucket, label string) error {
	target := filepath.Join(root, bucket)
	rel, rerr := filepath.Rel(root, target)
	if rerr != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("remove bucket %s: %q escapes root", label, bucket)
	}
	if err := os.RemoveAll(target); err != nil {
		return fmt.Errorf("remove bucket %s %s: %w", label, bucket, err)
	}
	return nil
}

// RemoveBucketMetaTrees physically removes a bucket's off-raft quorum-meta blob
// trees (.quorum_meta/{bucket} and .quorum_meta_versions/{bucket}) under every
// data dir. Called on bucket delete: os.RemoveAll(bucketDir) only clears the
// data subtree and leaves these blob trees (incl. hard-delete tombstone blobs
// written by purgePerVersionBlobs) behind. Idempotent.
func (s *ShardService) RemoveBucketMetaTrees(bucket string) error {
	for _, dataDir := range s.DataDirs() {
		for _, sub := range []string{quorumMetaSubDir, quorumMetaVersionsSubDir} {
			root := filepath.Join(dataDir, sub)
			if err := removeBucketChild(root, bucket, sub); err != nil {
				return err
			}
		}
	}
	return nil
}

// RemoveBucketPhysicalTrees removes all node-local physical bucket residue:
// legacy {root}/data/{bucket} plus the off-raft quorum-meta trees under
// {root}/data/shards. It is idempotent and safe to run via peer RPC after the
// meta-Raft DeleteBucket has committed.
func (s *ShardService) RemoveBucketPhysicalTrees(bucket string) error {
	for _, shardRoot := range s.DataDirs() {
		dataRoot := filepath.Dir(shardRoot)
		if err := removeBucketChild(dataRoot, bucket, "data tree"); err != nil {
			return err
		}
	}
	return s.RemoveBucketMetaTrees(bucket)
}
