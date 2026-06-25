package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// shardFileRe matches a canonical EC shard file name (shard_<N>). The orphan
// walker treats a directory holding ≥1 such file as a shard-leaf dir.
var shardFileRe = regexp.MustCompile(`^shard_\d+$`)

// minOrphanShardAge floors the orphan-shard age gate ABOVE the worst-case healthy
// EC write→commit wall-clock, so a shard is never reclaimed while its write may
// still be in flight. An EC write renames each shard_N BEFORE the metadata commit
// (object_put.go), and the slowest shard takes up to ecShardWriteAttempts ×
// (shardRPCTimeout + ecShardWriteBackoff) — a remote shard write retries that many
// times, each bounded by shardRPCTimeout. The commit then fans the quorum-meta blob
// out to the placement nodes; a coalesce publish CAS-retries up to
// maxCoalesceCASRetries, each bounded by quorumMetaReadTimeout + quorumMetaWriteTimeout
// (the widest commit window, which also dominates the plain full-object commit of
// two quorum-meta writes). Flooring above their sum guarantees that once a shard is
// age-eligible its write has either committed (→ kept by hasLiveShardRecord, which
// also fail-closes on an unreachable peer) or definitively failed (→ reclaimed).
//
// The legacy floor was only 2*proposeForwardTimeout (60s), which ignored the
// multi-minute shard-write window: a slow in-flight write whose early shard_N was
// renamed >60s before its commit could have that shard reclaimed mid-write — a
// pre-2026-06 data-loss bug. Reclaim is a background sweep, so the larger floor only
// delays reclaim of genuine orphans (harmless).
const minOrphanShardAge = ecShardWriteAttempts*(shardRPCTimeout+ecShardWriteBackoff) +
	(1+maxCoalesceCASRetries)*(quorumMetaReadTimeout+quorumMetaWriteTimeout)

// segStagingReclaimAge is the age floor for reclaiming an abandoned .segstaging
// staged shard-leaf. It must exceed the FULL server-side lifetime of one chunked
// PUT / multipart-complete — which writes ALL of an object's segments to staging
// before the commit-time promote, with each leaf's mtime fixed at write time — not
// just a single shard's in-flight window (minOrphanShardAge ~466s). A large/slow
// object whose whole segment-write→promote span exceeds that per-shard floor would
// otherwise have its early staging leaves reclaimed WHILE THE PUT IS STILL IN
// FLIGHT, then fail its commit-time promote (a spurious large-upload failure). The
// staging shards of a chunked PUT / CompleteMultipartUpload are written synchronously
// within the single request (UploadPart does not stage), so the span is bounded by
// one request; a generous fixed floor cleanly separates "abandoned" (crash / failed
// PUT / LWW loser) from "still in flight" at any realistic object size, with no
// per-upload registry. Staging reclaim is not latency-sensitive: a staged dir
// lingering well past any real request is only wasted disk, never data.
const segStagingReclaimAge = 24 * time.Hour

// SegStagingPrefix is the reserved bucket-relative path component under which a
// chunked-PUT / multipart-complete stages its segment EC shards before the
// commit-time promote: a real staging leaf is always
// <bucket>/.segstaging/<txn>/<blobID> (see segmentStagingShardKey), i.e. the
// component DIRECTLY under the bucket. The orphan walker anchors its staging
// branch on relParts[1]==SegStagingPrefix so a user object whose KEY merely
// contains ".segstaging" deeper in the path (e.g. "foo/.segstaging/bar") is NOT
// misclassified as staging and instead falls through to the regular path.
const SegStagingPrefix = ".segstaging"

// SetOrphanShardSweepGate wires the boot-computed predicate that permits the EC
// full-object orphan-shard sweep. Default (unset) is fail-closed: the sweep
// never runs. Call once during boot, before the scrubber starts.
func (b *DistributedBackend) SetOrphanShardSweepGate(gate func() bool) {
	b.orphanShardSweepGate = gate
}

// SetFrozenObjectVersionSource injects the snapshot Manager's
// AllFrozenObjectVersions so this backend can supply the snapshot half of the
// orphan-shard known-set. Call once during boot, before the scrubber starts.
func (b *DistributedBackend) SetFrozenObjectVersionSource(fn func() ([]storage.SnapshotObjectRef, error)) {
	b.frozenObjVersionSrc = fn
}

// SetHostedGroupBackendsSource wires the set of locally-hosted data-group
// backends (including this one) so the orphan-shard sweep can union the live
// versioned-set across all of them. Call once during boot, before the scrubber
// starts. nil/un-wired => the sweep treats this backend as the only group.
func (b *DistributedBackend) SetHostedGroupBackendsSource(fn func() []*DistributedBackend) {
	b.hostedGroupBackendsSrc = fn
}

// SetOwningGroupHostedChecker wires the predicate that reports whether a bucket's
// owning data group is locally hosted. Used to keep (never delete) shards the
// balancer floated in from groups this node does not host. Call once during boot,
// before the scrubber starts. nil/un-wired => every bucket is treated as locally
// owned (single-group).
func (b *DistributedBackend) SetOwningGroupHostedChecker(fn func(bucket string) bool) {
	b.owningGroupHostedFn = fn
}

// hostedGroupBackends returns the locally-hosted group backends to judge
// candidates against. Defaults to just this backend when un-wired (single-group).
func (b *DistributedBackend) hostedGroupBackends() []*DistributedBackend {
	if b.hostedGroupBackendsSrc == nil {
		return []*DistributedBackend{b}
	}
	return b.hostedGroupBackendsSrc()
}

// owningGroupHosted reports whether the bucket's owning group is locally hosted
// (so this node has authoritative metadata to judge its shards). Defaults to true
// when un-wired (single-group).
func (b *DistributedBackend) owningGroupHosted(bucket string) bool {
	if b.owningGroupHostedFn == nil {
		return true
	}
	return b.owningGroupHostedFn(bucket)
}

// allFrozenObjectVersionDirs maps every snapshot-frozen full-object version to
// its canonical (dataDirs[0]-rooted) shard dir. Fails closed when no source is
// wired or the source errors: the caller skips the whole sweep this cycle, so an
// un-wired/erroring backend never sweeps against an incomplete known-set.
func (b *DistributedBackend) allFrozenObjectVersionDirs() (map[string]bool, error) {
	if b.frozenObjVersionSrc == nil || b.shardSvc == nil {
		return nil, fmt.Errorf("frozen object-version source not wired")
	}
	refs, err := b.frozenObjVersionSrc()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(refs))
	for _, r := range refs {
		dir, derr := b.shardSvc.getShardDir(r.Bucket, r.Key+"/"+r.VersionID, 0)
		if derr != nil {
			continue // an escaping ref cannot match a real on-disk dir
		}
		out[filepath.Clean(dir)] = true
	}
	return out, nil
}

// orphanShardSweepAllowed reports whether the feature gate + caught-up gate both
// permit the sweep right now, re-evaluated on every call (membership and
// replication lag change at runtime). The caught-up gate covers EVERY hosted
// group: a lagging sibling group's FSM could otherwise mark its own
// committed-but-not-yet-applied shard an orphan.
func (b *DistributedBackend) orphanShardSweepAllowed() bool {
	if b.shardSvc == nil {
		return false
	}
	if b.orphanShardSweepGate == nil || !b.orphanShardSweepGate() {
		return false // disabled / un-wired: fail-closed
	}
	for _, gb := range b.hostedGroupBackends() {
		if gb == nil || !gb.CaughtUp(context.Background()) {
			return false // lagging (or missing) hosted group could mark a committed shard orphan
		}
	}
	return true
}

// WalkOrphanShards yields each full-object EC shard dir on disk that is not
// backed by live metadata (across ALL locally-hosted groups), a snapshot pin, or
// the scrubber's latest-only known-set, and is older than the (floored) age gate.
// It is fully self-gated: it no-ops (yields nothing) unless the feature gate, the
// all-hosted caught-up gate, and a complete snapshot known-set are all satisfied,
// and it keeps any shard whose owning group is not locally hosted (balancer-
// floated, unjudgeable) — so the shared scrubber never needs to know any of this.
// Implements scrubber.OrphanWalkable.
func (b *DistributedBackend) WalkOrphanShards(known map[string]bool, fn func(dir string) error) error {
	if !b.orphanShardSweepAllowed() {
		return nil
	}
	frozen, err := b.allFrozenObjectVersionDirs()
	if err != nil {
		return nil // fail-closed: never sweep without the full snapshot known-set
	}

	dataDirs := b.shardSvc.DataDirs()
	if len(dataDirs) == 0 {
		return nil
	}
	cutoff := time.Now().Add(-b.effectiveOrphanShardAge())
	// .segstaging leaves use a separate, far more generous floor (see
	// segStagingReclaimAge): their mtime is fixed when the segment is written, so a
	// large in-flight PUT's early staging leaf can age past the per-shard cutoff
	// while the PUT is still running. The generous floor exceeds the whole
	// segment-write→promote span of one request.
	stagingCutoff := time.Now().Add(-segStagingReclaimAge)
	seen := make(map[string]bool)

	for _, dataDir := range dataDirs {
		stopErr := b.walkOneShardRoot(dataDir, dataDirs[0], known, frozen, seen, cutoff, stagingCutoff, fn)
		if stopErr != nil {
			return stopErr
		}
	}
	return nil
}

// effectiveOrphanShardAge is the configured orphan age gate, floored to
// minOrphanShardAge so an in-flight EC write's full write+commit window (the
// bounded shard-RPC retries plus the quorum-meta commit) can never reach it.
func (b *DistributedBackend) effectiveOrphanShardAge() time.Duration {
	age := b.scrubOrphanAge
	if age < minOrphanShardAge {
		age = minOrphanShardAge
	}
	return age
}

func (b *DistributedBackend) walkOneShardRoot(
	dataDir, canonRoot string,
	known, frozen, seen map[string]bool,
	cutoff, stagingCutoff time.Time,
	fn func(dir string) error,
) error {
	var stopErr error
	walkErr := filepath.WalkDir(dataDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		entries, readErr := os.ReadDir(p)
		if readErr != nil {
			return filepath.SkipDir
		}
		hasShard, hasTmp, newest := classifyShardDir(entries)
		if !hasShard {
			return nil // intermediate dir → descend
		}
		// p is a shard-leaf dir. Never descend past it.
		if hasTmp {
			return filepath.SkipDir // in-flight write
		}
		rel, relErr := filepath.Rel(dataDir, p)
		if relErr != nil {
			return filepath.SkipDir
		}
		rel = filepath.ToSlash(rel)
		relParts := strings.Split(rel, "/")
		if len(relParts) >= 2 && relParts[1] == SegStagingPrefix {
			// Segment staging area. A REAL staging leaf is always
			// <bucket>/.segstaging/<txn>/<blobID> (segmentStagingShardKey), so the staging
			// namespace is the component DIRECTLY under the bucket (relParts[1]). We anchor on
			// that, NOT a broad strings.Contains: a user key like "foo/.segstaging/bar" has
			// relParts[1]=="foo" and must fall through to the regular path below.
			//
			// PR2 ages out ABANDONED staging leaves (crash between promote and commit, failed
			// PUT, LWW-loser completer) here. The model is DELETE-TIME LIVENESS, not a write-edge
			// invariant: we never assume staging is a closed namespace; instead we keep ANY
			// committed (live) object, however it was written, by reusing the SAME full-object
			// liveness used on the regular path. The order is exact:
			//
			//   1. /segments/ exclusion (FIRST): a real staging leaf is exactly
			//      .segstaging/<txn>/<blobID> and NEVER contains "/segments/". So a
			//      .segstaging-prefixed path WITH "/segments/" is a CHUNKED user object keyed
			//      under ".segstaging" whose committed segments live at
			//      .segstaging/<key>/segments/<blobID>; skip it wholesale exactly like the
			//      regular /segments/ branch. This is what lets the rest of this branch use ONLY
			//      full-object liveness and NEVER consult the (abandoned cross-node / Suspended /
			//      never-versioned) SEGMENT liveness oracle.
			//   2. Age gate: newest > stagingCutoff (segStagingReclaimAge ~24h) → KEEP. ALL of an
			//      object's segments are staged before the commit-time promote and each leaf's
			//      mtime is fixed at write time, so a large/slow in-flight PUT's early staging leaf
			//      can age past the per-shard cutoff (~466s) while the PUT is still running. The
			//      generous staging floor exceeds the whole segment-write→promote span of one
			//      request, so only a leaf no in-flight request can still own reaches reclaim.
			//   3. Full-object liveness — IDENTICAL to the regular path: the remaining candidate is
			//      a real abandoned staging leaf OR a NON-chunked full-object user object keyed
			//      ".segstaging/foo" (no "/segments/"). A real leaf .segstaging/<txn>/<blobID> →
			//      parseFullObjectRel sees key ".segstaging/<txn>", vid "<blobID>" → no committed
			//      object → (false, certain) in a healthy cluster → reclaim; degraded → uncertain →
			//      kept. A live ".segstaging/foo" full object → (true, …) → kept. Non-hosted /
			//      uncertain → keep (fail-closed).
			//
			// Reclaim is a DIRECT os.RemoveAll — NOT routed through fn/DeleteOrphanDir, which
			// reconfirms via a parseable full-object key that a real .segstaging leaf is not.
			// After the leaf is gone, remove its empty <txn>/ parent dir best-effort.
			if strings.Contains(rel, "/segments/") {
				return filepath.SkipDir // chunked user object keyed under .segstaging → keep
			}
			if newest.After(stagingCutoff) {
				return filepath.SkipDir // recent → could be a live in-flight PUT → keep
			}
			canonical := filepath.Clean(filepath.Join(canonRoot, rel))
			if known[canonical] || frozen[canonical] || seen[canonical] {
				return filepath.SkipDir // latest-known or snapshot-pinned
			}
			// LOAD-BEARING INVARIANT: this reclaim's liveness check is correct only
			// because parseFullObjectRel's bucket (parts[0]) is the PHYSICAL owner of
			// rel's directory, so hasLiveShardRecord queries the right bucket. That
			// parsed-bucket == physical-owner identity holds because every shard write
			// goes through getShardDir → ShardPathUnderDataDir, which enforces per-bucket
			// containment (rejects "../"-escapes and non-segment bucket names), so a key
			// can never land under a DIFFERENT bucket's .segstaging tree. If that
			// containment is ever weakened, this age-out (and the regular orphan path
			// below, which makes the same assumption) must be re-examined.
			bkt, fkey, vid, okF := parseFullObjectRel(rel)
			if !okF {
				// Unparseable staging-prefixed leaf (e.g. a bare-legacy object keyed
				// EXACTLY ".segstaging" → rel "<bucket>/.segstaging", len 2). Mirror the
				// regular full-object path's okF=false treatment EXACTLY: keep, never
				// delete. Falling through to os.RemoveAll here would reclaim a committed
				// object we cannot even resolve a liveness record for = data loss.
				return filepath.SkipDir
			}
			if !b.owningGroupHosted(bkt) {
				return filepath.SkipDir // balancer-floated from a non-hosted group → can't judge → keep
			}
			if recLive, certain := b.hasLiveShardRecord(bkt, fkey, vid); recLive || !certain {
				return filepath.SkipDir // live full object OR uncertain → keep (fail-closed)
			}
			// Only a parseable, hosted, certainly-dead staging leaf reaches reclaim.
			if rerr := os.RemoveAll(p); rerr != nil {
				stopErr = fmt.Errorf("reclaim staging %q: %w", p, rerr)
				return filepath.SkipDir
			}
			removeEmptySegStagingTxnDir(p)
			metrics.SegStagingReclaimed.Inc()
			return filepath.SkipDir // reclaimed → never descend past a removed leaf
		}
		if strings.Contains(rel, "/segments/") {
			// Segments stay a WHOLESALE Contains-skip (NOT parse-routed like coalesced
			// below): there is no parseSegmentRel, and a real segment shard falling
			// through to parseFullObjectRel would be reclaimed as a fake full-object
			// orphan = data loss on live append data. The coalesced/segments asymmetry
			// is deliberate. (A full-object key containing a "segments" component is kept
			// here — a pre-existing, safe leak; segment reclaim is a separate future slice.)
			return filepath.SkipDir
		}
		if newest.After(cutoff) {
			return filepath.SkipDir // age gate (minOrphanShardAge already covers the coalesce publish window)
		}
		canonical := filepath.Clean(filepath.Join(canonRoot, rel))
		if known[canonical] || frozen[canonical] || seen[canonical] {
			return filepath.SkipDir // latest-known or snapshot-pinned
		}
		seen[canonical] = true
		// Route on a PARSE-validated coalesced shape (penultimate == "coalesced"), NOT
		// strings.Contains: a full-object key with a "coalesced" path component (e.g.
		// bkt/coalesced/foo.txt/v1) contains "/coalesced/" but is NOT a coalesced shard —
		// a Contains branch would intercept it, never reach parseFullObjectRel, and leak
		// it forever. parseCoalescedRel fails on such a path → fall through to full-object.
		if _, _, _, okCoal := parseCoalescedRel(rel); okCoal {
			if b.coalescedShardReclaimable(rel) {
				if ferr := fn(canonical); ferr != nil {
					stopErr = ferr
					return filepath.SkipAll
				}
			}
			return filepath.SkipDir // handled (reclaimed or kept) — never descend
		}
		bucket, key, versionID, ok := parseFullObjectRel(rel)
		if !ok {
			return filepath.SkipDir // unversioned / unparseable → keep (leak, never delete)
		}
		if !b.owningGroupHosted(bucket) {
			return filepath.SkipDir // balancer-floated from a non-hosted group → can't judge → keep
		}
		// Not a live versioned object (the forward live-set above is bijective for
		// those). Remaining live candidate is a regular-PUT object: quorum-meta is
		// stored under the SAME cleaned key path, so the reverse-parsed key matches
		// and the peer-fallback point-lookup is correct.
		recLive, certain := b.hasLiveShardRecord(bucket, key, versionID)
		if recLive || !certain {
			return filepath.SkipDir // live OR uncertain → keep (fail-closed)
		}
		if ferr := fn(canonical); ferr != nil {
			stopErr = ferr
			return filepath.SkipAll
		}
		return filepath.SkipDir
	})
	if walkErr != nil {
		return fmt.Errorf("walk shard root %s: %w", dataDir, walkErr)
	}
	return stopErr
}

func removeEmptySegStagingTxnDir(stagingLeaf string) {
	parent := filepath.Dir(stagingLeaf)
	if err := os.Remove(parent); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTEMPTY) {
		return
	}
}

// classifyShardDir reports whether dir entries contain ≥1 shard_<N> file, any
// in-flight *.tmp file, and the newest shard-file mod time.
func classifyShardDir(entries []os.DirEntry) (hasShard, hasTmp bool, newest time.Time) {
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			hasTmp = true
			continue
		}
		if !shardFileRe.MatchString(name) {
			continue
		}
		hasShard = true
		if info, ierr := e.Info(); ierr == nil && info.ModTime().After(newest) {
			newest = info.ModTime()
		}
	}
	return hasShard, hasTmp, newest
}

// parseFullObjectRel splits a dataDir-relative shard-leaf path
// `<bucket>/<key…>/<versionID>` (key may contain '/'). Rejects (ok=false) a path
// with fewer than 3 components (e.g. an unversioned `<bucket>/<key>` dir), which
// the walker then keeps (leak, never delete).
func parseFullObjectRel(rel string) (bucket, key, versionID string, ok bool) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 3 {
		return "", "", "", false
	}
	bucket = parts[0]
	versionID = parts[len(parts)-1]
	key = strings.Join(parts[1:len(parts)-1], "/")
	if bucket == "" || key == "" || versionID == "" {
		return "", "", "", false
	}
	return bucket, key, versionID, true
}

// parseCoalescedRel splits a dataDir-relative coalesced-shard leaf path
// `<bucket>/<key…>/coalesced/<coalescedID>` (key may contain '/'). The literal
// `coalesced` MUST be the second-to-last component — that is the shape the EC writer
// produces (ShardKey = "<key>/coalesced/<coalescedID>"). Rejects (ok=false) anything
// else; the walker then routes such a path to the full-object branch instead.
func parseCoalescedRel(rel string) (bucket, key, coalescedID string, ok bool) {
	parts := strings.Split(strings.TrimSuffix(filepath.ToSlash(rel), "/"), "/")
	if len(parts) < 4 || parts[len(parts)-2] != "coalesced" {
		return "", "", "", false
	}
	bucket = parts[0]
	coalescedID = parts[len(parts)-1]
	key = strings.Join(parts[1:len(parts)-2], "/")
	if bucket == "" || key == "" || coalescedID == "" {
		return "", "", "", false
	}
	return bucket, key, coalescedID, true
}

// hasLiveShardRecord reports whether (bucket,key,versionID) is backed by a live
// object record in the off-raft quorum-meta blob store, and whether that
// determination is CERTAIN. Fail-closed: any read uncertainty returns
// (false, false) so the caller keeps the shards.
func (b *DistributedBackend) hasLiveShardRecord(bucket, key, versionID string) (live, certain bool) {
	// Versioning-enabled: the per-version blob is the shard-liveness authority
	// (live iff a blob exists that is neither a hard-delete tombstone nor a delete
	// marker). A per-version MISS means the version is gone → orphan-eligible.
	if on, serr := b.blobAuthReadOn(bucket); serr != nil {
		return false, false // uncertain → keep
	} else if on {
		cmd, ok, err := b.readQuorumMetaVersionDecodeStrict(bucket, key, versionID)
		if err != nil {
			return false, false // uncertain → keep
		}
		if ok {
			return !cmd.IsHardDeleted && !cmd.IsDeleteMarker, true
		}
		return false, true // no per-version blob → orphan-eligible
	}

	// Non-versioned: certainty-aware reclaim read. Plain readQuorumMeta maps an
	// exhausted peer fan-out to ErrObjectNotFound even when a metadata-holding peer
	// was merely unreachable, so reclaiming on its NotFound could delete a live
	// object whose quorum-meta sits on a briefly-down peer (K-of-N).
	// readQuorumMetaForReclaim fails CLOSED on any peer-uncertainty; a negative
	// (absent / different-version) claim is honored only when the responding set is
	// complete.
	obj, found, certain := b.readQuorumMetaForReclaim(bucket, key)
	if !certain {
		return false, false // peer-uncertainty or read error → keep
	}
	if found && obj != nil {
		if obj.VersionID == versionID && !obj.IsDeleteMarker {
			return true, true // live regular-PUT (this exact version is the winner)
		}
		return false, true // a different/newer version won → this one is overwritten
	}
	return false, true // proven absent in quorum-meta → orphan-eligible
}

// DeleteOrphanDir removes one full-object EC shard dir across every dataDir
// (shards are striped) and returns the number of shard files deleted. It
// RE-VALIDATES the orphan decision immediately before deleting (the scrubber's
// tombstone loop calls this a cycle after the walk, and the gate/liveness can
// change in between): if the sweep is no longer allowed, the dir is now
// snapshot-pinned, or it is now live/uncertain, it returns (0, nil) without
// deleting. Implements scrubber.OrphanWalkable.
func (b *DistributedBackend) DeleteOrphanDir(canonical string) (int, error) {
	if b.shardSvc == nil {
		return 0, nil
	}
	if !b.orphanShardSweepReconfirm(canonical) {
		return 0, nil // revalidation failed → do not delete
	}
	dataDirs := b.shardSvc.DataDirs()
	if len(dataDirs) == 0 {
		return 0, nil
	}
	rel, err := filepath.Rel(dataDirs[0], filepath.Clean(canonical))
	if err != nil {
		return 0, fmt.Errorf("orphan delete: rel %q: %w", canonical, err)
	}
	rel = filepath.ToSlash(rel)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return 0, fmt.Errorf("orphan delete: %q escapes shard root", canonical)
	}
	// Canonical-wide in-flight guard: the per-dataDir walk only saw ONE striped
	// instance, but a concurrent write/repair may be touching the SAME version
	// dir under a DIFFERENT dataDir (a .tmp or a shard newer than the age gate).
	// Deleting then would wipe an in-flight shard. Check every instance.
	if b.canonicalInflightOrFresh(dataDirs, rel) {
		return 0, nil
	}
	total := 0
	for _, dataDir := range dataDirs {
		phys := filepath.Join(dataDir, filepath.FromSlash(rel))
		if entries, rerr := os.ReadDir(phys); rerr == nil {
			for _, e := range entries {
				if !e.IsDir() && shardFileRe.MatchString(e.Name()) {
					total++
				}
			}
		}
		if rerr := os.RemoveAll(phys); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			return total, fmt.Errorf("orphan delete %q: %w", phys, rerr)
		}
	}
	return total, nil
}

// canonicalInflightOrFresh reports whether ANY dataDir's instance of rel holds
// an in-flight *.tmp write or a shard file newer than the (floored) age gate.
// Used at delete time so a striped write/repair under a non-walked dataDir
// cannot be wiped.
func (b *DistributedBackend) canonicalInflightOrFresh(dataDirs []string, rel string) bool {
	cutoff := time.Now().Add(-b.effectiveOrphanShardAge())
	for _, dataDir := range dataDirs {
		phys := filepath.Join(dataDir, filepath.FromSlash(rel))
		entries, rerr := os.ReadDir(phys)
		if rerr != nil {
			continue
		}
		_, hasTmp, newest := classifyShardDir(entries)
		if hasTmp || newest.After(cutoff) {
			return true
		}
	}
	return false
}

// hasLiveCoalescedRef reports whether a coalesced EC shard (bucket/key, blob id
// coalescedID) is still referenced by the live object manifest. Coalesced refs live
// ONLY in the latest-only quorum-meta blob (appendable/coalesced objects are
// versioning-disabled — append is 501-gated on Enabled — so there is no per-version
// coalesced blob), making readQuorumMetaForReclaim the authority. Fail-closed:
// certain=false (caller KEEPS) on a versioning-read error, on a versioning-ENABLED
// bucket (a coalesced record cannot legitimately exist there; a stray shard there is
// a can't-judge → keep, accepting a bounded residual leak for buckets switched to
// Enabled after coalescing), and on any reclaim-read uncertainty. Membership matches
// the physical ShardKey the EC reader uses, plus CoalescedID; keeps on either match.
func (b *DistributedBackend) hasLiveCoalescedRef(bucket, key, coalescedID string) (live, certain bool) {
	on, err := b.blobAuthReadOn(bucket)
	if err != nil {
		return false, false // versioning-read fault → uncertain → keep
	}
	if on {
		return false, false // versioning-enabled → not reclaimable here → keep
	}
	obj, found, c := b.readQuorumMetaForReclaim(bucket, key)
	if !c {
		return false, false // peer-uncertainty / read error → keep
	}
	if !found || obj == nil {
		return false, true // proven absent → orphan-eligible
	}
	wantShardKey := key + "/coalesced/" + coalescedID
	for _, ref := range obj.Coalesced {
		if ref.ShardKey == wantShardKey || ref.CoalescedID == coalescedID {
			return true, true // still referenced → live
		}
	}
	return false, true // live object does not reference this blob → orphan-eligible
}

// coalescedShardReclaimable reports whether a coalesced-shard leaf rel path is safe
// to reclaim. Single source of truth shared by the walk and the DeleteOrphanDir
// re-validation so they cannot drift. Fail-closed everywhere. T4 ambiguity guard: a
// regular object literally keyed ".../coalesced" has an identical-shape shard path,
// so interpret BOTH ways and reclaim ONLY if it is a definite orphan under both. The
// full-object interpretation only applies when the path is ALSO a valid full-object
// shape — if it is not, there is no live full-object to protect, so proceed to the
// coalesced decision (else a true coalesced orphan would leak).
func (b *DistributedBackend) coalescedShardReclaimable(rel string) bool {
	bucket, keyCoal, coalescedID, ok := parseCoalescedRel(rel)
	if !ok {
		return false // not a coalesced shape → keep (caller should not have routed here)
	}
	if !b.owningGroupHosted(bucket) {
		return false // balancer-floated from a non-hosted group → can't judge → keep
	}
	if bucketF, keyFull, vidFull, okF := parseFullObjectRel(rel); okF && bucketF == bucket {
		if recLive, certain := b.hasLiveShardRecord(bucket, keyFull, vidFull); recLive || !certain {
			return false // a real object lives here, or uncertain → keep
		}
	}
	live, certain := b.hasLiveCoalescedRef(bucket, keyCoal, coalescedID)
	return !live && certain
}

// orphanShardSweepReconfirm re-runs the gate + caught-up + snapshot + liveness
// checks for a single canonical dir at deletion time (TOCTOU close).
func (b *DistributedBackend) orphanShardSweepReconfirm(canonical string) bool {
	if !b.orphanShardSweepAllowed() {
		return false
	}
	frozen, err := b.allFrozenObjectVersionDirs()
	if err != nil {
		return false // fail-closed
	}
	c := filepath.Clean(canonical)
	if frozen[c] {
		return false // now snapshot-pinned
	}
	dataDirs := b.shardSvc.DataDirs()
	if len(dataDirs) == 0 {
		return false
	}
	rel, rerr := filepath.Rel(dataDirs[0], c)
	if rerr != nil {
		return false
	}
	relSlash := filepath.ToSlash(rel)
	// Coalesced shards re-validate through the shared dual-interpretation predicate.
	// Route on a parse-validated coalesced shape (NOT strings.Contains) for the same
	// reason as the walk: a full-object key with a "coalesced" component must reach the
	// full-object reconfirm below, not be intercepted here.
	if _, _, _, okCoal := parseCoalescedRel(relSlash); okCoal {
		return b.coalescedShardReclaimable(relSlash)
	}
	bucket, key, versionID, ok := parseFullObjectRel(relSlash)
	if !ok {
		return false
	}
	if !b.owningGroupHosted(bucket) {
		return false // ownership moved to a non-hosted group since the walk → can't judge → keep
	}
	recLive, certain := b.hasLiveShardRecord(bucket, key, versionID)
	return !recLive && certain
}

// Compile-time assertion: DistributedBackend satisfies OrphanWalkable.
var _ scrubber.OrphanWalkable = (*DistributedBackend)(nil)
