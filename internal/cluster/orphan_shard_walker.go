package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

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

// liveVersionedShardDirs forward-maps every live versioned object (FSM obj:
// record) to its canonical (dataDirs[0]-rooted) shard dir. This is the ONLY
// data-loss-safe way to protect versioned objects: shard writes use
// filepath.Join, which CLEANS the key, so a logical key like "a/../b" lands in
// the physical dir ".../bkt/b/<ver>". Reverse-parsing that cleaned path gives
// the wrong logical key ("b"), so a per-candidate FSM lookup would MISS the real
// obj: record "obj:bkt/a/../b/<ver>" and the walker would delete a live retained
// version. Forward-mapping (logical key -> getShardDir, the same Join the writer
// used) is bijective with the on-disk layout, so cleanable keys are protected.
//
// Fail-closed: a scan/decode error returns an error so the caller skips the
// whole sweep (never sweeps against a partial live-set). Tombstones are skipped
// (no shards). quorum-meta (regular-PUT) is NOT enumerated here — it is K-of-N
// (a parity node lacks the local record) so it needs the peer-fallback
// point-lookup in hasLiveShardRecord, and its storage path is itself cleaned so
// the reverse-parsed key matches.
func (b *DistributedBackend) liveVersionedShardDirs() (map[string]bool, error) {
	if b.shardSvc == nil {
		return nil, fmt.Errorf("no shard service")
	}
	// Phase 1 (inside the read txn): collect candidate FSM obj: records. The
	// per-bucket versioning lookup (blobAuthReadOn) opens its OWN read txn, so it
	// must run OUTSIDE this View — collect here, classify below.
	type fsmRec struct {
		bucket, key, versionID string
		meta                   objectMeta
	}
	var recs []fsmRec
	verr := b.store.View(func(txn MetadataTxn) error {
		return b.ks().scanGroupPrefix(txn, []byte("obj:"), func(rawKey []byte, item MetaItem) error {
			s := string(rawKey[len("obj:"):]) // <bucket>/<key>/<versionID>
			slash := strings.IndexByte(s, '/')
			if slash < 0 {
				return nil
			}
			bucket := s[:slash]
			rest := s[slash+1:]
			last := strings.LastIndexByte(rest, '/')
			if last < 0 {
				return nil
			}
			versionID := rest[last+1:]
			if bucket == "" || versionID == "" {
				return nil
			}
			raw, cerr := b.itemValueCopy(item)
			if cerr != nil {
				return cerr // fail-closed
			}
			meta, merr := unmarshalObjectMeta(raw)
			if merr != nil {
				return merr // fail-closed
			}
			if meta.ETag == deleteMarkerETag {
				return nil // tombstone — no shards
			}
			recs = append(recs, fsmRec{bucket: bucket, key: meta.Key, versionID: versionID, meta: meta})
			return nil
		})
	})
	if verr != nil {
		return nil, verr
	}
	// Phase 2 (outside the txn): forward-map each record EXCEPT a plain (non-carve-out)
	// versioned record under a versioning-enabled bucket — those are blob-authoritative
	// under blob-primary (protected via hasLiveShardRecord's per-version blob lookup),
	// and a lingering one (e.g. left by a hard delete) must NOT keep its now-tombstoned
	// shards alive. Carve-outs (appendable/coalesced) and non-versioned records stay
	// FSM-authoritative and need the bijective forward-map for cleanable keys.
	out := make(map[string]bool)
	verCache := make(map[string]bool)
	for _, r := range recs {
		ver, ok := verCache[r.bucket]
		if !ok {
			v, err := b.blobAuthReadOn(r.bucket)
			if err != nil {
				return nil, err // fail-closed
			}
			ver = v
			verCache[r.bucket] = v
		}
		if ver && !isFsmCarveoutClass(r.meta, false) {
			continue // plain versioned → blob-authoritative, not forward-mapped here
		}
		dir, derr := b.shardSvc.getShardDir(r.bucket, r.key+"/"+r.versionID, 0)
		if derr != nil {
			continue // escaping key cannot match a real on-disk dir
		}
		out[filepath.Clean(dir)] = true
	}
	return out, nil
}

// liveVersionedShardDirsAllHosted unions liveVersionedShardDirs across every
// locally-hosted group backend. The shared ShardService dataDirs commingle all
// hosted groups' shards, so a versioned object owned by ANY hosted group (its
// obj: record lives under that group's ks prefix on the shared store) must be in
// the known-set or the sweep would false-orphan it. Fail-closed: a nil backend or
// any scan error returns an error so the caller skips the whole sweep.
func (b *DistributedBackend) liveVersionedShardDirsAllHosted() (map[string]bool, error) {
	out := make(map[string]bool)
	for _, gb := range b.hostedGroupBackends() {
		if gb == nil {
			return nil, fmt.Errorf("hosted group backend is nil")
		}
		m, err := gb.liveVersionedShardDirs()
		if err != nil {
			return nil, err
		}
		for k := range m {
			out[k] = true
		}
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
	live, err := b.liveVersionedShardDirsAllHosted()
	if err != nil {
		return nil // fail-closed: never sweep without the full live versioned-set
	}

	dataDirs := b.shardSvc.DataDirs()
	if len(dataDirs) == 0 {
		return nil
	}
	cutoff := time.Now().Add(-b.effectiveOrphanShardAge())
	seen := make(map[string]bool)

	for _, dataDir := range dataDirs {
		stopErr := b.walkOneShardRoot(dataDir, dataDirs[0], known, frozen, live, seen, cutoff, fn)
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
	known, frozen, live, seen map[string]bool,
	cutoff time.Time,
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
		if known[canonical] || frozen[canonical] || live[canonical] || seen[canonical] {
			return filepath.SkipDir // latest-known, snapshot-pinned, or a live versioned object
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
// metadata record, and whether that determination is CERTAIN. Fail-closed: any
// read uncertainty returns (false, false) so the caller keeps the shards.
//
// Order: FSM obj: (covers versioned objects incl. PreserveLatest), then
// peer-fallback quorum-meta (covers regular-PUT; K-of-N means a parity node may
// hold shards without a LOCAL quorum-meta record, so the peer fan-out in
// readQuorumMeta is mandatory — a local-only read would false-orphan it).
func (b *DistributedBackend) hasLiveShardRecord(bucket, key, versionID string) (live, certain bool) {
	// Blob-primary: for a versioning-enabled bucket the per-version blob is the
	// shard-liveness authority for plain versioned objects (live iff a blob exists
	// that is neither a hard-delete tombstone nor a delete marker); carve-outs
	// (appendable/coalesced) stay FSM-authoritative. A stale plain-versioned FSM
	// record is NON-authoritative here, so a hard-deleted version's shards become
	// orphan-eligible even while its FSM record lingers.
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
		return b.fsmCarveoutShardLive(bucket, key, versionID)
	}
	var fsmLive, fsmFound bool
	verr := b.store.View(func(txn MetadataTxn) error {
		item, gerr := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if gerr != nil {
			if errors.Is(gerr, ErrMetaKeyNotFound) {
				return nil // not in FSM obj: → fall through to quorum-meta
			}
			return gerr // genuine read error
		}
		raw, cerr := b.itemValueCopy(item)
		if cerr != nil {
			return cerr
		}
		meta, merr := unmarshalObjectMeta(raw)
		if merr != nil {
			return merr
		}
		fsmFound = true
		fsmLive = meta.ETag != deleteMarkerETag
		return nil
	})
	if verr != nil {
		return false, false // UNCERTAIN → keep
	}
	if fsmFound {
		if fsmLive {
			return true, true
		}
		// FSM tombstone: no shards expected; fall through to quorum-meta to be sure.
	}

	// Certainty-aware reclaim read: plain readQuorumMeta maps an exhausted peer
	// fan-out to ErrObjectNotFound even when a metadata-holding peer was merely
	// unreachable, so reclaiming on its NotFound could delete a live object whose
	// quorum-meta sits on a briefly-down peer (K-of-N). readQuorumMetaForReclaim
	// fails CLOSED on any peer-uncertainty; a negative (absent / different-version)
	// claim is honored only when the responding set is complete.
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
	return false, true // proven absent in FSM and quorum-meta → orphan-eligible
}

// fsmCarveoutShardLive judges shard liveness from the FSM obj: record for a
// versioning-enabled bucket when no per-version blob exists: only a carve-out
// (appendable/coalesced) record is authoritative and keeps its shards alive. A
// plain versioned FSM record is non-authoritative under blob-primary (the blob is
// the authority and already reported no live version), so it is orphan-eligible.
// Fail-closed: a read error returns (false, false) so the caller keeps the shards.
func (b *DistributedBackend) fsmCarveoutShardLive(bucket, key, versionID string) (live, certain bool) {
	var found, carveLive bool
	verr := b.store.View(func(txn MetadataTxn) error {
		item, gerr := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if gerr != nil {
			if errors.Is(gerr, ErrMetaKeyNotFound) {
				return nil
			}
			return gerr
		}
		raw, cerr := b.itemValueCopy(item)
		if cerr != nil {
			return cerr
		}
		meta, merr := unmarshalObjectMeta(raw)
		if merr != nil {
			return merr
		}
		// versionID != "" → bareLegacy=false; carve-out = appendable || coalesced.
		if isFsmCarveoutClass(meta, false) && meta.ETag != deleteMarkerETag {
			found, carveLive = true, true
		}
		return nil
	})
	if verr != nil {
		return false, false // UNCERTAIN → keep
	}
	if found {
		return carveLive, true
	}
	return false, true // no carve-out record → orphan-eligible (blob already said not-live)
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
	live, err := b.liveVersionedShardDirsAllHosted()
	if err != nil {
		return false // fail-closed
	}
	c := filepath.Clean(canonical)
	if frozen[c] || live[c] {
		return false // now snapshot-pinned or a live versioned object (cleanable-key safe)
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
