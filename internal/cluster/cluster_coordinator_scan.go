package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// ListObjectVersions enumerates every version of every object in the bucket.
// Objects are key-hash-placed across shard groups (RouteObjectWrite →
// groupIDForObject), so a single-group read (RouteBucket) would only see the
// versions that happen to live in the bucket's assigned group. This fans out
// across ALL shard groups, unions each group's local FSM enumeration, and
// reconciles a single authoritative IsLatest per key.
func (c *ClusterCoordinator) ListObjectVersions(
	ctx context.Context, bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	state := c.runtimeState()
	groups := c.shardGroupsForVersionedList()
	// With ≤1 group the fan-out is identical to the single-group read, so
	// keep the simpler path (also covers minimally-wired tests with no meta source).
	if len(groups) <= 1 {
		return c.listObjectVersionsSingleGroup(ctx, state, bucket, prefix, maxKeys)
	}
	// Validate the bucket once (the single-group path returned ErrNoSuchBucket
	// via RouteBucket; preserve that — group leaves bypass the bucket check).
	if _, err := state.opRouter.RouteBucket(bucket); err != nil {
		return nil, err
	}
	// S4c-c T2: under blob-authoritative every leaf computes the SAME cluster-wide blob
	// view (→ N× duplicated across groups) plus its own local carve-outs. Dedup
	// by (Key,VID) keep-the-first (duplicates are byte-identical; ObjectVersion
	// has no MetaSeq so keep-one is correct) and apply maxKeys ONCE — NOT
	// reconcileVersionIsLatest (the blob view has no cross-group lat: split).
	// Fail closed on the blob-authority read.
	if on, serr := c.bucketBlobAuthOn(bucket); serr != nil {
		return nil, serr
	} else if on {
		// maxKeys=0 (untruncated) so per-leaf truncation can't underfill the
		// deduped result; the real maxKeys is applied once below.
		merged, err := c.fanOutListObjectVersions(ctx, state, groups, bucket, prefix, 0)
		if err != nil {
			return nil, err
		}
		merged = dedupVersionsKeepFirst(merged)
		sortObjectVersions(merged)
		if maxKeys > 0 && len(merged) > maxKeys {
			merged = merged[:maxKeys]
		}
		return merged, nil
	}
	// maxKeys=0 (untruncated) so per-leaf truncation can't underfill the merged
	// result before the cluster-wide dedup/reconcile below.
	merged, err := c.fanOutListObjectVersions(ctx, state, groups, bucket, prefix, 0)
	if err != nil {
		return nil, err
	}
	// Non-versioned/Suspended: each leaf returns the SAME cluster-wide latest-only
	// blob view (→ N× duplicated across groups), plus its own LOCAL FSM carve-outs
	// (appendable/coalesced — distinct per owning group, never duplicated). Dedup by
	// (Key,VID) keep-the-first collapses the blob N× without disturbing the carve-outs
	// (distinct tuples). reconcileVersionIsLatest then fixes IsLatest across the
	// deduped set (a blob object is its own single latest).
	merged = dedupVersionsKeepFirst(merged)
	reconcileVersionIsLatest(merged)
	sortObjectVersions(merged)
	if maxKeys > 0 && len(merged) > maxKeys {
		merged = merged[:maxKeys]
	}
	return merged, nil
}

// bucketBlobAuthOn reports whether the bucket's blob-authority state is "on",
// FAIL-CLOSED on a read error (mirrors DistributedBackend.blobAuthReadOn).
func (c *ClusterCoordinator) bucketBlobAuthOn(bucket string) (bool, error) {
	// S2 blob-primary: blob-authoritative for every versioning-enabled bucket
	// (mirrors DistributedBackend.blobAuthReadOn).
	state, err := c.GetBucketVersioning(bucket)
	if err != nil {
		return false, fmt.Errorf("read versioning state for bucket %q: %w", bucket, err)
	}
	return state == "Enabled", nil
}

// dedupVersionsKeepFirst removes (Key,VersionID) duplicates, keeping the first
// occurrence. Under blob-authoritative every leaf returns the identical cluster-wide
// blob view, so the blob entries are byte-identical duplicates across groups;
// ObjectVersion carries no MetaSeq tiebreak, so keep-one is correct. Carve-out
// keys are owned by exactly one group and never duplicate.
func dedupVersionsKeepFirst(versions []*storage.ObjectVersion) []*storage.ObjectVersion {
	seen := make(map[[2]string]bool, len(versions))
	out := versions[:0]
	for _, v := range versions {
		k := [2]string{v.Key, v.VersionID}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, v)
	}
	return out
}

// shardGroupsForVersionedList returns the cluster-wide shard group list (NOT
// c.groups.All(), which is local-only). Empty when no meta source is wired.
func (c *ClusterCoordinator) shardGroupsForVersionedList() []ShardGroupEntry {
	if c.meta == nil {
		return nil
	}
	return c.meta.ShardGroups()
}

func (c *ClusterCoordinator) listObjectVersionsSingleGroup(
	ctx context.Context, state clusterCoordinatorRuntime, bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	target, err := state.opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := state.localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListObjectVersions(ctx, bucket, prefix, maxKeys)
	}
	return c.forwardRuntime().listObjectVersions(ctx, target, bucket, prefix, maxKeys)
}

// fanOutListObjectVersions queries every shard group's per-version FSM
// concurrently (local backend or forwarded) and unions the results. Fail-closed:
// a single group error fails the whole LIST rather than silently returning a
// partial version set, which would look like data loss.
func (c *ClusterCoordinator) fanOutListObjectVersions(
	ctx context.Context, state clusterCoordinatorRuntime, groups []ShardGroupEntry, bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	type groupResult struct {
		versions []*storage.ObjectVersion
		err      error
	}
	ch := make(chan groupResult, len(groups))
	for _, g := range groups {
		g := g
		go func() {
			target, err := state.opRouter.routeGroup(g.ID)
			if err != nil {
				ch <- groupResult{err: fmt.Errorf("ListObjectVersions route group %s: %w", g.ID, err)}
				return
			}
			if gb, err := state.localExec.ResolveRead(ctx, target); err != nil {
				ch <- groupResult{err: err}
			} else if gb != nil {
				vs, lerr := gb.ListObjectVersions(ctx, bucket, prefix, maxKeys)
				ch <- groupResult{versions: vs, err: lerr}
			} else {
				vs, ferr := c.forwardRuntime().listObjectVersions(ctx, target, bucket, prefix, maxKeys)
				ch <- groupResult{versions: vs, err: ferr}
			}
		}()
	}
	var (
		merged   []*storage.ObjectVersion
		firstErr error
	)
	for range groups {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		merged = append(merged, r.versions...)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return merged, nil
}

// reconcileVersionIsLatest enforces exactly one IsLatest per key after a
// cross-group merge. Candidates are ONLY versions a group already flagged
// IsLatest (its lat: pointer) — a non-flagged version (e.g. a PreserveLatest
// write) is never promoted. When a key split across groups left >1 flagged
// version (divergent lat:), the newest (max UUIDv7 VersionID) wins and the
// others are demoted. No-op in the common single-group-per-key case.
func reconcileVersionIsLatest(versions []*storage.ObjectVersion) {
	latestByKey := make(map[string]*storage.ObjectVersion)
	for _, v := range versions {
		if !v.IsLatest {
			continue
		}
		cur, ok := latestByKey[v.Key]
		if !ok {
			latestByKey[v.Key] = v
			continue
		}
		if latestWins(v.LastModified, v.VersionID, cur.LastModified, cur.VersionID) {
			cur.IsLatest = false
			latestByKey[v.Key] = v
		} else {
			v.IsLatest = false
		}
	}
}

// sortObjectVersions orders by (Key asc, ModTime-primary desc) — newest version
// first within each key (latestWins: higher ModTime, tie → higher VersionID), so
// the IsLatest version sorts first. Mirrors the per-group leaf sort.
func sortObjectVersions(versions []*storage.ObjectVersion) {
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key {
			return versions[i].Key < versions[j].Key
		}
		return latestWins(versions[i].LastModified, versions[i].VersionID,
			versions[j].LastModified, versions[j].VersionID)
	})
}

// ScanObjectsGrouped fans out to all locally-owned shard groups so that
// objects written to any shard group (not just group-0 / base) are visible
// to the lifecycle expiration scan. In single-node mode every seeded group is
// locally owned; in cluster mode the leader scans its own shard groups.
// Falls back to c.base when no data groups are registered (tests / legacy).
func (c *ClusterCoordinator) ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error) {
	type scanner interface {
		ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error)
	}

	// No DataGroupManager — fall back to base (covers tests and legacy single-group mode).
	if c.groups == nil {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanObjectsGrouped", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanObjectsGrouped(bucket)
	}

	groups := c.groups.All()
	if len(groups) == 0 {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanObjectsGrouped", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanObjectsGrouped(bucket)
	}

	// Collect per-group channels; only locally-owned groups (Backend() != nil) participate.
	var srcs []<-chan storage.ObjectKeyGroup
	for _, dg := range groups {
		gb := dg.Backend()
		if gb == nil {
			continue // not locally owned — skip (no RPC fan-out for lifecycle)
		}
		ch, err := gb.ScanObjectsGrouped(bucket)
		if err != nil {
			return nil, fmt.Errorf("ScanObjectsGrouped group %s: %w", dg.ID(), err)
		}
		srcs = append(srcs, ch)
	}
	if len(srcs) == 0 {
		// No local backends (all placeholder groups) — nothing to scan.
		out := make(chan storage.ObjectKeyGroup)
		close(out)
		return out, nil
	}

	out := make(chan storage.ObjectKeyGroup, 16)
	go func() {
		defer close(out)
		for _, src := range srcs {
			for g := range src {
				out <- g
			}
		}
	}()
	return out, nil
}

// ScanLocalMultipartUploads fans out to all locally-owned shard groups so that
// MPU metadata stored in any shard group's keyspace is visible to the lifecycle
// worker. MPU metadata is stored in the shard group that owns the write path for
// the bucket (not necessarily group-0 / base). Falls back to c.base when no data
// groups are registered (tests / legacy single-group mode).
func (c *ClusterCoordinator) ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error) {
	type scanner interface {
		ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error)
	}

	// No DataGroupManager — fall back to base (covers tests and legacy single-group mode).
	if c.groups == nil {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanLocalMultipartUploads", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanLocalMultipartUploads(bucket)
	}

	groups := c.groups.All()
	if len(groups) == 0 {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanLocalMultipartUploads", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanLocalMultipartUploads(bucket)
	}

	// Collect per-group channels; only locally-owned groups (Backend() != nil) participate.
	// Keep the source group ID alongside each channel: lifecycle abort feeds these
	// records back into AbortMultipartUpload, which routes by the group encoded in
	// the upload ID. Without re-encoding here, expired MPUs created under group-ID
	// routing would fall back to legacy hash routing and leak on divergent nodes.
	type groupScan struct {
		groupID string
		ch      <-chan storage.MultipartUploadRecord
	}
	var srcs []groupScan
	for _, dg := range groups {
		gb := dg.Backend()
		if gb == nil {
			continue // not locally owned — skip (no RPC fan-out for lifecycle)
		}
		ch, err := gb.ScanLocalMultipartUploads(bucket)
		if err != nil {
			return nil, fmt.Errorf("ScanLocalMultipartUploads group %s: %w", dg.ID(), err)
		}
		srcs = append(srcs, groupScan{groupID: dg.ID(), ch: ch})
	}
	if len(srcs) == 0 {
		out := make(chan storage.MultipartUploadRecord)
		close(out)
		return out, nil
	}

	wrap := c.multipartGroupIDRouting()
	out := make(chan storage.MultipartUploadRecord, 16)
	go func() {
		defer close(out)
		for _, src := range srcs {
			for rec := range src.ch {
				if wrap {
					rec.UploadID = encodeMultipartUploadID(src.groupID, rec.UploadID)
				}
				out <- rec
			}
		}
	}()
	return out, nil
}

// ScrubPeerStat is the cluster-package-local snapshot of one peer's scrub
// session state. Returned by ScrubSessionStat fan-out; serve.go's adapter
// converts to admin.ScrubJobInfo so cluster does not import admin.
type ScrubPeerStat struct {
	Bucket       string
	KeyPrefix    string
	DryRun       bool
	Status       string
	StartedAt    int64
	DoneAt       int64
	Checked      int64
	Healthy      int64
	Detected     int64
	Repaired     int64
	Unrepairable int64
	Skipped      int64
	OwnedHere    bool
}

// ScrubSessionStat fans out a ScrubSessionStat RPC to every peer in the
// cluster (excluding self), aggregating per-peer scrub stats for cluster-wide
// admin GET /v1/scrub/jobs/<id>. Each peer call has a 5s timeout; failures
// are returned as the second slice so admin can flag partial=true.
func (c *ClusterCoordinator) ScrubSessionStat(ctx context.Context, sessionID string) ([]ScrubPeerStat, []string, error) {
	if c.forward == nil || c.addr == nil {
		return nil, nil, nil
	}
	nodes := c.addr.Nodes()
	if len(nodes) <= 1 {
		return nil, nil, nil
	}
	args := buildScrubSessionStatArgs(sessionID)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		infos    []ScrubPeerStat
		failures []string
	)
	for _, n := range nodes {
		if c.matchSelfPeer(n.ID) {
			continue
		}
		wg.Add(1)
		go func(node MetaNodeEntry) {
			defer wg.Done()
			peerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			reply, err := c.forward.Send(peerCtx, []string{node.Address}, "", raftpb.ForwardOpScrubSessionStat, args)
			if err != nil {
				mu.Lock()
				failures = append(failures, node.ID)
				mu.Unlock()
				return
			}
			info, ok := decodeScrubSessionStatReply(reply)
			if !ok {
				mu.Lock()
				failures = append(failures, node.ID)
				mu.Unlock()
				return
			}
			if !info.found {
				return
			}
			mu.Lock()
			infos = append(infos, info.toPeerStat())
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return infos, failures, nil
}

func buildScrubSessionStatArgs(sessionID string) []byte {
	b := flatbuffers.NewBuilder(64)
	sidOff := b.CreateString(sessionID)
	raftpb.ScrubSessionStatArgsStart(b)
	raftpb.ScrubSessionStatArgsAddSessionId(b, sidOff)
	b.Finish(raftpb.ScrubSessionStatArgsEnd(b))
	return b.FinishedBytes()
}

type scrubSessionStatDecoded struct {
	found        bool
	bucket       string
	keyPrefix    string
	dryRun       bool
	status       string
	startedAt    int64
	doneAt       int64
	checked      int64
	healthy      int64
	detected     int64
	repaired     int64
	unrepairable int64
	skipped      int64
	ownedHere    bool
}

func (d scrubSessionStatDecoded) toPeerStat() ScrubPeerStat {
	return ScrubPeerStat{
		Bucket:       d.bucket,
		KeyPrefix:    d.keyPrefix,
		DryRun:       d.dryRun,
		Status:       d.status,
		StartedAt:    d.startedAt,
		DoneAt:       d.doneAt,
		Checked:      d.checked,
		Healthy:      d.healthy,
		Detected:     d.detected,
		Repaired:     d.repaired,
		Unrepairable: d.unrepairable,
		Skipped:      d.skipped,
		OwnedHere:    d.ownedHere,
	}
}

func decodeScrubSessionStatReply(reply []byte) (scrubSessionStatDecoded, bool) {
	if len(reply) == 0 {
		return scrubSessionStatDecoded{}, false
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	if fr == nil || fr.Status() != raftpb.ForwardStatusOK {
		return scrubSessionStatDecoded{}, false
	}
	ss := fr.ScrubSession(nil)
	if ss == nil {
		return scrubSessionStatDecoded{}, false
	}
	return scrubSessionStatDecoded{
		found:        ss.Found(),
		bucket:       string(ss.Bucket()),
		keyPrefix:    string(ss.KeyPrefix()),
		dryRun:       ss.DryRun(),
		status:       string(ss.Status()),
		startedAt:    ss.StartedAt(),
		doneAt:       ss.DoneAt(),
		checked:      ss.Checked(),
		healthy:      ss.Healthy(),
		detected:     ss.Detected(),
		repaired:     ss.Repaired(),
		unrepairable: ss.Unrepairable(),
		skipped:      ss.Skipped(),
		ownedHere:    ss.OwnedHere(),
	}, true
}
