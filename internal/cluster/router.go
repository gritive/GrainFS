package cluster

import (
	"errors"
	"hash/fnv"
	"sync/atomic"
)

// ErrNoGroup is returned by Router when no DataGroup can be found for a bucket.
var ErrNoGroup = errors.New("router: no data group for bucket")

// HashAssign returns the group_id for bucket using FNV-32 hash modulo the
// caller-supplied list of group IDs. Returns "" if groups is empty.
//
// Deterministic across processes given the same inputs. Used by CreateBucket
// to record an initial bucket→group mapping in meta-Raft. Caller must pass a
// sorted, deduplicated group list — the modulo result is position-dependent.
func HashAssign(bucket string, groups []string) string {
	if len(groups) == 0 {
		return ""
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(bucket))
	idx := h.Sum32() % uint32(len(groups))
	return groups[idx]
}

// routerSnap is the immutable routing table for Router. COW replacement enables lock-free reads.
// bucketMap is frozen once published via atomic.Pointer.Store — never mutate in-place; always copy-on-write.
type routerSnap struct {
	bucketMap      map[string]string // bucket → group_id (explicit assignments)
	defaultGroupID string            // fallback group_id for unassigned buckets
}

// Router provides bucket-level routing (design doc Layer 1).
// Layer 2 (object→EC shard within group) is handled by ringFNV32 in ring.go.
// key-range sharding is explicitly excluded per design doc.
type Router struct {
	snap atomic.Pointer[routerSnap]
	mgr  *DataGroupManager
}

func NewRouter(mgr *DataGroupManager) *Router {
	r := &Router{mgr: mgr}
	r.snap.Store(&routerSnap{bucketMap: make(map[string]string)})
	return r
}

// SetDefault sets the fallback group_id for buckets without an explicit assignment.
func (r *Router) SetDefault(groupID string) {
	for {
		old := r.snap.Load()
		newSnap := &routerSnap{bucketMap: old.bucketMap, defaultGroupID: groupID}
		if r.snap.CompareAndSwap(old, newSnap) {
			return
		}
	}
}

// AssignBucket explicitly maps a bucket to a group_id.
func (r *Router) AssignBucket(bucket, groupID string) {
	for {
		old := r.snap.Load()
		newMap := make(map[string]string, len(old.bucketMap)+1)
		for k, v := range old.bucketMap {
			newMap[k] = v
		}
		newMap[bucket] = groupID
		newSnap := &routerSnap{bucketMap: newMap, defaultGroupID: old.defaultGroupID}
		if r.snap.CompareAndSwap(old, newSnap) {
			return
		}
	}
}

// Sync merges assignments from a MetaFSM snapshot into the routing table (bootstrap-only).
// Must be called once after MetaRaft start/restore. Existing entries (e.g., those added
// by the OnBucketAssigned callback during concurrent log replay) are preserved; snapshot
// entries take precedence over any conflicting prior value.
// Runtime additions must use AssignBucket to avoid overwriting concurrent updates.
func (r *Router) Sync(assignments map[string]string) {
	for {
		old := r.snap.Load()
		newMap := make(map[string]string, len(old.bucketMap)+len(assignments))
		for k, v := range old.bucketMap {
			newMap[k] = v
		}
		for k, v := range assignments {
			newMap[k] = v
		}
		newSnap := &routerSnap{bucketMap: newMap, defaultGroupID: old.defaultGroupID}
		if r.snap.CompareAndSwap(old, newSnap) {
			return
		}
	}
}

// ExplicitGroup returns the group_id explicitly assigned to bucket, or ("", false)
// if the bucket has no explicit assignment (i.e., would fall through to the default).
// Used by CreateBucket to distinguish "already assigned" from "needs hash-assign".
func (r *Router) ExplicitGroup(bucket string) (string, bool) {
	snap := r.snap.Load()
	gid, ok := snap.bucketMap[bucket]
	return gid, ok
}

// RouteKey returns the DataGroup for the given bucket.
// key is accepted but unused at Layer 1; reserved for future Layer 2 (ringFNV32) integration.
func (r *Router) RouteKey(bucket, _ string) (*DataGroup, error) {
	snap := r.snap.Load()
	gid, ok := snap.bucketMap[bucket]
	if !ok {
		gid = snap.defaultGroupID
	}
	if gid == "" {
		return nil, ErrNoGroup
	}
	g := r.mgr.Get(gid)
	if g == nil {
		return nil, ErrNoGroup
	}
	return g, nil
}
