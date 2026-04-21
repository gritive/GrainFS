package scrubber_test

// Slice 3 of refactor/unify-storage-paths: verify the scrubber honours the
// optional ShardOwner / ShardRepairer contracts so each cluster node only
// verifies its own shards and repairs them via peer-sourced reconstruction.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// ownerBackend is a mockBackend variant that also reports ownership and
// services repair requests. Exercises the scrubber's ShardOwner filter
// and ShardRepairer delegation paths.
type ownerBackend struct {
	*mockBackend
	nodeID       string
	ownedByKey   map[string][]int // "bucket/key" → owned shard indices
	repaired     []repairCall
	repairedLock sync.Mutex
	repairErr    error
}

type repairCall struct {
	bucket, key, versionID string
	shardIdx               int
}

func newOwnerBackend(nodeID string) *ownerBackend {
	return &ownerBackend{
		mockBackend: newMockBackend(),
		nodeID:      nodeID,
		ownedByKey:  make(map[string][]int),
	}
}

func (o *ownerBackend) NodeID() string { return o.nodeID }

func (o *ownerBackend) OwnedShards(bucket, key, versionID, nodeID string) []int {
	if nodeID != o.nodeID {
		return nil
	}
	return o.ownedByKey[bucket+"/"+key]
}

func (o *ownerBackend) RepairShardLocal(bucket, key, versionID string, shardIdx int) error {
	o.repairedLock.Lock()
	o.repaired = append(o.repaired, repairCall{bucket, key, versionID, shardIdx})
	o.repairedLock.Unlock()
	if o.repairErr != nil {
		return o.repairErr
	}
	// Simulate peer-sourced reconstruct: restore the shard.
	o.mu.Lock()
	o.shards[fmt.Sprintf("%s/%s/%d", bucket, key, shardIdx)] = []byte("reconstructed")
	o.mu.Unlock()
	return nil
}

func (o *ownerBackend) repairedCalls() []repairCall {
	o.repairedLock.Lock()
	defer o.repairedLock.Unlock()
	return append([]repairCall(nil), o.repaired...)
}

// TestScrubber_ShardOwner_SkipsUnownedObjects ensures that an object whose
// placement excludes this node is skipped entirely — it is neither verified
// nor counted.
func TestScrubber_ShardOwner_SkipsUnownedObjects(t *testing.T) {
	o := newOwnerBackend("node-A")
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "peer-owned", DataShards: 2, ParityShards: 1, VersionID: "v1"}
	o.records["b"] = []scrubber.ObjectRecord{rec}
	// Intentionally no ownedByKey entry for peer-owned: node-A holds no shards.

	s := scrubber.New(o, time.Hour)
	s.RunOnce(context.Background())

	stats := s.Stats()
	assert.EqualValues(t, 0, stats.ObjectsChecked, "peer-owned objects must not be verified")
	assert.EqualValues(t, 0, stats.ShardErrors)
	assert.Empty(t, o.repairedCalls(), "no RepairShardLocal calls on peer-owned objects")
}

// TestScrubber_ShardOwner_VerifiesOnlyOwnedShards confirms a missing
// peer-owned shard does NOT trigger ShardErrors on this node — even when
// local shards are healthy. In cluster mode, the peer's own monitor is
// responsible for peer shards.
func TestScrubber_ShardOwner_VerifiesOnlyOwnedShards(t *testing.T) {
	o := newOwnerBackend("node-A")
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "mixed", DataShards: 2, ParityShards: 1, VersionID: "v1"}
	o.records["b"] = []scrubber.ObjectRecord{rec}
	o.ownedByKey["b/mixed"] = []int{0} // node-A owns shard 0 only

	// Store shard 0 (ours), leave shard 1 absent (peer's) and shard 2 absent (peer's).
	o.storeShards("b", "mixed", [][]byte{[]byte("s0"), nil, nil})

	s := scrubber.New(o, time.Hour)
	s.RunOnce(context.Background())

	stats := s.Stats()
	assert.EqualValues(t, 1, stats.ObjectsChecked, "owned object must be verified")
	assert.EqualValues(t, 0, stats.ShardErrors, "missing peer shards must not count as errors here")
	assert.Empty(t, o.repairedCalls(), "no repair when owned shard is healthy")
}

// TestScrubber_ShardOwner_RepairsOwnedMissingShard wires ShardOwner +
// ShardRepairer together: a missing owned shard is detected, RepairShardLocal
// is called exactly once for that index, and stats increment.
func TestScrubber_ShardOwner_RepairsOwnedMissingShard(t *testing.T) {
	o := newOwnerBackend("node-A")
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "mine", DataShards: 2, ParityShards: 1, VersionID: "v1"}
	o.records["b"] = []scrubber.ObjectRecord{rec}
	o.ownedByKey["b/mine"] = []int{0, 2}

	// Store peer shard (1) plus our shard 2; shard 0 (ours) is missing.
	o.storeShards("b", "mine", [][]byte{nil, []byte("peer"), []byte("mine-2")})

	s := scrubber.New(o, time.Hour)
	s.RunOnce(context.Background())

	stats := s.Stats()
	assert.EqualValues(t, 1, stats.ObjectsChecked)
	assert.EqualValues(t, 1, stats.ShardErrors, "one owned shard missing")
	assert.EqualValues(t, 1, stats.Repaired, "one object repaired")

	calls := o.repairedCalls()
	if assert.Len(t, calls, 1, "exactly one RepairShardLocal call") {
		assert.Equal(t, "b", calls[0].bucket)
		assert.Equal(t, "mine", calls[0].key)
		assert.Equal(t, "v1", calls[0].versionID)
		assert.Equal(t, 0, calls[0].shardIdx)
	}
}
