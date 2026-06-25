package cluster

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestQuorumMetaWriteRPC_RemoteCASRejectSurfacesSentinel is the BUG-1 regression:
// a REMOTE replica's CAS reject must round-trip through the shard RPC error
// channel and be reconstituted as errQuorumMetaCASReject at the client, so
// fanOutQuorumMeta counts it like a local reject. It drives the EXACT handler →
// client path: handleRPC produces the on-wire "Error" reply, and the client's
// unmarshalEnvelope + quorumMetaWriteRPCError mapping consumes it.
//
// Before the fix the client discarded the response body and returned a generic
// "remote quorum meta error", so a remote CAS reject was never counted as the
// distinguishable sentinel and the multi-node RMW retry/SlowDown logic was
// bypassed.
func TestQuorumMetaWriteRPC_RemoteCASRejectSurfacesSentinel(t *testing.T) {
	b := newTestDistributedBackend(t)

	// Seat a base blob at MetaSeq 3 on the (remote-acting) node's local store.
	base := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 3})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", base))

	// A stale CAS candidate: read base=2 → offers MetaSeq 3, but existing is already
	// 3, so it needs 4 → the local guard rejects with errQuorumMetaCASReject.
	stale := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 3, MetaSeqCAS: true})

	// Handler side: produce the on-wire reply exactly as a remote node would.
	reqEnv := buildShardEnvelope("WriteQuorumMeta", "bkt", "k", 0, stale)
	respBytes := b.shardSvc.handleRPC(reqEnv.FinishedBytes())
	reqEnv.Reset()
	shardBuilderPool.Put(reqEnv)

	// The handler MUST emit the distinguishable CAS-reject wire code, not free text.
	rpcType, body, err := unmarshalEnvelope(respBytes)
	require.NoError(t, err)
	require.Equal(t, "Error", rpcType, "a CAS reject is reported as an Error reply")
	require.Equal(t, quorumMetaCASRejectWireCode, string(body),
		"handler must serialize the CAS reject as the stable wire code")

	// Client side: the mapping reconstitutes errQuorumMetaCASReject (errors.Is-able).
	mapped := quorumMetaWriteRPCError("peer-addr", body)
	require.ErrorIs(t, mapped, errQuorumMetaCASReject,
		"a remote CAS reject must surface as errQuorumMetaCASReject at the client")

	// A non-CAS remote error must NOT be misclassified as a CAS reject.
	generic := quorumMetaWriteRPCError("peer-addr", []byte("disk full"))
	require.NotErrorIs(t, generic, errQuorumMetaCASReject,
		"a generic remote error must not be mistaken for a CAS reject")
}

// TestQuorumMetaVersionWriteRPC_RemoteCASRejectSurfacesSentinel mirrors BUG-1 on
// the per-version write handler → client path.
func TestQuorumMetaVersionWriteRPC_RemoteCASRejectSurfacesSentinel(t *testing.T) {
	b := newTestDistributedBackend(t)
	sub := filepath.Join("k", "vid-1")

	base := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 3})
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, base))

	stale := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 3, MetaSeqCAS: true})
	reqEnv := buildShardEnvelope("WriteQuorumMetaVersion", "bkt", sub, 0, stale)
	respBytes := b.shardSvc.handleRPC(reqEnv.FinishedBytes())
	reqEnv.Reset()
	shardBuilderPool.Put(reqEnv)

	rpcType, body, err := unmarshalEnvelope(respBytes)
	require.NoError(t, err)
	require.Equal(t, "Error", rpcType)
	require.Equal(t, quorumMetaCASRejectWireCode, string(body))
	require.ErrorIs(t, quorumMetaWriteRPCError("peer-addr", body), errQuorumMetaCASReject)
}

func TestQuorumMetaLocalWriteWithResult_ReportsApplySkipAndReplay(t *testing.T) {
	b := newTestDistributedBackend(t)
	first := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true,
	})
	result, err := b.shardSvc.writeQuorumMetaLocalWithResult("bkt", "k", first)
	require.NoError(t, err)
	require.True(t, result.applied, "absent target must publish the candidate")
	require.False(t, result.hadPrevious)

	result, err = b.shardSvc.writeQuorumMetaLocalWithResult("bkt", "k", first)
	require.NoError(t, err)
	require.False(t, result.applied, "byte-identical CAS replay is a no-op, not a fresh publish")

	olderLWW := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 99, MetaSeq: 1,
	})
	result, err = b.shardSvc.writeQuorumMetaLocalWithResult("bkt", "k", olderLWW)
	require.NoError(t, err)
	require.False(t, result.applied, "LWW loss is a no-op, not a fresh publish")
}

func TestQuorumMetaLocalRollbackIfMatch_RemovesMatchingBlobWithNoPrevious(t *testing.T) {
	b := newTestDistributedBackend(t)
	matching := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true,
	})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", matching))

	require.NoError(t, b.shardSvc.rollbackQuorumMetaLocalIfMatch("bkt", "k", matching, nil, false))
	_, err := b.shardSvc.readQuorumMetaRaw("bkt", "k")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestQuorumMetaLocalRollbackIfMatch_KeepsChangedBlob(t *testing.T) {
	b := newTestDistributedBackend(t)
	stale := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true,
	})
	newer := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 101, MetaSeq: 2, MetaSeqCAS: true,
	})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", newer))

	require.NoError(t, b.shardSvc.rollbackQuorumMetaLocalIfMatch("bkt", "k", stale, nil, false))
	raw, err := b.shardSvc.readQuorumMetaRaw("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, newer, raw)
}

func TestQuorumMetaLocalRollbackIfMatch_RestoresPreviousBlob(t *testing.T) {
	b := newTestDistributedBackend(t)
	previous := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true,
	})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", previous))
	next := mustEncodeMetaCmd(t, PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v2", ModTime: 101, MetaSeq: 2, MetaSeqCAS: true,
	})
	result, err := b.shardSvc.writeQuorumMetaLocalWithResult("bkt", "k", next)
	require.NoError(t, err)
	require.True(t, result.applied)
	require.True(t, result.hadPrevious)
	require.Equal(t, previous, result.previous)

	require.NoError(t, b.shardSvc.rollbackQuorumMetaLocalIfMatch("bkt", "k", next, result.previous, result.hadPrevious))
	raw, err := b.shardSvc.readQuorumMetaRaw("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, previous, raw, "failed CAS quorum write must restore the previously committed latest blob")
}

// TestFanOutOwnerLocalFirst_LocalDurableBeforeReturn is the BUG-2 regression at
// the fan-out primitive: even when k peer acks arrive FIRST, the owner-local
// write must already be durable when fanOutQuorumMetaOwnerLocalFirst returns —
// because the next same-owner RMW reads its base owner-local-first. The peer
// writers ack instantly; the local write records completion. With the
// owner-local-first ordering the local write is synchronous and therefore
// completes strictly before the function returns.
func TestFanOutOwnerLocalFirst_LocalDurableBeforeReturn(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, "p1", "p2", "p3"} // owner + 3 peers, K=2

	var localDone atomic.Bool
	writeLocal := func() error {
		localDone.Store(true)
		return nil
	}
	// Peers ack immediately; the plain fan-out would have returned on k peer acks
	// without waiting for the (formerly concurrent) local write.
	writePeer := func(_ context.Context, _ string) error { return nil }
	cleanupLocal := func() error { return nil }

	err := fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, cleanupLocal, writePeer)
	require.NoError(t, err)
	require.True(t, localDone.Load(),
		"owner-local write must be durable before fanOut returns (BUG-2 stale-base fix)")
}

func TestFanOutOwnerLocalFirst_CleansLocalOnPeerQuorumFailure(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, "p1", "p2"}

	var localWrites atomic.Int32
	writeLocal := func() error {
		localWrites.Add(1)
		return nil
	}
	var cleanupCalls atomic.Int32
	cleanupLocal := func() error {
		cleanupCalls.Add(1)
		return nil
	}
	writePeer := func(_ context.Context, _ string) error { return errors.New("peer down") }

	err := fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, cleanupLocal, writePeer)
	require.Error(t, err)
	require.Equal(t, int32(1), localWrites.Load())
	require.Equal(t, int32(1), cleanupCalls.Load(), "failed peer quorum must roll back the owner-local publish")
}

func TestFanOutOwnerLocalFirst_DoesNotCleanLocalOnSuccess(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, "p1", "p2"}

	var cleanupCalls atomic.Int32
	writeLocal := func() error { return nil }
	cleanupLocal := func() error {
		cleanupCalls.Add(1)
		return nil
	}
	writePeer := func(_ context.Context, _ string) error { return nil }

	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, cleanupLocal, writePeer))
	require.Equal(t, int32(0), cleanupCalls.Load(), "successful quorum must keep the owner-local blob")
}

func TestFanOutOwnerLocalFirst_JoinsCleanupFailureAndPreservesOriginal(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, "p1"}
	cleanupErr := errors.New("cleanup failed")

	writeLocal := func() error { return nil }
	cleanupLocal := func() error { return cleanupErr }
	writePeer := func(_ context.Context, _ string) error { return errQuorumMetaCASReject }

	err := fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, cleanupLocal, writePeer)
	require.ErrorIs(t, err, errQuorumMetaCASReject)
	require.ErrorIs(t, err, cleanupErr)
}

// TestFanOutOwnerLocalFirst_LocalCASRejectSurfaces verifies the owner's own base
// advancing (local CAS reject) is surfaced immediately as the sentinel, so the
// RMW re-reads and retries rather than silently dropping the write.
func TestFanOutOwnerLocalFirst_LocalCASRejectSurfaces(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, "p1", "p2"}

	writeLocal := func() error { return errQuorumMetaCASReject }
	var peerCalled atomic.Bool
	writePeer := func(_ context.Context, _ string) error { peerCalled.Store(true); return nil }
	cleanupLocal := func() error { return nil }

	err := fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, cleanupLocal, writePeer)
	require.ErrorIs(t, err, errQuorumMetaCASReject,
		"a local CAS reject must surface so the RMW retries")
	require.False(t, peerCalled.Load(),
		"a failed owner-local write short-circuits before peer fan-out")
}

// TestFanOutOwnerLocalFirst_PeerFailureBudgetUnchanged verifies the N-K peer
// failure tolerance is preserved: with the local ack counted, k-1 of n-1 peers
// must ack. A K=2 over 4 nodes (owner + 3 peers) tolerates up to 2 peer failures
// (n-k = 4-2). The local write succeeds; 2 peers fail, 1 acks → success.
func TestFanOutOwnerLocalFirst_PeerFailureBudgetUnchanged(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, "p1", "p2", "p3"}
	failed := map[string]bool{"p1": true, "p2": true}

	writeLocal := func() error { return nil }
	cleanupLocal := func() error { return nil }
	writePeer := func(_ context.Context, node string) error {
		if failed[node] {
			return errors.New("peer down")
		}
		return nil
	}
	// Local ack + 1 peer ack = K=2 reached, 2 peer failures within budget.
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, cleanupLocal, writePeer))

	// One more peer failure (all 3 peers down) → K unreachable → error.
	allFail := func(_ context.Context, _ string) error { return errors.New("peer down") }
	require.Error(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 3, writeLocal, cleanupLocal, allFail),
		"K=3 with the local ack but all peers down is unreachable")
}

// TestFanOutOwnerLocalFirst_DuplicateSelfCountsAllLocalAcks pins the regression
// that a placement set listing self MORE THAN ONCE (e.g. EC over a single host:
// NodeIDs=[self,self,...]) must count every self entry as a local ack, exactly as
// the old all-node fan-out did. With selfCount >= k the single (idempotent) local
// write satisfies the whole quorum and NO peer RPC is attempted — otherwise the
// duplicate-self entries would be routed to a (non-existent) peer transport and
// spuriously fail the quorum.
func TestFanOutOwnerLocalFirst_DuplicateSelfCountsAllLocalAcks(t *testing.T) {
	ctx := context.Background()
	const self = "self"
	nodes := []string{self, self, self, self, self, self} // EC 4+2 over a single host

	var localCalls atomic.Int32
	writeLocal := func() error { localCalls.Add(1); return nil }
	cleanupLocal := func() error { return nil }
	var peerCalled atomic.Bool
	writePeer := func(_ context.Context, _ string) error { peerCalled.Store(true); return nil }

	// K=4 (ECData). All 6 placement entries are self → quorum satisfied locally.
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 4, writeLocal, cleanupLocal, writePeer))
	require.Equal(t, int32(1), localCalls.Load(), "owner-local write runs once (idempotent for duplicates)")
	require.False(t, peerCalled.Load(), "duplicate-self quorum must not attempt any peer RPC")
}

// TestFanOutOwnerLocalFirst_OwnerNotPlacement falls back to the plain all-node
// fan-out when the owner is not a placement node (no local copy to keep fresh).
func TestFanOutOwnerLocalFirst_OwnerNotPlacement(t *testing.T) {
	ctx := context.Background()
	nodes := []string{"p1", "p2", "p3"}

	var localCalled atomic.Bool
	writeLocal := func() error { localCalled.Store(true); return nil }
	cleanupLocal := func() error { return nil }
	var peerAcks atomic.Int32
	writePeer := func(_ context.Context, _ string) error { peerAcks.Add(1); return nil }

	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, "self-not-in-set", 2, writeLocal, cleanupLocal, writePeer))
	require.False(t, localCalled.Load(), "owner is not a placement node — local writer must not run")
}

// TestAppendBlobRMW_SecondCASReadsFirstMetaSeqWithSlowPeer is the BUG-2 end-to-end
// regression: two sequential CAS writes by the same owner both succeed and the
// second reads the first's MetaSeq from the REAL owner-local store — even when a
// peer write is deliberately SLOW. Before the fix the latest-only fan-out returned
// on the fast peer ack (K reached) while the owner-local write was still racing, so
// the second CAS write read a stale/absent local base, re-offered the same MetaSeq,
// and CAS-rejected. With owner-local-first the local blob is durable before each
// write returns, so the second reads MetaSeq 1 and lands MetaSeq 2.
func TestAppendBlobRMW_SecondCASReadsFirstMetaSeqWithSlowPeer(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	self := b.selfAddr
	nodes := []string{self, "slow-peer"} // owner + 1 slow peer, K=1

	// The owner-local writer is the REAL guarded store (writeQuorumMetaLocal). The
	// peer writer is slower than the local write so the plain fan-out would have
	// returned before the local blob was durable.
	localWrite := func(blob []byte) func() error {
		return func() error { return b.shardSvc.writeQuorumMetaLocal("bkt", "k", blob) }
	}
	slowPeerWrite := func(_ context.Context, _ string) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}
	cleanupLocal := func() error { return nil }

	// First CAS write: base absent → MetaSeq 1.
	blob1 := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true})
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 1, localWrite(blob1), cleanupLocal, slowPeerWrite))

	// The owner-local base read must reflect the first write's MetaSeq immediately.
	base, err := b.readQuorumMetaCmd("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, uint64(1), base.MetaSeq,
		"first write must be durable owner-local before the second reads its base")

	// Second CAS write computes MetaSeq = base+1 = 2 and lands (no stale-base reject).
	blob2 := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", ModTime: 101, MetaSeq: base.MetaSeq + 1, MetaSeqCAS: true})
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 1, localWrite(blob2), cleanupLocal, slowPeerWrite))

	after, err := b.readQuorumMetaCmd("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, uint64(2), after.MetaSeq, "second CAS write lands; no stale-base CAS reject")
}

func TestWriteQuorumMetaCAS_CleansOwnerLocalBlobOnPeerQuorumFailure(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	self := b.selfAddr
	cmd := PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100,
		MetaSeq: 1, MetaSeqCAS: true,
		NodeIDs: []string{self, "missing-peer"},
		ECData:  2, ECParity: 0,
	}

	err := b.writeQuorumMeta(ctx, cmd)
	require.Error(t, err, "peer quorum is unreachable")
	_, readErr := b.shardSvc.readQuorumMetaRaw("bkt", "k")
	require.ErrorIs(t, readErr, storage.ErrObjectNotFound,
		"failed CAS quorum write must not leave an owner-local phantom latest-only blob")
}

func TestWriteQuorumMetaCAS_RestoresPreviousBlobOnPeerQuorumFailure(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	self := b.selfAddr
	previous := PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100,
		MetaSeq: 1, MetaSeqCAS: true,
		NodeIDs: []string{self},
		ECData:  1,
	}
	previousBlob := mustEncodeMetaCmd(t, previous)
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", previousBlob))
	failedNext := PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v2", ModTime: 101,
		MetaSeq: 2, MetaSeqCAS: true,
		NodeIDs: []string{self, "missing-peer"},
		ECData:  2,
	}

	err := b.writeQuorumMeta(ctx, failedNext)
	require.Error(t, err, "peer quorum is unreachable")

	raw, readErr := b.shardSvc.readQuorumMetaRaw("bkt", "k")
	require.NoError(t, readErr)
	require.Equal(t, previousBlob, raw, "failed CAS quorum write must restore the previously committed latest blob")
}

func TestWriteQuorumMetaCAS_DoesNotDeletePreExistingIdenticalBlobOnPeerFailure(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	self := b.selfAddr
	committed := PutObjectMetaCmd{
		Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100,
		MetaSeq: 1, MetaSeqCAS: true,
		NodeIDs: []string{self, "missing-peer"},
		ECData:  2,
	}
	blob := mustEncodeMetaCmd(t, committed)
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", blob))

	replay := committed
	err := b.writeQuorumMeta(ctx, replay)
	require.Error(t, err, "peer quorum is unreachable")

	raw, readErr := b.shardSvc.readQuorumMetaRaw("bkt", "k")
	require.NoError(t, readErr)
	require.Equal(t, blob, raw, "failed idempotent replay must not delete a previously committed identical blob")
}
