package cluster

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

	err := fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, writePeer)
	require.NoError(t, err)
	require.True(t, localDone.Load(),
		"owner-local write must be durable before fanOut returns (BUG-2 stale-base fix)")
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

	err := fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, writePeer)
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
	writePeer := func(_ context.Context, node string) error {
		if failed[node] {
			return errors.New("peer down")
		}
		return nil
	}
	// Local ack + 1 peer ack = K=2 reached, 2 peer failures within budget.
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 2, writeLocal, writePeer))

	// One more peer failure (all 3 peers down) → K unreachable → error.
	allFail := func(_ context.Context, _ string) error { return errors.New("peer down") }
	require.Error(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 3, writeLocal, allFail),
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
	var peerCalled atomic.Bool
	writePeer := func(_ context.Context, _ string) error { peerCalled.Store(true); return nil }

	// K=4 (ECData). All 6 placement entries are self → quorum satisfied locally.
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 4, writeLocal, writePeer))
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
	var peerAcks atomic.Int32
	writePeer := func(_ context.Context, _ string) error { peerAcks.Add(1); return nil }

	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, "self-not-in-set", 2, writeLocal, writePeer))
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

	// First CAS write: base absent → MetaSeq 1.
	blob1 := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true})
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 1, localWrite(blob1), slowPeerWrite))

	// The owner-local base read must reflect the first write's MetaSeq immediately.
	base, err := b.readQuorumMetaCmd("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, uint64(1), base.MetaSeq,
		"first write must be durable owner-local before the second reads its base")

	// Second CAS write computes MetaSeq = base+1 = 2 and lands (no stale-base reject).
	blob2 := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", ModTime: 101, MetaSeq: base.MetaSeq + 1, MetaSeqCAS: true})
	require.NoError(t, fanOutQuorumMetaOwnerLocalFirst(ctx, nodes, self, 1, localWrite(blob2), slowPeerWrite))

	after, err := b.readQuorumMetaCmd("bkt", "k")
	require.NoError(t, err)
	require.Equal(t, uint64(2), after.MetaSeq, "second CAS write lands; no stale-base CAS reject")
}
