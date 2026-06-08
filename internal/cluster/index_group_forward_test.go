package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// igForwardCluster brings up index groups for a SINGLE group ID across multiple
// nodes (one elects leader, the rest are followers), wiring each follower's
// forward hook through the PRODUCTION IndexGroupProposeForwardSender →
// in-proc transport → the leader-side IndexGroupProposeForwardReceiver. This is
// the path Slice 4b boot-wires; the Slice-4a igCluster shortcut (forward hook
// calls leader.ProposeWait directly) is deliberately NOT used here.
type igForwardCluster struct {
	*igCluster
	groupID  string
	receiver *IndexGroupProposeForwardReceiver
	// forwardCalls counts how many times any node's forward hook fired, so the
	// leader-local test can assert the hook was NOT invoked.
	forwardCalls atomic.Int64
}

// dial routes a forward payload to the leader-side receiver's Handle method,
// exactly as the real transport would deliver a StreamIndexGroupProposeForward
// message. The peer argument is the resolved leader target; in-proc we ignore
// it because the single receiver owns the leader's group.
func (c *igForwardCluster) dial(_ context.Context, _ string, payload []byte) ([]byte, error) {
	reply := c.receiver.Handle(&transport.Message{
		Type:    transport.StreamIndexGroupProposeForward,
		Payload: payload,
	})
	return reply.Payload, nil
}

// startForwardNode mirrors igCluster.startNode but wires the forward hook to the
// production sender rather than the direct-ProposeWait shortcut.
func (c *igForwardCluster) startForwardNode(t *testing.T, id string, peers []string, election time.Duration, sender *IndexGroupProposeForwardSender) *indexGroup {
	t.Helper()
	c.igCluster.startNodeWithForward(t, id, peers, election, func(self *indexGroup) indexGroupForwardFunc {
		return func(ctx context.Context, data []byte) (uint64, error) {
			c.forwardCalls.Add(1)
			// The exact hook Task 5 will install: resolve the current leader via
			// LeaderID(), forward through the production sender.
			return sender.Send(ctx, self.node.LeaderID(), c.groupID, data)
		}
	})
	return c.groups[id]
}

// startForward2 brings up n1(fast→leader)+n2(slow follower) for groupID, wiring
// the production forward path, and registers both groups in the receiver's
// IndexGroupManager so the leader-side Lookup succeeds.
func startForward2(t *testing.T, groupID string) *igForwardCluster {
	t.Helper()
	c := &igForwardCluster{igCluster: newIGCluster(), groupID: groupID}
	mgr := NewIndexGroupManager()
	c.receiver = NewIndexGroupProposeForwardReceiver(mgr)
	sender := NewIndexGroupProposeForwardSender(c.dial)

	c.startForwardNode(t, "n1", []string{"n2"}, igFastElection, sender)
	c.startForwardNode(t, "n2", []string{"n1"}, igSlowElection, sender)
	require.Eventually(t, func() bool { return c.leaderID() == "n1" }, 10*time.Second, 50*time.Millisecond,
		"n1 should win election")

	// Register both groups under the SAME group ID — the manager is keyed by
	// group ID and the receiver looks up by the forwarded group ID. The leader's
	// group is the one that actually proposes; registering both mirrors boot
	// (every node registers its local index group instance).
	mgr.register(groupID, c.groups["n1"], func() error { return nil })
	mgr.register(groupID, c.groups["n2"], func() error { return nil })
	// register is last-writer-wins on a single key; re-register the leader so the
	// receiver resolves the leader's group. (In production each node has its own
	// manager; here a single shared manager stands in for the leader node.)
	mgr.register(groupID, c.groups["n1"], func() error { return nil })
	return c
}

// TestIndexGroupForward_FollowerForwardThroughProductionPath is the core test:
// a PUT proposed at the FOLLOWER (n2) must travel the production sender +
// receiver path to the leader (n1), commit, and replicate to all nodes.
func TestIndexGroupForward_FollowerForwardThroughProductionPath(t *testing.T) {
	c := startForward2(t, "ig0")
	n2 := c.groups["n2"]
	require.False(t, c.lookup("n2").IsLeader(), "n2 must be a follower")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	before := c.forwardCalls.Load()
	require.NoError(t, n2.ProposeObjectIndex(ctx,
		ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "ig0", Size: 7, ModTime: 1}, false),
		"follower forward (put) through production path must succeed")
	require.Greater(t, c.forwardCalls.Load(), before,
		"forward hook MUST fire on the follower (non-leader forward path exercised)")

	for id, g := range c.groups {
		g := g
		require.Eventually(t, func() bool {
			got, ok := g.ObjectIndexLatest("b", "k")
			return ok && got.VersionID == "v1"
		}, 5*time.Second, 20*time.Millisecond, "node %s should see the forwarded put replicated", id)
	}
}

// TestIndexGroupForward_LeaderLocalDoesNotForward verifies that when the local
// node IS the leader, proposeOrForward proposes locally and the forward hook is
// never invoked.
func TestIndexGroupForward_LeaderLocalDoesNotForward(t *testing.T) {
	c := startForward2(t, "ig0")
	n1 := c.groups["n1"]
	require.True(t, c.lookup("n1").IsLeader(), "n1 must be the leader")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	before := c.forwardCalls.Load()
	require.NoError(t, n1.ProposeObjectIndex(ctx,
		ObjectIndexEntry{Bucket: "b", Key: "kl", VersionID: "v1", PlacementGroupID: "ig0", Size: 3, ModTime: 1}, false),
		"leader-local propose must succeed")
	require.Equal(t, before, c.forwardCalls.Load(),
		"forward hook MUST NOT fire when the local node is the leader")

	got, ok := n1.ObjectIndexLatest("b", "kl")
	require.True(t, ok)
	assert.Equal(t, "v1", got.VersionID)
}

// TestIndexGroupForward_ReceiverReturnsCommittedIndex verifies the receiver
// returns the leader's committed log index (non-zero) so the follower can wait
// for local apply.
func TestIndexGroupForward_ReceiverReturnsCommittedIndex(t *testing.T) {
	c := startForward2(t, "ig0")

	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, mustEncodePutIndex(t,
		ObjectIndexEntry{Bucket: "b", Key: "ki", VersionID: "v1", PlacementGroupID: "ig0", Size: 1, ModTime: 1}))
	require.NoError(t, err)

	payload := encodeGroupForwardPayload("ig0", data)
	reply := c.receiver.Handle(&transport.Message{
		Type:    transport.StreamIndexGroupProposeForward,
		Payload: payload,
	})
	idx, applyErr, transportErr := decodeProposeForwardReply(reply.Payload)
	require.NoError(t, transportErr)
	require.NoError(t, applyErr)
	assert.Greater(t, idx, uint64(0), "receiver must return the leader's committed index")
}

// TestIndexGroupForward_NotReadyRetryable is the boot-race test: when the group
// is NOT yet in the manager's map, the receiver replies with a RETRYABLE
// not-ready error that the sender distinguishes from a hard failure.
func TestIndexGroupForward_NotReadyRetryable(t *testing.T) {
	mgr := NewIndexGroupManager() // empty — group never registered
	receiver := NewIndexGroupProposeForwardReceiver(mgr)

	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, mustEncodePutIndex(t,
		ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "ig0", Size: 1, ModTime: 1}))
	require.NoError(t, err)
	payload := encodeGroupForwardPayload("ig0", data)

	reply := receiver.Handle(&transport.Message{
		Type:    transport.StreamIndexGroupProposeForward,
		Payload: payload,
	})

	// Real encode→decode round-trip: the not-ready signal must survive the wire
	// and be classified as RETRYABLE (distinct from a hard error).
	_, applyErr, transportErr := decodeProposeForwardReply(reply.Payload)
	require.NoError(t, transportErr, "not-ready must be a clean reply, not a truncated payload")
	require.Error(t, applyErr, "not-ready must carry an error")
	require.True(t, isIndexGroupNotReady(applyErr),
		"decoded error must be classified as retryable not-ready, got: %v", applyErr)

	// A hard error (e.g. a different message) must NOT be classified as not-ready.
	hardReply := encodeProposeForwardReply(0, fmt.Errorf("index group: some hard FSM apply failure"))
	_, hardErr, _ := decodeProposeForwardReply(hardReply)
	require.Error(t, hardErr)
	require.False(t, isIndexGroupNotReady(hardErr),
		"a hard error must NOT be mistaken for retryable not-ready")
}

// TestIndexGroupForward_SenderRetriesNotReady verifies the sender retries when
// the leader replies not-ready (boot race) and succeeds once the group appears.
func TestIndexGroupForward_SenderRetriesNotReady(t *testing.T) {
	c := startForward2(t, "ig0")

	// Swap the receiver's manager for one that is empty for the first N dials,
	// then resolves — simulating staggered boot where the group registers late.
	lateMgr := NewIndexGroupManager()
	lateReceiver := NewIndexGroupProposeForwardReceiver(lateMgr)
	var dialCount atomic.Int64
	sender := NewIndexGroupProposeForwardSender(func(ctx context.Context, _ string, payload []byte) ([]byte, error) {
		if dialCount.Add(1) == 1 {
			// First dial: group not yet present → not-ready.
			reply := lateReceiver.Handle(&transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload})
			return reply.Payload, nil
		}
		// Second dial: group now present.
		lateMgr.register("ig0", c.groups["n1"], func() error { return nil })
		reply := lateReceiver.Handle(&transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload})
		return reply.Payload, nil
	})

	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, mustEncodePutIndex(t,
		ObjectIndexEntry{Bucket: "b", Key: "kr", VersionID: "v1", PlacementGroupID: "ig0", Size: 1, ModTime: 1}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := sender.Send(ctx, "n1", "ig0", data)
	require.NoError(t, err, "sender must retry past the boot-race not-ready and succeed")
	assert.Greater(t, idx, uint64(0))
	assert.GreaterOrEqual(t, dialCount.Load(), int64(2), "sender must have retried at least once")
}

// TestIndexGroupForward_NotLeaderIsTerminal verifies that when the receiver
// resolves to a FOLLOWER (so node.ProposeWait returns ErrNotLeader), the sender
// surfaces a TERMINAL error from a SINGLE Send call — NOT a retryable not-ready.
// Send itself does not loop on not-leader (its leaderHint is fixed per call and
// the reply wire carries no leader-hint slot).
//
// IMPORTANT (do not mis-read as "matches the data-group path"): unlike the
// data-group DistributedBackend propose, which runs an INTERNAL bounded
// convergence loop (re-checks IsLeader, retries ErrNotLeader until proposeCtx,
// reconstructs the typed raft.ErrNotLeader sentinel — backend.go:1130-1199 /
// :904-905), neither Send nor the Slice-4a proposeOrForward loops today. That
// convergence loop is REQUIRED and lives in Task 4.7 (re-read LeaderID + re-check
// local IsLeader + treat not-leader AND empty-hint as retryable until ctx). This
// test pins ONLY the single-call terminal classification, which the Task-4.7 loop
// builds on; it does NOT claim the overall write path is terminal on not-leader.
func TestIndexGroupForward_NotLeaderIsTerminal(t *testing.T) {
	c := startForward2(t, "ig0")
	require.True(t, c.lookup("n1").IsLeader(), "n1 must be leader")
	require.False(t, c.lookup("n2").IsLeader(), "n2 must be a follower")

	// Point the manager's group for ig0 at the FOLLOWER (n2): the receiver will
	// call n2.node.ProposeWait, which returns ErrNotLeader.
	followerMgr := NewIndexGroupManager()
	followerMgr.register("ig0", c.groups["n2"], func() error { return nil })
	followerReceiver := NewIndexGroupProposeForwardReceiver(followerMgr)
	var dials atomic.Int64
	sender := NewIndexGroupProposeForwardSender(func(ctx context.Context, _ string, payload []byte) ([]byte, error) {
		dials.Add(1)
		reply := followerReceiver.Handle(&transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload})
		return reply.Payload, nil
	})

	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, mustEncodePutIndex(t,
		ObjectIndexEntry{Bucket: "b", Key: "knl", VersionID: "v1", PlacementGroupID: "ig0", Size: 1, ModTime: 1}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = sender.Send(ctx, "n2", "ig0", data)
	require.Error(t, err, "not-leader must surface as an error")
	require.False(t, isIndexGroupNotReady(err),
		"not-leader must be TERMINAL, not classified as retryable not-ready: %v", err)
	require.Equal(t, int64(1), dials.Load(),
		"sender must NOT retry a not-leader reply (single dial, terminal)")
}

// TestIndexGroupForward_ProposeOrForwardConvergesPastNotLeader is the Task 4.7
// test: a follower whose forward hook initially fails with a not-leader (and an
// empty-leader-hint) error — as happens at cold boot before an election settles,
// or during leadership churn with a stale LeaderID — must NOT fail terminally.
// proposeOrForward must re-invoke the hook on a bounded backoff until the group
// converges, then the write must land.
func TestIndexGroupForward_ProposeOrForwardConvergesPastNotLeader(t *testing.T) {
	c := startForward2(t, "ig0")
	n2 := c.groups["n2"]
	require.False(t, c.lookup("n2").IsLeader(), "n2 must be a follower")

	// Build a production sender whose dial routes to the leader-side receiver.
	sender := NewIndexGroupProposeForwardSender(c.dial)

	var calls atomic.Int64
	// Case 2 must yield a real not-leader reply, so route it to a receiver whose
	// manager maps ig0 → the FOLLOWER (n2). Case 1 uses the empty leader hint
	// (cold-boot). Case 3+ forwards to the real leader and lands the write.
	followerMgr := NewIndexGroupManager()
	followerMgr.register("ig0", c.groups["n2"], func() error { return nil })
	followerReceiver := NewIndexGroupProposeForwardReceiver(followerMgr)
	n2.forward = func(ctx context.Context, data []byte) (uint64, error) {
		switch calls.Add(1) {
		case 1:
			return sender.Send(ctx, "", "ig0", data) // empty-hint
		case 2:
			s := NewIndexGroupProposeForwardSender(func(ctx context.Context, _ string, payload []byte) ([]byte, error) {
				reply := followerReceiver.Handle(&transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload})
				return reply.Payload, nil
			})
			return s.Send(ctx, "n2", "ig0", data) // resolves to follower → not-leader
		default:
			return sender.Send(ctx, c.leaderID(), "ig0", data) // real leader
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, n2.ProposeObjectIndex(ctx,
		ObjectIndexEntry{Bucket: "b", Key: "kconv", VersionID: "v1", PlacementGroupID: "ig0", Size: 1, ModTime: 1}, false),
		"proposeOrForward must converge past not-leader / empty-hint and land the write")
	require.GreaterOrEqual(t, calls.Load(), int64(3),
		"proposeOrForward must have retried the forward hook past the retryable failures")

	for id, g := range c.groups {
		g := g
		require.Eventually(t, func() bool {
			got, ok := g.ObjectIndexLatest("b", "kconv")
			return ok && got.VersionID == "v1"
		}, 5*time.Second, 20*time.Millisecond, "node %s should see the converged write", id)
	}
}

// TestIndexGroupForward_ProposeOrForwardRespectsCtxCancel verifies the
// convergence loop is bounded by the caller ctx: a forward hook that never
// converges (always not-leader) must return promptly when ctx is cancelled,
// not spin forever.
func TestIndexGroupForward_ProposeOrForwardRespectsCtxCancel(t *testing.T) {
	c := startForward2(t, "ig0")
	n2 := c.groups["n2"]
	require.False(t, c.lookup("n2").IsLeader(), "n2 must be a follower")

	// Hook always fails not-leader (forwards to the follower receiver forever).
	followerMgr := NewIndexGroupManager()
	followerMgr.register("ig0", c.groups["n2"], func() error { return nil })
	followerReceiver := NewIndexGroupProposeForwardReceiver(followerMgr)
	sender := NewIndexGroupProposeForwardSender(func(ctx context.Context, _ string, payload []byte) ([]byte, error) {
		reply := followerReceiver.Handle(&transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload})
		return reply.Payload, nil
	})
	n2.forward = func(ctx context.Context, data []byte) (uint64, error) {
		return sender.Send(ctx, "n2", "ig0", data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := n2.ProposeObjectIndex(ctx,
		ObjectIndexEntry{Bucket: "b", Key: "kcancel", VersionID: "v1", PlacementGroupID: "ig0", Size: 1, ModTime: 1}, false)
	require.Error(t, err, "a never-converging forward must surface ctx error, not hang")
	require.Less(t, time.Since(start), 2*time.Second, "loop must return promptly after ctx deadline, not spin")
}

func mustEncodePutIndex(t *testing.T, entry ObjectIndexEntry) []byte {
	t.Helper()
	payload, err := encodeMetaPutObjectIndexCmd(entry, false)
	require.NoError(t, err)
	return payload
}
