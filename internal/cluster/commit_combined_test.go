package cluster

// Coordinator-side tests for the combined PUT commit tail
// (QuorumMetaStore.promoteAndWriteQuorumMeta): one per-node round that promotes
// staged shards AND writes the latest-only quorum-meta blob, with the full-guard
// semantics — 3-class errors, vote-multiplicity quorum over cmd.NodeIDs as a
// LIST, capability gating with legacy fallback, drain-before-return on abort,
// and abort-path content-matched meta rollback.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/stretchr/testify/require"
)

// --- extend the pre-existing fakePeerQuorumMeta with the new interface methods
// (methods may live in any file of the package; the base fake fails loudly if a
// test that did not opt into the combined round reaches it).

func (f *fakePeerQuorumMeta) PromoteAndWriteQuorumMeta(ctx context.Context, addr, bucket, key string, pairs []stagedPromotePair, blob []byte) (bool, []byte, bool, error) {
	return false, nil, false, fmt.Errorf("fakePeerQuorumMeta: unexpected PromoteAndWriteQuorumMeta to %s", addr)
}

func (f *fakePeerQuorumMeta) RollbackQuorumMetaIfMatch(ctx context.Context, addr, bucket, key string, expected, previous []byte, hadPrevious bool) error {
	return fmt.Errorf("fakePeerQuorumMeta: unexpected RollbackQuorumMetaIfMatch to %s", addr)
}

// --- combined-round fakes ------------------------------------------------------

type combinedDispatchRecord struct {
	node     string
	pairsLen int
	blobNil  bool
}

// fakeCombinedPeer records combined dispatches per node (addr == node id via the
// identity resolvePeerAddress) and can fail or gate individual nodes.
type fakeCombinedPeer struct {
	*fakePeerQuorumMeta
	mu             sync.Mutex
	dispatches     []combinedDispatchRecord
	errs           map[string]error  // addr → dispatch result
	resolveErrs    map[string]error  // node → resolvePeerAddress failure injection
	ackPrevious    map[string][]byte // addr → previous blob returned on a clean ack
	ackNoOp        map[string]bool   // addr → clean ack with applied=false (idempotent replay / LWW skip)
	ackAfterCancel map[string]bool   // addr → block until ctx canceled, then return errs[addr]
	block          map[string]chan struct{}
	inFlight       atomic.Int32
	rollbackNodes  []string
	rollbackBlobs  map[string][]byte
	rollbackPrev   map[string][]byte
	rollbackHad    map[string]bool
}

func newFakeCombinedPeer() *fakeCombinedPeer {
	return &fakeCombinedPeer{
		fakePeerQuorumMeta: newFakePeerQuorumMeta(nil),
		errs:               map[string]error{},
		resolveErrs:        map[string]error{},
		ackPrevious:        map[string][]byte{},
		ackNoOp:            map[string]bool{},
		ackAfterCancel:     map[string]bool{},
		block:              map[string]chan struct{}{},
		rollbackBlobs:      map[string][]byte{},
		rollbackPrev:       map[string][]byte{},
		rollbackHad:        map[string]bool{},
	}
}

func (f *fakeCombinedPeer) PromoteAndWriteQuorumMeta(ctx context.Context, addr, bucket, key string, pairs []stagedPromotePair, blob []byte) (bool, []byte, bool, error) {
	f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	f.mu.Lock()
	f.dispatches = append(f.dispatches, combinedDispatchRecord{node: addr, pairsLen: len(pairs), blobNil: blob == nil})
	err := f.errs[addr]
	waitCancel := f.ackAfterCancel[addr]
	gate := f.block[addr]
	prev, hadPrev := f.ackPrevious[addr]
	applied := blob != nil && !f.ackNoOp[addr]
	f.mu.Unlock()
	if waitCancel {
		<-ctx.Done() // "late" node: answers only after the abort's cancel
		if err == nil {
			return applied, prev, hadPrev, nil
		}
		return false, nil, false, err
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
	if err != nil {
		return false, nil, false, err
	}
	return applied, prev, hadPrev, nil
}

func (f *fakeCombinedPeer) RollbackQuorumMetaIfMatch(ctx context.Context, addr, bucket, key string, expected, previous []byte, hadPrevious bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollbackNodes = append(f.rollbackNodes, addr)
	f.rollbackBlobs[addr] = append([]byte(nil), expected...)
	f.rollbackPrev[addr] = append([]byte(nil), previous...)
	f.rollbackHad[addr] = hadPrevious
	return nil
}

// resolvePeerAddress overrides the identity resolver so a test can inject a
// per-node resolution failure (the placement node isn't in the address book).
func (f *fakeCombinedPeer) resolvePeerAddress(peer string) (string, error) {
	f.mu.Lock()
	err := f.resolveErrs[peer]
	f.mu.Unlock()
	if err != nil {
		return "", err
	}
	return peer, nil
}

func (f *fakeCombinedPeer) dispatchSnapshot() []combinedDispatchRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]combinedDispatchRecord(nil), f.dispatches...)
}

func (f *fakeCombinedPeer) rollbackSnapshot() ([]string, map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	blobs := make(map[string][]byte, len(f.rollbackBlobs))
	for k, v := range f.rollbackBlobs {
		blobs[k] = append([]byte(nil), v...)
	}
	return append([]string(nil), f.rollbackNodes...), blobs
}

// failingLocalQuorumMeta wraps the in-memory local fake with a failing
// latest-only write (self meta-write failure injection).
type failingLocalQuorumMeta struct {
	*fakeLocalQuorumMeta
	writeErr error
}

func (f *failingLocalQuorumMeta) writeQuorumMetaLocal(bucket, key string, data []byte) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	return f.fakeLocalQuorumMeta.writeQuorumMetaLocal(bucket, key, data)
}

func (f *failingLocalQuorumMeta) writeQuorumMetaLocalWithResult(bucket, key string, data []byte) (quorumMetaLocalWriteResult, error) {
	if f.writeErr != nil {
		return quorumMetaLocalWriteResult{}, f.writeErr
	}
	return f.fakeLocalQuorumMeta.writeQuorumMetaLocalWithResult(bucket, key, data)
}

// combinedFixture wires a QuorumMetaStore over the combined fakes plus the new
// seams (promoteLocalStaged / rollbackLocalIfMatch / capabilityEvidence).
type selfRollbackRecord struct {
	expected    []byte
	previous    []byte
	hadPrevious bool
}

type combinedFixture struct {
	local         *fakeLocalQuorumMeta
	peer          *fakeCombinedPeer
	store         *QuorumMetaStore
	self          string
	selfPromotes  atomic.Int32
	legacyCalls   atomic.Int32
	selfRollbacks []selfRollbackRecord
}

func newCombinedFixture(versioning, self string, capableNodes ...string) *combinedFixture {
	fx := &combinedFixture{
		local: newFakeLocalQuorumMeta(nil),
		peer:  newFakeCombinedPeer(),
		self:  self,
	}
	fx.store = &QuorumMetaStore{
		local:      func() localQuorumMetaStore { return fx.local },
		peer:       func() quorumMetaPeerRPC { return fx.peer },
		groups:     func() ShardGroupSource { return nil },
		versioning: fakeVersioning{state: versioning},
		selfAddr:   func() string { return self },
		multiGen:   &atomic.Bool{},
	}
	fx.store.promoteLocalStaged = func(bucket, stagingKey, finalKey string, logicalShardSize int64) error {
		fx.selfPromotes.Add(1)
		return nil
	}
	fx.store.rollbackLocalIfMatch = func(bucket, key string, expected, previous []byte, hadPrevious bool) error {
		fx.selfRollbacks = append(fx.selfRollbacks, selfRollbackRecord{expected: append([]byte(nil), expected...), previous: append([]byte(nil), previous...), hadPrevious: hadPrevious})
		return nil
	}
	fx.store.capabilityProbe = grantCommitCombined(capableNodes...)
	return fx
}

func (fx *combinedFixture) legacyPromote(context.Context) error {
	fx.legacyCalls.Add(1)
	return nil
}

// grantCommitCombined returns a capability probe (func(nodeID string) bool) that
// reports the listed nodes as commit-combined capable. Node IDs are used
// directly (the fixture's identity resolvePeerAddress means node id == addr).
func grantCommitCombined(nodes ...string) func(nodeID string) bool {
	capable := map[string]bool{}
	for _, n := range nodes {
		capable[n] = true
	}
	return func(nodeID string) bool { return capable[nodeID] }
}

func combinedTestCmd(nodeIDs []string, ecData uint8) PutObjectMetaCmd {
	return PutObjectMetaCmd{
		Bucket: "b", Key: "obj", ModTime: 500, ETag: "e-combined",
		NodeIDs: nodeIDs, ECData: ecData,
	}
}

func combinedTestPromotes(nodes ...string) (map[string][]stagedPromotePair, []string) {
	promotes := map[string][]stagedPromotePair{}
	order := make([]string, 0, len(nodes))
	for _, n := range nodes {
		promotes[n] = []stagedPromotePair{{
			stagingKey: ".segstaging/txn/" + n, finalKey: "obj/segments/" + n, logicalShardSize: 42,
		}}
		order = append(order, n)
	}
	return promotes, order
}

// --- tests ---------------------------------------------------------------------

func TestPromoteAndWriteQuorumMeta_CombinedAllOK(t *testing.T) {
	self := "self"
	remotes := []string{"n2", "n3", "n4"}
	all := append([]string{self}, remotes...)
	fx := newCombinedFixture("", self, all...)
	promotes, order := combinedTestPromotes(all...)
	cmd := combinedTestCmd(all, 2)

	require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))

	// Each REMOTE node dispatched exactly once, with pairs AND blob.
	recs := fx.peer.dispatchSnapshot()
	require.Len(t, recs, len(remotes))
	seen := map[string]combinedDispatchRecord{}
	for _, r := range recs {
		_, dup := seen[r.node]
		require.False(t, dup, "node %s dispatched more than once", r.node)
		seen[r.node] = r
	}
	for _, n := range remotes {
		r, ok := seen[n]
		require.True(t, ok, "node %s not dispatched", n)
		require.Equal(t, 1, r.pairsLen)
		require.False(t, r.blobNil)
	}
	// Self executed locally: promote + meta write.
	require.Equal(t, int32(1), fx.selfPromotes.Load())
	blob, err := fx.local.readQuorumMetaRaw("b", "obj")
	require.NoError(t, err)
	want, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)
	require.Equal(t, want, blob)
	require.Equal(t, int32(0), fx.legacyCalls.Load(), "combined round must not run the legacy promote")
}

func TestPromoteAndWriteQuorumMeta_ErrorClasses(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	tests := []struct {
		name    string
		nodeErr error // injected on n2 (a pairs-carrying node)
		wantErr bool
	}{
		{"promote_failure_aborts", fmt.Errorf("stage gone: %w", errCombinedPromoteFailed), true},
		{"meta_failure_on_pairs_node_tolerated", fmt.Errorf("disk full: %w", errCombinedMetaFailed), false},
		{"transport_failure_on_pairs_node_aborts", errors.New("connection reset (promote state unknown)"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newCombinedFixture("", "coordinator", nodes...)
			fx.peer.errs["n2"] = tc.nodeErr
			promotes, order := combinedTestPromotes(nodes...)
			cmd := combinedTestCmd(nodes, 2) // K=2, n=4 → up to 2 meta-vote failures tolerable

			err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err, "a positively-identified meta-class failure within n−K must not fail the PUT")
			}
		})
	}
}

func TestPromoteAndWriteQuorumMeta_MetaQuorumShort(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	fx := newCombinedFixture("", "coordinator", nodes...)
	for _, n := range []string{"n1", "n2", "n3"} {
		fx.peer.errs[n] = fmt.Errorf("meta write failed: %w", errCombinedMetaFailed)
	}
	promotes, order := combinedTestPromotes(nodes...)
	cmd := combinedTestCmd(nodes, 2)

	err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)
	require.Contains(t, err.Error(), "quorum")
}

// A pairs-only node (staged shards on a node OUTSIDE cmd.NodeIDs — a segment
// whose placement group extends beyond the meta placement) breaks the global
// promote-before-meta barrier IN A CRASH: meta-carrying nodes can persist the
// blob before the pairs-only node's promote lands, and a coordinator crash
// then leaves committed metadata referencing staged shards with NO cleanup or
// rollback running. The combined round therefore refuses such placements and
// falls back to the legacy two-round flow (global promote barrier).
func TestPromoteAndWriteQuorumMeta_PairsOnlyNodeForcesLegacyFallback(t *testing.T) {
	quorumNodes := []string{"n1", "n2", "n3", "n4"}
	allWithPairs := append([]string{"n5"}, quorumNodes...)

	fx := newCombinedFixture("", "coordinator", allWithPairs...)
	promotes, order := combinedTestPromotes(allWithPairs...)
	cmd := combinedTestCmd(quorumNodes, 2) // n5 has pairs but NOT in NodeIDs

	require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
	require.Empty(t, fx.peer.dispatchSnapshot(), "a pairs-only node must force the legacy two-round flow (global promote barrier)")
	require.Equal(t, int32(1), fx.legacyCalls.Load(), "legacyPromote must run exactly once")
}

func TestPromoteAndWriteQuorumMeta_MetaOnlyNodeFailureTolerated(t *testing.T) {
	pairsNodes := []string{"n1", "n2", "n3"}
	all := append([]string(nil), pairsNodes...)
	all = append(all, "n4") // meta-only: in NodeIDs, no pairs
	fx := newCombinedFixture("", "coordinator", all...)
	fx.peer.errs["n4"] = errors.New("plain transport failure")
	promotes, order := combinedTestPromotes(pairsNodes...)
	cmd := combinedTestCmd(all, 2) // n=4, K=2 → tolerance 2

	require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote),
		"a meta-only node's failure counts toward metaFailed (tolerated up to n−K), never as a promote abort")
	for _, r := range fx.peer.dispatchSnapshot() {
		if r.node == "n4" {
			require.Equal(t, 0, r.pairsLen, "meta-only node must be dispatched with zero pairs")
		}
	}
}

func TestPromoteAndWriteQuorumMeta_EarlyReturnAtQuorum(t *testing.T) {
	pairsNodes := []string{"n1", "n2", "n3"}
	all := append(append([]string(nil), pairsNodes...), "n4") // n4 = pairs-less meta straggler
	fx := newCombinedFixture("", "coordinator", all...)
	fx.peer.block["n4"] = make(chan struct{}) // never released: only ctx cancel unblocks
	promotes, order := combinedTestPromotes(pairsNodes...)
	cmd := combinedTestCmd(all, 2)

	done := make(chan error, 1)
	go func() {
		done <- fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("promoteAndWriteQuorumMeta waited for a pairs-less meta straggler after quorum was met")
	}
}

func TestPromoteAndWriteQuorumMeta_VersionedFallsBack(t *testing.T) {
	fx := newCombinedFixture("Enabled", "self", "self")
	promotes, order := combinedTestPromotes("self")
	cmd := combinedTestCmd([]string{"self"}, 1)
	cmd.VersionID = "019ed400-0000-7000-8000-00000000000a"

	require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
	require.Empty(t, fx.peer.dispatchSnapshot(), "versioned bucket must never take the combined round")
	require.Equal(t, int32(0), fx.selfPromotes.Load(), "combined self-dispatch must not run on the legacy path")
	require.Equal(t, int32(1), fx.legacyCalls.Load(), "legacyPromote must run exactly once")
	// Legacy writeQuorumMeta ran: per-version blob + latest-only blob on the local fake.
	raws, err := fx.local.readQuorumMetaVersionsRawLocal(cmd.Bucket, cmd.Key)
	require.NoError(t, err)
	require.Len(t, raws, 1, "versioned legacy path must write the per-version blob")
	_, err = fx.local.readQuorumMetaRaw(cmd.Bucket, cmd.Key)
	require.NoError(t, err, "versioned legacy path must write the latest-only blob")
}

func TestPromoteAndWriteQuorumMeta_MetaSeqCASFallsBack(t *testing.T) {
	fx := newCombinedFixture("", "self", "self")
	promotes, order := combinedTestPromotes("self")
	cmd := combinedTestCmd([]string{"self"}, 1)
	cmd.MetaSeqCAS = true
	cmd.MetaSeq = 1

	require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
	require.Empty(t, fx.peer.dispatchSnapshot(), "MetaSeqCAS must keep the owner-local-first legacy sequence")
	require.Equal(t, int32(0), fx.selfPromotes.Load())
	require.Equal(t, int32(1), fx.legacyCalls.Load())
	_, err := fx.local.readQuorumMetaRaw(cmd.Bucket, cmd.Key)
	require.NoError(t, err, "legacy CAS write must have persisted the latest-only blob")
}

func TestPromoteAndWriteQuorumMeta_CapabilityGate(t *testing.T) {
	t.Run("capability_missing_falls_back", func(t *testing.T) {
		// n2 lacks the commit-combined capability → legacy two-round flow.
		fx := newCombinedFixture("", "self", "self") // evidence grants nothing for n2
		promotes, order := combinedTestPromotes("self", "n2")
		cmd := combinedTestCmd([]string{"self", "n2"}, 2)

		require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
		require.Empty(t, fx.peer.dispatchSnapshot())
		require.Equal(t, int32(1), fx.legacyCalls.Load())
		_, err := fx.local.readQuorumMetaRaw(cmd.Bucket, cmd.Key)
		require.NoError(t, err, "legacy fallback must have written the latest-only blob")
	})

	t.Run("capability_nil_probe_falls_back", func(t *testing.T) {
		fx := newCombinedFixture("", "self")
		fx.store.capabilityProbe = nil
		promotes, order := combinedTestPromotes("self", "n2")
		cmd := combinedTestCmd([]string{"self", "n2"}, 2)

		require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
		require.Empty(t, fx.peer.dispatchSnapshot())
		require.Equal(t, int32(1), fx.legacyCalls.Load())
	})
}

func TestPromoteAndWriteQuorumMeta_DuplicateNodeIDsVoteWeighting(t *testing.T) {
	t.Run("single_dispatch_two_votes", func(t *testing.T) {
		fx := newCombinedFixture("", "self", "self")
		promotes, order := combinedTestPromotes("self")
		cmd := combinedTestCmd([]string{"self", "self"}, 2) // vote LIST: self counts twice

		require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
		require.Empty(t, fx.peer.dispatchSnapshot(), "single-node placement must dispatch zero peer RPCs")
		require.Equal(t, int32(1), fx.selfPromotes.Load(), "exactly ONE local dispatch for the duplicated self votes")
		_, err := fx.local.readQuorumMetaRaw(cmd.Bucket, cmd.Key)
		require.NoError(t, err)
	})

	t.Run("single_dispatch_meta_failure_is_two_failed_votes", func(t *testing.T) {
		fx := newCombinedFixture("", "self", "self")
		fx.store.local = func() localQuorumMetaStore {
			return &failingLocalQuorumMeta{
				fakeLocalQuorumMeta: fx.local,
				writeErr:            errors.New("read-only meta dir"),
			}
		}
		promotes, order := combinedTestPromotes("self")
		cmd := combinedTestCmd([]string{"self", "self"}, 2) // n=2, K=2 → tolerance 0

		err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
		require.Error(t, err)
		require.NotErrorIs(t, err, errCombinedPromoteFailed, "a self META write failure is quorum loss, not a promote abort")
		require.Contains(t, err.Error(), "quorum")
	})
}

func TestPromoteAndWriteQuorumMeta_AbortRollsBackAckedMetas(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	fx := newCombinedFixture("", "coordinator", nodes...)
	fx.peer.errs["n3"] = fmt.Errorf("promote exploded: %w", errCombinedPromoteFailed)
	fx.peer.ackAfterCancel["n4"] = true // acks only after the abort's cancel — the drained late ack
	promotes, order := combinedTestPromotes(nodes...)
	cmd := combinedTestCmd(nodes, 2)

	err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)

	// Rollback ran BEFORE the call returned: the snapshot taken immediately after
	// return already holds the full acked set — including the drained late ack.
	rollbackNodes, rollbackBlobs := fx.peer.rollbackSnapshot()
	require.ElementsMatch(t, []string{"n1", "n2", "n4"}, rollbackNodes,
		"every node whose combined call ACKED (its meta persisted) must be rolled back")
	want, encErr := encodeQuorumMetaBlob(cmd)
	require.NoError(t, encErr)
	for node, blob := range rollbackBlobs {
		require.Equal(t, want, blob, "rollback for %s must be content-matched against the round's blob", node)
	}
}

// A meta-class failure does NOT prove the blob was not published: the local
// write can fail AFTER the rename made it visible (e.g. the directory fsync
// errors). Content-matched rollback on a node that truly published nothing is
// a no-op, so meta-failed nodes must be INCLUDED in the abort rollback set
// (delete-only — the error response carries no previous blob).
func TestPromoteAndWriteQuorumMeta_AbortRollsBackMetaFailedNodesToo(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	fx := newCombinedFixture("", "coordinator", nodes...)
	fx.peer.errs["n2"] = fmt.Errorf("promote exploded: %w", errCombinedPromoteFailed) // abort trigger
	fx.peer.errs["n3"] = fmt.Errorf("dir fsync after rename: %w", errCombinedMetaFailed)
	promotes, order := combinedTestPromotes(nodes...)
	cmd := combinedTestCmd(nodes, 2)

	err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)

	rollbackNodes, _ := fx.peer.rollbackSnapshot()
	require.Contains(t, rollbackNodes, "n3",
		"a meta-failed node may have published (rename before the failing fsync) and must be rolled back")
	require.NotContains(t, rollbackNodes, "n2",
		"a promote-failed node positively never reached the meta write")
}

// A clean NO-OP ack (applied=false: the identical blob was already on disk —
// an idempotent retry, or an LWW skip) means this round published NOTHING on
// that node. Rolling it back would content-match the PRIOR committed PUT's
// identical metadata and delete it — the no-op node must be excluded.
func TestPromoteAndWriteQuorumMeta_NoOpAckNotRolledBack(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	fx := newCombinedFixture("", "coordinator", nodes...)
	fx.peer.ackNoOp["n1"] = true // identical blob already on disk from a prior committed PUT
	fx.peer.errs["n3"] = fmt.Errorf("promote exploded: %w", errCombinedPromoteFailed)
	promotes, order := combinedTestPromotes(nodes...)
	cmd := combinedTestCmd(nodes, 2)

	err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)

	rollbackNodes, _ := fx.peer.rollbackSnapshot()
	require.NotContains(t, rollbackNodes, "n1",
		"a no-op ack published nothing this round; rollback would delete the prior committed identical blob")
	require.Contains(t, rollbackNodes, "n2", "an applied ack is still rolled back")
}

func TestPromoteAndWriteQuorumMeta_ResolveFailure(t *testing.T) {
	t.Run("pairs_node_resolve_failure_aborts", func(t *testing.T) {
		nodes := []string{"n1", "n2", "n3", "n4"}
		fx := newCombinedFixture("", "coordinator", nodes...)
		fx.peer.resolveErrs["n2"] = errors.New("node not in address book")
		promotes, order := combinedTestPromotes(nodes...) // n2 carries pairs
		cmd := combinedTestCmd(nodes, 2)
		err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
		require.Error(t, err, "resolve failure on a pairs-carrying node is promote-class → abort")
		require.ErrorIs(t, err, errCombinedPromoteFailed)
	})

	t.Run("meta_only_node_resolve_failure_tolerated", func(t *testing.T) {
		pairsNodes := []string{"n1", "n2", "n3"}
		all := append(append([]string(nil), pairsNodes...), "n4") // n4 = meta-only
		fx := newCombinedFixture("", "coordinator", all...)
		fx.peer.resolveErrs["n4"] = errors.New("node not in address book")
		promotes, order := combinedTestPromotes(pairsNodes...)
		cmd := combinedTestCmd(all, 2) // n=4, K=2 → tolerance 2
		require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote),
			"a meta-only node's resolve failure counts as a tolerated meta failure, not a promote abort")
	})
}

func TestPromoteAndWriteQuorumMeta_SelfPromoteNotWired(t *testing.T) {
	fx := newCombinedFixture("", "self", "self")
	fx.store.promoteLocalStaged = nil // self carries pairs but promote is unwired
	promotes, order := combinedTestPromotes("self")
	cmd := combinedTestCmd([]string{"self"}, 1)
	err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)
	require.ErrorIs(t, err, errCombinedPromoteFailed, "an unwired self promote is promote-class → abort")
}

func TestPromoteAndWriteQuorumMeta_EntryGuards(t *testing.T) {
	t.Run("empty_node_ids", func(t *testing.T) {
		fx := newCombinedFixture("", "self", "self")
		cmd := combinedTestCmd(nil, 1) // empty placement
		err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, nil, nil, fx.legacyPromote)
		require.Error(t, err)
	})

	t.Run("votes_below_quorum", func(t *testing.T) {
		fx := newCombinedFixture("", "self", "self", "n2")
		promotes, order := combinedTestPromotes("self", "n2")
		cmd := combinedTestCmd([]string{"self", "n2"}, 3) // K=3 > 2 votes
		err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
		require.Error(t, err)
		require.Contains(t, err.Error(), "placement votes")
	})
}

func TestPromoteAndWriteQuorumMeta_SelfRollbackOnAbort(t *testing.T) {
	self := "self"
	nodes := []string{self, "n2", "n3"}
	fx := newCombinedFixture("", self, nodes...)
	// Pre-seed a previous blob on self so the ack captures it (overwrite PUT).
	prevBlob, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: "b", Key: "obj", ModTime: 100, ETag: "old", NodeIDs: nodes, ECData: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fx.local.writeQuorumMetaLocal("b", "obj", prevBlob))
	// n3 promote-fails → abort; self + n2 acked → rolled back.
	fx.peer.errs["n3"] = fmt.Errorf("promote gone: %w", errCombinedPromoteFailed)
	promotes, order := combinedTestPromotes(nodes...)
	cmd := combinedTestCmd(nodes, 2) // ModTime 500 > 100 → overwrites the previous

	err = fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)

	require.Len(t, fx.selfRollbacks, 1, "self acked then abort → exactly one self rollback")
	rb := fx.selfRollbacks[0]
	want, encErr := encodeQuorumMetaBlob(cmd)
	require.NoError(t, encErr)
	require.Equal(t, want, rb.expected, "self rollback is content-matched against the round's blob")
	require.True(t, rb.hadPrevious, "overwrite PUT abort must RESTORE the previous, not delete")
	require.Equal(t, prevBlob, rb.previous, "self rollback carries the exact overwritten previous blob")
}

// TestNewCommitCombinedProbe covers FIX 4's core: capability evidence is keyed
// by the RESOLVED raft address, while placement targets are raw node IDs, so the
// probe MUST resolve before checking the gate.
func TestNewCommitCombinedProbe(t *testing.T) {
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.ReportEvidence(compat.Evidence{
		NodeID:       compat.NodeID("127.0.0.1:7001"), // keyed by ADDRESS
		Capabilities: map[string]bool{capabilityCommitCombined: true},
		LastSeen:     time.Now(),
		Ready:        true,
	})
	book := probeAddrBook{nodes: []MetaNodeEntry{{ID: "node-1", Address: "127.0.0.1:7001"}}}
	probe := NewCommitCombinedProbe(gate, book)

	require.True(t, probe("node-1"), "a node whose resolved address has commit-combined evidence is capable")
	require.False(t, probe("node-unknown"), "an unresolvable node ID is not capable (→ legacy fallback)")
	require.False(t, NewCommitCombinedProbe(nil, book)("node-1"), "a nil gate is never capable")
}

// TestCommitCombinedCapableAllSelfSkipsProbe: an all-self placement must never
// call the probe (solo: zero gate overhead).
func TestCommitCombinedCapableAllSelfSkipsProbe(t *testing.T) {
	probeCalls := 0
	s := &QuorumMetaStore{
		selfAddr:        func() string { return "self" },
		capabilityProbe: func(string) bool { probeCalls++; return true },
	}
	require.True(t, s.commitCombinedCapable([]string{"self", "self"}))
	require.Equal(t, 0, probeCalls, "all-self targets must short-circuit without probing")
}

type probeAddrBook struct{ nodes []MetaNodeEntry }

func (b probeAddrBook) Nodes() []MetaNodeEntry { return b.nodes }

func TestPromoteAndWriteQuorumMeta_AbortDrainsBeforeReturn(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	fx := newCombinedFixture("", "coordinator", nodes...)
	fx.peer.errs["n3"] = fmt.Errorf("promote exploded: %w", errCombinedPromoteFailed)
	fx.peer.ackAfterCancel["n4"] = true
	promotes, order := combinedTestPromotes(nodes...)
	cmd := combinedTestCmd(nodes, 2)

	err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
	require.Error(t, err)
	require.Equal(t, int32(0), fx.peer.inFlight.Load(),
		"no dispatch goroutine may still be promoting/writing when the abort returns (legacy g.Wait() parity)")
	require.Len(t, fx.peer.dispatchSnapshot(), len(nodes), "every target must have been dispatched and drained")
}
