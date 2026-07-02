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

	"github.com/stretchr/testify/require"
)

// --- extend the pre-existing fakePeerQuorumMeta with the new interface methods
// (methods may live in any file of the package; the base fake fails loudly if a
// test that did not opt into the combined round reaches it).

func (f *fakePeerQuorumMeta) PromoteAndWriteQuorumMeta(ctx context.Context, addr, bucket, key string, pairs []stagedPromotePair, blob []byte) error {
	return fmt.Errorf("fakePeerQuorumMeta: unexpected PromoteAndWriteQuorumMeta to %s", addr)
}

func (f *fakePeerQuorumMeta) RollbackQuorumMetaIfMatch(ctx context.Context, addr, bucket, key string, expected []byte) error {
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
	errs           map[string]error // addr → dispatch result
	ackAfterCancel map[string]bool  // addr → block until ctx canceled, then return errs[addr]
	block          map[string]chan struct{}
	inFlight       atomic.Int32
	rollbackNodes  []string
	rollbackBlobs  map[string][]byte
}

func newFakeCombinedPeer() *fakeCombinedPeer {
	return &fakeCombinedPeer{
		fakePeerQuorumMeta: newFakePeerQuorumMeta(nil),
		errs:               map[string]error{},
		ackAfterCancel:     map[string]bool{},
		block:              map[string]chan struct{}{},
		rollbackBlobs:      map[string][]byte{},
	}
}

func (f *fakeCombinedPeer) PromoteAndWriteQuorumMeta(ctx context.Context, addr, bucket, key string, pairs []stagedPromotePair, blob []byte) error {
	f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	f.mu.Lock()
	f.dispatches = append(f.dispatches, combinedDispatchRecord{node: addr, pairsLen: len(pairs), blobNil: blob == nil})
	err := f.errs[addr]
	waitCancel := f.ackAfterCancel[addr]
	gate := f.block[addr]
	f.mu.Unlock()
	if waitCancel {
		<-ctx.Done() // "late" node: answers only after the abort's cancel
		return err
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
	return err
}

func (f *fakeCombinedPeer) RollbackQuorumMetaIfMatch(ctx context.Context, addr, bucket, key string, expected []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollbackNodes = append(f.rollbackNodes, addr)
	f.rollbackBlobs[addr] = append([]byte(nil), expected...)
	return nil
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

// combinedFixture wires a QuorumMetaStore over the combined fakes plus the new
// seams (promoteLocalStaged / rollbackLocalIfMatch / capabilityEvidence).
type combinedFixture struct {
	local        *fakeLocalQuorumMeta
	peer         *fakeCombinedPeer
	store        *QuorumMetaStore
	self         string
	selfPromotes atomic.Int32
	legacyCalls  atomic.Int32
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
	fx.store.rollbackLocalIfMatch = func(bucket, key string, expected []byte) error { return nil }
	fx.store.capabilityEvidence = grantCommitCombined(capableNodes...)
	return fx
}

func (fx *combinedFixture) legacyPromote(context.Context) error {
	fx.legacyCalls.Add(1)
	return nil
}

func grantCommitCombined(nodes ...string) func() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, n := range nodes {
		out[n] = map[string]bool{capabilityCommitCombined: true}
	}
	return func() map[string]map[string]bool { return out }
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

func TestPromoteAndWriteQuorumMeta_PairsOnlyNodeExcludedFromQuorum(t *testing.T) {
	quorumNodes := []string{"n1", "n2", "n3", "n4"}
	allWithPairs := append([]string{"n5"}, quorumNodes...)

	t.Run("dispatched_without_blob", func(t *testing.T) {
		fx := newCombinedFixture("", "coordinator", allWithPairs...)
		promotes, order := combinedTestPromotes(allWithPairs...)
		cmd := combinedTestCmd(quorumNodes, 2) // n5 NOT in NodeIDs
		require.NoError(t, fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote))
		for _, r := range fx.peer.dispatchSnapshot() {
			if r.node == "n5" {
				require.True(t, r.blobNil, "pairs-only node must be dispatched without a meta blob")
				return
			}
		}
		t.Fatal("n5 was never dispatched")
	})

	t.Run("plain_error_aborts_even_with_quorum", func(t *testing.T) {
		fx := newCombinedFixture("", "coordinator", allWithPairs...)
		fx.peer.errs["n5"] = errors.New("promote transport failure")
		promotes, order := combinedTestPromotes(allWithPairs...)
		cmd := combinedTestCmd(quorumNodes, 2)
		err := fx.store.promoteAndWriteQuorumMeta(context.Background(), cmd, promotes, order, fx.legacyPromote)
		require.Error(t, err, "a promote-class failure on a pairs-only node must abort even though the meta quorum was met")
	})
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

	t.Run("capability_nil_snapshot_falls_back", func(t *testing.T) {
		fx := newCombinedFixture("", "self")
		fx.store.capabilityEvidence = func() map[string]map[string]bool { return nil }
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
