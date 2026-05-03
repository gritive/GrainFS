package incident

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReducer_MissingShardFixedWithReceipt(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	r := NewReducer()
	state, err := r.Reduce([]Fact{
		{CorrelationID: "cid-1", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", VersionID: "v1", ShardID: 0}, At: now},
		{CorrelationID: "cid-1", Type: FactDiagnosed, Message: "repairable from 2 surviving shards", At: now.Add(time.Millisecond)},
		{CorrelationID: "cid-1", Type: FactActionStarted, Action: ActionReconstructShard, At: now.Add(2 * time.Millisecond)},
		{CorrelationID: "cid-1", Type: FactVerified, Message: "object read verified", At: now.Add(3 * time.Millisecond)},
		{CorrelationID: "cid-1", Type: FactReceiptSigned, ReceiptID: "rcpt-1", At: now.Add(4 * time.Millisecond)},
	})
	require.NoError(t, err)
	assert.Equal(t, "cid-1", state.ID)
	assert.Equal(t, StateFixed, state.State)
	assert.Equal(t, ProofSigned, state.Proof.Status)
	assert.Equal(t, "rcpt-1", state.Proof.ReceiptID)
	assert.Equal(t, "No action needed.", state.NextAction)
}

func TestReducer_VerifiedWithoutReceiptIsProofUnavailable(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	r := NewReducer()
	state, err := r.Reduce([]Fact{
		{CorrelationID: "cid-2", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", ShardID: 0}, At: now},
		{CorrelationID: "cid-2", Type: FactActionStarted, Action: ActionReconstructShard, At: now.Add(time.Millisecond)},
		{CorrelationID: "cid-2", Type: FactVerified, At: now.Add(2 * time.Millisecond)},
	})
	require.NoError(t, err)
	assert.Equal(t, StateProofUnavailable, state.State)
	assert.Equal(t, ProofMissing, state.Proof.Status)
	assert.Contains(t, state.NextAction, "Check heal-receipt signing")
}

func TestReducer_RepairFailureBlocksIncident(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	r := NewReducer()
	state, err := r.Reduce([]Fact{
		{CorrelationID: "cid-3", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", ShardID: 0}, At: now},
		{CorrelationID: "cid-3", Type: FactActionFailed, Action: ActionReconstructShard, ErrorCode: "insufficient_survivors", At: now.Add(time.Millisecond)},
	})
	require.NoError(t, err)
	assert.Equal(t, StateBlocked, state.State)
	assert.Equal(t, ProofNotRequired, state.Proof.Status)
	assert.Contains(t, state.NextAction, "Restore a peer or recover from backup")
}

func TestReducer_CorruptionIsolation(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	r := NewReducer()
	state, err := r.Reduce([]Fact{
		{CorrelationID: "cid-4", Type: FactObserved, Cause: CauseCorruptShard, Scope: Scope{Kind: ScopeObject, Bucket: "b", Key: "bad.bin", VersionID: "v1"}, At: now},
		{CorrelationID: "cid-4", Type: FactActionStarted, Action: ActionIsolateObject, At: now.Add(time.Millisecond)},
		{CorrelationID: "cid-4", Type: FactIsolated, Action: ActionIsolateObject, At: now.Add(2 * time.Millisecond)},
	})
	require.NoError(t, err)
	assert.Equal(t, StateIsolated, state.State)
	assert.Equal(t, CauseCorruptShard, state.Cause)
	assert.Contains(t, state.NextAction, "Review the object")
}
