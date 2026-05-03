package incident

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReducer_ReducesIncidentFamilies(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	tests := []struct {
		name       string
		facts      []Fact
		wantState  State
		wantCause  Cause
		wantAction Action
		wantProof  ProofStatus
		wantSev    Severity
		wantNext   string
	}{
		{
			name: "missing shard fixed with signed receipt",
			facts: []Fact{
				{CorrelationID: "cid-1", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", VersionID: "v1", ShardID: 0}, At: now},
				{CorrelationID: "cid-1", Type: FactDiagnosed, Message: "repairable from 2 surviving shards", At: now.Add(time.Millisecond)},
				{CorrelationID: "cid-1", Type: FactActionStarted, Action: ActionReconstructShard, At: now.Add(2 * time.Millisecond)},
				{CorrelationID: "cid-1", Type: FactVerified, Message: "object read verified", At: now.Add(3 * time.Millisecond)},
				{CorrelationID: "cid-1", Type: FactReceiptSigned, ReceiptID: "rcpt-1", At: now.Add(4 * time.Millisecond)},
			},
			wantState:  StateFixed,
			wantCause:  CauseMissingShard,
			wantAction: ActionReconstructShard,
			wantProof:  ProofSigned,
			wantSev:    SeverityInfo,
			wantNext:   "No action needed.",
		},
		{
			name: "verified repair without receipt is proof unavailable",
			facts: []Fact{
				{CorrelationID: "cid-2", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", ShardID: 0}, At: now},
				{CorrelationID: "cid-2", Type: FactActionStarted, Action: ActionReconstructShard, At: now.Add(time.Millisecond)},
				{CorrelationID: "cid-2", Type: FactVerified, At: now.Add(2 * time.Millisecond)},
			},
			wantState:  StateProofUnavailable,
			wantCause:  CauseMissingShard,
			wantAction: ActionReconstructShard,
			wantProof:  ProofMissing,
			wantSev:    SeverityWarning,
			wantNext:   "Check heal-receipt signing",
		},
		{
			name: "repair failure blocks incident",
			facts: []Fact{
				{CorrelationID: "cid-3", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", ShardID: 0}, At: now},
				{CorrelationID: "cid-3", Type: FactActionFailed, Action: ActionReconstructShard, ErrorCode: "insufficient_survivors", At: now.Add(time.Millisecond)},
			},
			wantState:  StateBlocked,
			wantCause:  CauseMissingShard,
			wantAction: ActionReconstructShard,
			wantProof:  ProofNotRequired,
			wantSev:    SeverityCritical,
			wantNext:   "Restore a peer or recover from backup",
		},
		{
			name: "corruption isolation remains degraded",
			facts: []Fact{
				{CorrelationID: "cid-4", Type: FactObserved, Cause: CauseCorruptShard, Scope: Scope{Kind: ScopeObject, Bucket: "b", Key: "bad.bin", VersionID: "v1"}, At: now},
				{CorrelationID: "cid-4", Type: FactActionStarted, Action: ActionIsolateObject, At: now.Add(time.Millisecond)},
				{CorrelationID: "cid-4", Type: FactIsolated, Action: ActionIsolateObject, At: now.Add(2 * time.Millisecond)},
			},
			wantState:  StateIsolated,
			wantCause:  CauseCorruptShard,
			wantAction: ActionIsolateObject,
			wantProof:  ProofNotRequired,
			wantSev:    SeverityDegraded,
			wantNext:   "Review the object",
		},
		{
			name: "fd warning diagnosed with operator next action",
			facts: []Fact{
				{CorrelationID: "fd-node-1", Type: FactObserved, Cause: CauseFDExhaustionRisk, Scope: Scope{Kind: ScopeNode, NodeID: "node-1"}, Message: "open FD usage is 72.4% of limit 1024", At: now},
				{CorrelationID: "fd-node-1", Type: FactDiagnosed, Cause: CauseFDExhaustionRisk, Message: "projected to reach 90% in 18m; likely contributors: socket=450,badger=80", At: now.Add(time.Millisecond)},
			},
			wantState:  StateDiagnosed,
			wantCause:  CauseFDExhaustionRisk,
			wantAction: ActionResourceWarning,
			wantProof:  ProofNotRequired,
			wantSev:    SeverityWarning,
			wantNext:   "Inspect connection growth",
		},
		{
			name: "fd critical failure blocks without repair proof",
			facts: []Fact{
				{CorrelationID: "fd-node-2", Type: FactObserved, Cause: CauseFDExhaustionRisk, Scope: Scope{Kind: ScopeNode, NodeID: "node-2"}, At: now},
				{CorrelationID: "fd-node-2", Type: FactActionFailed, Cause: CauseFDExhaustionRisk, Action: ActionResourceWarning, ErrorCode: "fd_critical", Message: "open FD usage is 94.0% of limit 1024", At: now.Add(time.Millisecond)},
			},
			wantState:  StateBlocked,
			wantCause:  CauseFDExhaustionRisk,
			wantAction: ActionResourceWarning,
			wantProof:  ProofNotRequired,
			wantSev:    SeverityCritical,
			wantNext:   "Raise LimitNOFILE",
		},
		{
			name: "fd recovery resolves as fixed without receipt",
			facts: []Fact{
				{CorrelationID: "fd-node-3", Type: FactObserved, Cause: CauseFDExhaustionRisk, Scope: Scope{Kind: ScopeNode, NodeID: "node-3"}, At: now},
				{CorrelationID: "fd-node-3", Type: FactDiagnosed, Cause: CauseFDExhaustionRisk, Message: "projected to reach 90% in 18m", At: now.Add(time.Millisecond)},
				{CorrelationID: "fd-node-3", Type: FactResolved, Cause: CauseFDExhaustionRisk, Message: "open FD usage recovered below warn threshold for 60s", At: now.Add(2 * time.Millisecond)},
			},
			wantState:  StateFixed,
			wantCause:  CauseFDExhaustionRisk,
			wantAction: ActionResourceWarning,
			wantProof:  ProofNotRequired,
			wantSev:    SeverityInfo,
			wantNext:   "No action needed.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := NewReducer().Reduce(tt.facts)
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, state.State)
			assert.Equal(t, tt.wantCause, state.Cause)
			assert.Equal(t, tt.wantAction, state.Action)
			assert.Equal(t, tt.wantProof, state.Proof.Status)
			assert.Equal(t, tt.wantSev, state.Severity)
			assert.Contains(t, state.NextAction, tt.wantNext)
		})
	}
}
