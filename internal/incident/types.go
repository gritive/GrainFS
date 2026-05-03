package incident

import "time"

type State string

const (
	StateObserved         State = "observed"
	StateDiagnosed        State = "diagnosed"
	StateActing           State = "acting"
	StateVerifying        State = "verifying"
	StateFixed            State = "fixed"
	StateIsolated         State = "isolated"
	StateBlocked          State = "blocked"
	StateNeedsHuman       State = "needs-human"
	StateProofUnavailable State = "proof-unavailable"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityDegraded Severity = "degraded"
	SeverityCritical Severity = "critical"
)

type Cause string

const (
	CauseMissingShard     Cause = "missing_shard"
	CauseCorruptShard     Cause = "corrupt_shard"
	CauseCorruptBlob      Cause = "corrupt_blob"
	CauseFDExhaustionRisk Cause = "fd_exhaustion_risk"
)

type Action string

const (
	ActionReconstructShard Action = "reconstruct_shard"
	ActionIsolateObject    Action = "isolate_object"
	ActionNoopBlocked      Action = "noop_blocked"
	ActionResourceWarning  Action = "resource_warning"
)

type ScopeKind string

const (
	ScopeShard  ScopeKind = "shard"
	ScopeObject ScopeKind = "object"
	ScopeBucket ScopeKind = "bucket"
	ScopeNode   ScopeKind = "node"
)

type Scope struct {
	Kind      ScopeKind `json:"kind"`
	Bucket    string    `json:"bucket,omitempty"`
	Key       string    `json:"key,omitempty"`
	VersionID string    `json:"version_id,omitempty"`
	ShardID   int       `json:"shard_id,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
}

type ProofStatus string

const (
	ProofSigned      ProofStatus = "signed"
	ProofMissing     ProofStatus = "missing"
	ProofNotRequired ProofStatus = "not-required"
)

type Proof struct {
	Status    ProofStatus `json:"status"`
	ReceiptID string      `json:"receipt_id,omitempty"`
	Reason    string      `json:"reason,omitempty"`
}

type IncidentState struct {
	ID          string    `json:"id"`
	State       State     `json:"state"`
	Severity    Severity  `json:"severity"`
	Cause       Cause     `json:"cause"`
	Action      Action    `json:"action,omitempty"`
	Scope       Scope     `json:"scope"`
	Decision    string    `json:"decision,omitempty"`
	Proof       Proof     `json:"proof"`
	NextAction  string    `json:"next_action"`
	ObservedAt  time.Time `json:"observed_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

type FactType string

const (
	FactObserved      FactType = "observed"
	FactDiagnosed     FactType = "diagnosed"
	FactActionStarted FactType = "action_started"
	FactActionFailed  FactType = "action_failed"
	FactVerified      FactType = "verified"
	FactReceiptSigned FactType = "receipt_signed"
	FactIsolated      FactType = "isolated"
	FactResolved      FactType = "resolved"
)

type Fact struct {
	CorrelationID string
	Type          FactType
	Cause         Cause
	Action        Action
	Scope         Scope
	ReceiptID     string
	ErrorCode     string
	Message       string
	At            time.Time
}
