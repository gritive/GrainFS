package badgerrole

import "time"

type Role string

const (
	RoleMeta          Role = "meta"
	RoleMetaRaftLog   Role = "meta_raft_log"
	RoleSharedRaftLog Role = "shared_raft_log"
	RoleSharedFSM     Role = "shared_fsm"
	RoleGroupState    Role = "group_state"
	RoleGroupRaftLog  Role = "group_raft_log"
	RoleReceipts      Role = "receipts"
	RoleVolumeCatalog Role = "volume_catalog"
	RoleIncidentState Role = "incident_state"
)

type OptionsKind string

const (
	OptionsSmall   OptionsKind = "small"
	OptionsRaftLog OptionsKind = "raft_log"
)

type Criticality string

const (
	CriticalityRequired Criticality = "required"
	CriticalityReadOnly Criticality = "read_only_allowed"
	CriticalityOptional Criticality = "optional"
)

type SourceContract string

const (
	SourceContractBadgerSelf     SourceContract = "badger_self"
	SourceContractRaftLog        SourceContract = "raft_log"
	SourceContractOperatorMarker SourceContract = "operator_marker"
	SourceContractNone           SourceContract = "none"
)

type RecoveryAction string

const (
	RecoveryActionNone            RecoveryAction = "none"
	RecoveryActionBlockStart      RecoveryAction = "block_start"
	RecoveryActionStartReadOnly   RecoveryAction = "start_read_only"
	RecoveryActionDisableFeature  RecoveryAction = "disable_feature"
	RecoveryActionNeedsQuarantine RecoveryAction = "needs_quarantine"
)

type DecisionStatus string

const (
	DecisionOK                  DecisionStatus = "ok"
	DecisionMissing             DecisionStatus = "missing"
	DecisionOpenFailed          DecisionStatus = "open_failed"
	DecisionReadOnlyProbeFailed DecisionStatus = "read_only_probe_failed"
	DecisionWritableProbeFailed DecisionStatus = "writable_probe_failed"
	DecisionQuarantineRequired  DecisionStatus = "quarantine_required"
	DecisionUnknownRole         DecisionStatus = "unknown_role"
)

type StartupMode string

const (
	StartupModeWritable StartupMode = "start_writable"
	StartupModeReadOnly StartupMode = "start_read_only"
	StartupModeBlocked  StartupMode = "block_start"
)

type RoleSpec struct {
	Role                Role
	DisplayName         string
	RelativePath        string
	OptionsKind         OptionsKind
	Criticality         Criticality
	SourceContract      SourceContract
	ReadOnlyEligible    bool
	QuarantineEligible  bool
	FeatureFlag         string
	MaxProbeConcurrency int
}

type PathContext struct {
	DataDir string
	GroupID string
}

type Decision struct {
	Role           Role
	GroupID        string
	Path           string
	Status         DecisionStatus
	Action         RecoveryAction
	Reason         string
	Err            error
	ProbeDuration  time.Duration
	OpenedReadOnly bool
}

type StartupResult struct {
	Mode             StartupMode
	Decisions        []Decision
	BlockedReasons   []string
	ReadOnlyReasons  []string
	DisabledFeatures []string
}
