package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// MetaCmdType aliases the FlatBuffers-generated type for use within this package.
type MetaCmdType = clusterpb.MetaCmdType

const (
	MetaCmdTypeNoOp                         = clusterpb.MetaCmdTypeNoOp
	MetaCmdTypeAddNode                      = clusterpb.MetaCmdTypeAddNode
	MetaCmdTypeRemoveNode                   = clusterpb.MetaCmdTypeRemoveNode
	MetaCmdTypePutShardGroup                = clusterpb.MetaCmdTypePutShardGroup        // PR-C
	MetaCmdTypePutBucketAssignment          = clusterpb.MetaCmdTypePutBucketAssignment  // PR-D
	MetaCmdTypeSetLoadSnapshot              = clusterpb.MetaCmdTypeSetLoadSnapshot      // PR-D
	MetaCmdTypeProposeRebalancePlan         = clusterpb.MetaCmdTypeProposeRebalancePlan // PR-D
	MetaCmdTypeAbortPlan                    = clusterpb.MetaCmdTypeAbortPlan            // PR-D
	MetaCmdTypeIcebergCreateNamespace       = clusterpb.MetaCmdTypeIcebergCreateNamespace
	MetaCmdTypeIcebergDeleteNamespace       = clusterpb.MetaCmdTypeIcebergDeleteNamespace
	MetaCmdTypeIcebergCreateTable           = clusterpb.MetaCmdTypeIcebergCreateTable
	MetaCmdTypeIcebergCommitTable           = clusterpb.MetaCmdTypeIcebergCommitTable
	MetaCmdTypeIcebergDeleteTable           = clusterpb.MetaCmdTypeIcebergDeleteTable
	MetaCmdTypeRotateKeyBegin               = clusterpb.MetaCmdTypeRotateKeyBegin
	MetaCmdTypeRotateKeySwitch              = clusterpb.MetaCmdTypeRotateKeySwitch
	MetaCmdTypeRotateKeyDrop                = clusterpb.MetaCmdTypeRotateKeyDrop
	MetaCmdTypeRotateKeyAbort               = clusterpb.MetaCmdTypeRotateKeyAbort
	MetaCmdTypeScrubTrigger                 = clusterpb.MetaCmdTypeScrubTrigger // PR4
	MetaCmdTypePutObjectIndex               = clusterpb.MetaCmdTypePutObjectIndex
	MetaCmdTypeDeleteObjectIndex            = clusterpb.MetaCmdTypeDeleteObjectIndex
	MetaCmdTypeIAMSACreate                  = clusterpb.MetaCmdTypeIAMSACreate
	MetaCmdTypeIAMSADelete                  = clusterpb.MetaCmdTypeIAMSADelete
	MetaCmdTypeIAMKeyCreate                 = clusterpb.MetaCmdTypeIAMKeyCreate
	MetaCmdTypeIAMKeyCreateScoped           = clusterpb.MetaCmdTypeIAMKeyCreateScoped
	MetaCmdTypeIAMKeyRevoke                 = clusterpb.MetaCmdTypeIAMKeyRevoke
	MetaCmdTypeIAMGrantPut                  = clusterpb.MetaCmdTypeIAMGrantPut
	MetaCmdTypeIAMGrantDelete               = clusterpb.MetaCmdTypeIAMGrantDelete
	MetaCmdTypeIAMGrantWildcardPut          = clusterpb.MetaCmdTypeIAMGrantWildcardPut
	MetaCmdTypeIAMGrantWildcardDelete       = clusterpb.MetaCmdTypeIAMGrantWildcardDelete
	MetaCmdTypeIAMInitFirstSA               = clusterpb.MetaCmdTypeIAMInitFirstSA
	MetaCmdTypeIAMBucketUpstreamPut         = clusterpb.MetaCmdTypeIAMBucketUpstreamPut
	MetaCmdTypeIAMBucketUpstreamDelete      = clusterpb.MetaCmdTypeIAMBucketUpstreamDelete
	MetaCmdTypeBucketLifecyclePut           = clusterpb.MetaCmdTypeBucketLifecyclePut
	MetaCmdTypeBucketLifecycleDelete        = clusterpb.MetaCmdTypeBucketLifecycleDelete
	MetaCmdTypeNfsExportUpsert              = clusterpb.MetaCmdTypeNfsExportUpsert
	MetaCmdTypeNfsExportDelete              = clusterpb.MetaCmdTypeNfsExportDelete
	MetaCmdTypeNfsExportBucketDeleteCascade = clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade
	MetaCmdTypeNfsExportCreate              = clusterpb.MetaCmdTypeNfsExportCreate
	MetaCmdTypeCapabilityActivate           = clusterpb.MetaCmdTypeCapabilityActivate
	MetaCmdTypeMigrationCutover             = clusterpb.MetaCmdTypeMigrationCutover
	MetaCmdTypeConfigPut                    = clusterpb.MetaCmdTypeConfigPut
	MetaCmdTypeConfigDelete                 = clusterpb.MetaCmdTypeConfigDelete
	MetaCmdTypeDEKRotate                    = clusterpb.MetaCmdTypeDEKRotate
	MetaCmdTypeDEKVersionPrune              = clusterpb.MetaCmdTypeDEKVersionPrune
	MetaCmdTypePolicyPut                    = clusterpb.MetaCmdTypePolicyPut
	MetaCmdTypePolicyDelete                 = clusterpb.MetaCmdTypePolicyDelete
	MetaCmdTypeGroupPut                     = clusterpb.MetaCmdTypeGroupPut
	MetaCmdTypeGroupDelete                  = clusterpb.MetaCmdTypeGroupDelete
	MetaCmdTypeGroupMemberPut               = clusterpb.MetaCmdTypeGroupMemberPut
	MetaCmdTypeGroupMemberDelete            = clusterpb.MetaCmdTypeGroupMemberDelete
	MetaCmdTypePolicyAttachToSAPut          = clusterpb.MetaCmdTypePolicyAttachToSAPut
	MetaCmdTypePolicyAttachToSADelete       = clusterpb.MetaCmdTypePolicyAttachToSADelete
	MetaCmdTypePolicyAttachToGroupPut       = clusterpb.MetaCmdTypePolicyAttachToGroupPut
	MetaCmdTypePolicyAttachToGroupDelete    = clusterpb.MetaCmdTypePolicyAttachToGroupDelete
	MetaCmdTypeBucketPolicyPut              = clusterpb.MetaCmdTypeBucketPolicyPut
	MetaCmdTypeBucketPolicyDelete           = clusterpb.MetaCmdTypeBucketPolicyDelete
	MetaCmdTypeCreateBucketWithPolicyAttach = clusterpb.MetaCmdTypeCreateBucketWithPolicyAttach
	MetaCmdTypeJWTSigningKeyRotate          = clusterpb.MetaCmdTypeJWTSigningKeyRotate
	MetaCmdTypeJWTSigningKeyPrune           = clusterpb.MetaCmdTypeJWTSigningKeyPrune
	MetaCmdTypeMountSACreate                = clusterpb.MetaCmdTypeMountSACreate
	MetaCmdTypeMountSADelete                = clusterpb.MetaCmdTypeMountSADelete
	MetaCmdTypeMountSAAttachPolicy          = clusterpb.MetaCmdTypeMountSAAttachPolicy
	MetaCmdTypeMountSADetachPolicy          = clusterpb.MetaCmdTypeMountSADetachPolicy
)

// MetaNodeEntry is the plain-Go representation of a cluster member.
type MetaNodeEntry struct {
	ID      string
	Address string
	Role    uint8 // 0=Voter 1=Learner
}

// ShardGroupEntry describes data Raft group membership.
// bucket→group mapping is managed separately by Router. key-range sharding excluded.
type ShardGroupEntry struct {
	ID      string
	PeerIDs []string
}

// LoadStatEntry is the plain-Go representation of per-node load statistics.
type LoadStatEntry struct {
	NodeID         string
	DiskUsedPct    float64
	DiskAvailBytes uint64
	RequestsPerSec float64
	UpdatedAt      time.Time
}

// ObjectIndexEntry is the meta-Raft global object index row used to route an
// object version to its owning data Raft group before touching group-local FSMs.
type ObjectIndexEntry struct {
	Bucket           string
	Key              string
	VersionID        string
	PlacementGroupID string
	Size             int64
	ContentType      string
	ETag             string
	ModTime          int64
	ECData           uint8
	ECParity         uint8
	NodeIDs          []string
	IsDeleteMarker   bool
	// Parts is non-empty only for CompleteMultipartUpload objects. The S3
	// GetObject/HeadObject ?partNumber=N path uses this to translate part
	// numbers into byte ranges over the assembled body.
	Parts []storage.MultipartPartEntry
	// DekGen is the DEK generation that sealed this object's blobs.
	// 0 means the legacy (pre-Task-12) key. FlatBuffer default is 0 so
	// pre-Task-12 records automatically decode as generation 0.
	DekGen uint32
}

type ObjectIndexSummary struct {
	Bucket               string         `json:"bucket,omitempty"`
	PlacementGroupCounts map[string]int `json:"placement_group_counts"`
}

// RebalancePlan describes a single voter migration between data Raft group nodes.
type RebalancePlan struct {
	PlanID    string
	GroupID   string
	FromNode  string
	ToNode    string
	CreatedAt time.Time
}

type IcebergNamespaceEntry struct {
	Warehouse  string
	Namespace  []string
	Properties map[string]string
}

type IcebergTableEntry struct {
	Warehouse        string
	Identifier       icebergcatalog.Identifier
	MetadataLocation string
	Properties       map[string]string
}

type IcebergCreateNamespaceCmd struct {
	RequestID  string
	Warehouse  string
	Namespace  []string
	Properties map[string]string
}

type IcebergDeleteNamespaceCmd struct {
	RequestID string
	Warehouse string
	Namespace []string
}

type IcebergCreateTableCmd struct {
	RequestID        string
	Warehouse        string
	Identifier       icebergcatalog.Identifier
	MetadataLocation string
	Properties       map[string]string
}

type IcebergCommitTableCmd struct {
	RequestID                string
	Warehouse                string
	Identifier               icebergcatalog.Identifier
	ExpectedMetadataLocation string
	NewMetadataLocation      string
}

type IcebergDeleteTableCmd struct {
	RequestID  string
	Warehouse  string
	Identifier icebergcatalog.Identifier
}

// MetaFSM implements raft.Snapshotter for the meta-Raft group.
// It holds cluster membership state.
//
// Lock discipline: mu is a RWMutex shared by all three state maps (nodes,
// shardGroups, bucketAssignments) and the callback field.
// RWMutex is justified here because:
//   - There is exactly ONE writer goroutine (runApplyLoop), so write contention
//     is zero and write locks are never contended.
//   - Multiple reader goroutines (HTTP handlers, routing) hold RLock concurrently
//     while the writer is idle — RWMutex allows this, plain Mutex would not.
//   - Snapshot() and Restore() must read/write all three maps atomically; a
//     single lock (rather than per-map atomics) is the simplest consistency guarantee.
//   - onBucketAssigned is stored in the same lock to ensure the callback always
//     sees the freshly updated map state without a separate atomic.
type MetaFSM struct {
	mu                sync.RWMutex
	nodes             map[string]MetaNodeEntry
	shardGroups       map[string]ShardGroupEntry // key = group ID
	bucketAssignments map[string]string          // bucket → group_id (PR-D)
	objectIndex       map[string]ObjectIndexEntry
	objectLatest      map[string]string
	loadSnapshot      map[string]LoadStatEntry                    // node_id → stats (PR-D)
	activePlan        *RebalancePlan                              // nil = no active plan (PR-D)
	icebergNamespaces map[string]map[string]IcebergNamespaceEntry // warehouse → nsKey → entry
	icebergTables     map[string]map[string]IcebergTableEntry     // warehouse → tableKey → entry
	onBucketAssigned  func(string, string)                        // protected by mu; set before Start() (PR-D)
	onRebalancePlan   func(*RebalancePlan)                        // must not block; set before Start() (PR-D)
	onShardGroupAdded func(ShardGroupEntry)                       // fired after PutShardGroup applies; protected by mu (v0.0.7.0)
	onIcebergResult   func(string, error)                         // requestID, typed catalog result; must not block
	onScrubTrigger    func(scrubber.ScrubTriggerEntry)            // PR4: cluster-wide scrub trigger applied; must not block
	onNfsExportChange func()                                      // fired after NFS export registry apply; must not block

	// 클러스터 키 회전 — 결정론적 FSM은 여기, side-effect (디스크 I/O,
	// transport identity swap)는 onRotationApplied 콜백으로 분리 (D16).
	rotation          *RotationFSM
	onRotationApplied func(RotationState) // 매 phase 변경 commit 후 호출; nil 이면 no-op

	// IAM sub-FSM — wired after construction via SetIAM (Phase 1). iamStore is
	// always non-nil (default empty); iamApplier is nil until SetIAM is called.
	iamStore   *iam.Store
	iamApplier *iam.Applier

	// lifecycleStore is wired via SetLifecycle. nil = lifecycle commands return
	// an error (not configured).
	lifecycleStore *lifecycle.Store

	// exportStore is wired via SetExportStore. nil = NFS export commands return
	// an error (not configured).
	exportStore     *nfsexport.Store
	exportFsidMajor uint64

	// migrationStore is wired via SetMigration. nil = migration commands return
	// an error (not configured).
	migrationStore *migration.JobStore
	activeFeatures compat.ActiveFeatures

	// clusterCfg holds the cluster-wide policy snapshot. Initialised to an
	// empty ClusterConfig (defaults) in NewMetaFSM; mutated only from the FSM
	// apply goroutine via applyClusterConfigPatch / Restore. Consumers read
	// lock-free via atomic.Pointer.
	clusterCfg *ClusterConfig

	// encryptor is used to gate cluster-config patches that carry a wrapped
	// alert-webhook secret. nil means the encryptor has not been wired; such
	// patches are rejected at apply time (see applyClusterConfigPatch). Wired
	// via SetEncryptor before the raft log starts replaying.
	encryptor *encrypt.Encryptor

	// policyStore is the IAM policy document store. nil until SetPolicyStore is
	// called; PolicyPut/PolicyDelete commands are safe no-ops when nil.
	policyStore *policystore.InMemoryStore

	// policyResolver is the IAM effective-policy resolver. nil until
	// SetPolicyResolver is called. When non-nil its cache is invalidated on
	// every PolicyPut/PolicyDelete apply.
	policyResolver *policy.Resolver

	// groupStore is the IAM group store. nil until SetGroupStore is called;
	// Group* commands are safe no-ops when nil.
	groupStore *group.InMemoryStore

	// policyAttachStore is the SA/group/MountSA→policy attachment store. nil until
	// SetPolicyAttachStore is called; PolicyAttach* commands are safe no-ops when nil.
	policyAttachStore *policyattach.InMemoryStore

	// bucketPolicyStore is the per-bucket policy document store. nil until
	// SetBucketPolicyStore is called; BucketPolicy* commands are safe no-ops when nil.
	bucketPolicyStore *bucketpolicy.InMemoryStore

	// mountSAStore is the NFS/9P mount service account store backed by Badger.
	// nil until SetMountSAStore is called; MountSA* commands return an error when nil.
	mountSAStore *mountsastore.Store

	// cfgStore is the cluster-wide config registry. nil until SetConfigStore is
	// called; ConfigPut/ConfigDelete commands are safe no-ops when nil.
	cfgStore *config.Store

	// dekKeeper holds the in-memory DEK generation table. nil until SetDEKKeeper
	// is called; DEKRotate/DEKVersionPrune commands are safe no-ops when nil.
	dekKeeper *encrypt.DEKKeeper

	// dekRefCounts holds the per-generation reference count: how many
	// ObjectIndexEntry records reference each DEK generation. Incremented on
	// applyPutObjectIndex, decremented on applyDeleteObjectIndex. Persisted
	// in the DKVS snapshot trailer alongside DEK versions (Task 12).
	dekRefCounts map[uint32]uint64

	// pendingDEKVersions and pendingDEKActive hold the DEK snapshot state decoded
	// during Restore. The runtime uses PendingDEKVersions() after Restore to wire
	// a new DEKKeeper via LoadFromFSM(kek, versions).
	pendingDEKVersions map[uint32][]byte
	pendingDEKActive   uint32

	// jwtKeyStore holds the wrapped JWT signing key seeds persisted in the JKEY
	// snapshot trailer. Always non-nil after NewMetaFSM.
	jwtKeyStore *JWTKeyStore

	// jwtKeys is the in-process JWT KeySet reconstructed from jwtKeyStore seeds
	// on Restore. SetJWTKeySet allows serveruntime to swap in its own instance.
	jwtKeys *iamjwt.KeySet

	// postCommitHooks is an atomic pointer to the slice of post-commit hooks.
	// Read on every apply via a single atomic load (no mutex). Register path
	// uses copy-on-write CAS; see post_commit.go for the contract.
	postCommitHooks postCommitHooksField
}

func NewMetaFSM() *MetaFSM {
	return &MetaFSM{
		nodes:             make(map[string]MetaNodeEntry),
		shardGroups:       make(map[string]ShardGroupEntry),
		bucketAssignments: make(map[string]string),
		objectIndex:       make(map[string]ObjectIndexEntry),
		objectLatest:      make(map[string]string),
		loadSnapshot:      make(map[string]LoadStatEntry),
		icebergNamespaces: make(map[string]map[string]IcebergNamespaceEntry),
		icebergTables:     make(map[string]map[string]IcebergTableEntry),
		rotation:          NewRotationFSM(),
		iamStore:          iam.NewStore(),
		activeFeatures:    compat.NewActiveFeatures(),
		clusterCfg:        NewClusterConfig(),
		dekRefCounts:      make(map[uint32]uint64),
		jwtKeyStore:       NewJWTKeyStore(),
		jwtKeys:           iamjwt.NewKeySet(),
	}
}

// ClusterConfig returns the cluster-wide policy snapshot. Read-only; consumers
// call its getters at use-time. Safe for concurrent reads.
func (f *MetaFSM) ClusterConfig() *ClusterConfig { return f.clusterCfg }

// SetIAM wires the IAM Applier into the MetaFSM. Must be called before the
// raft log starts replaying; set alongside the Encryptor used to decrypt
// secret_key_enc payloads. iamApplier nil = IAM commands return "not configured".
func (f *MetaFSM) SetIAM(store *iam.Store, applier *iam.Applier) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.iamStore = store
	f.iamApplier = applier
}

// IAMStore returns the IAM Store for read access (auth checks, bootstrap shim).
func (f *MetaFSM) IAMStore() *iam.Store { return f.iamStore }

// SetConfigStore wires the cluster-wide config registry into the MetaFSM.
// Must be called before the raft log starts replaying. nil means
// ConfigPut/ConfigDelete commands are safe no-ops (not configured yet).
func (f *MetaFSM) SetConfigStore(s *config.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgStore = s
}

// SetPolicyStore wires the IAM policy store into the MetaFSM. Must be called
// before the raft log starts replaying. nil means PolicyPut/PolicyDelete
// commands are safe no-ops (not configured yet).
func (f *MetaFSM) SetPolicyStore(s *policystore.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policyStore = s
}

// SetPolicyResolver wires the effective-policy resolver into the MetaFSM.
// When non-nil, its cache is invalidated on every PolicyPut/PolicyDelete apply.
// nil is safe (no-op invalidation).
func (f *MetaFSM) SetPolicyResolver(r *policy.Resolver) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policyResolver = r
}

// SetGroupStore wires the IAM group store into the MetaFSM. Must be called
// before the raft log starts replaying. nil means Group* commands are safe
// no-ops (not configured yet).
func (f *MetaFSM) SetGroupStore(s *group.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groupStore = s
}

// SetPolicyAttachStore wires the SA/group→policy attachment store into the
// MetaFSM. Must be called before the raft log starts replaying. nil means
// PolicyAttach* commands are safe no-ops.
func (f *MetaFSM) SetPolicyAttachStore(s *policyattach.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policyAttachStore = s
}

// SetBucketPolicyStore wires the per-bucket policy document store into the
// MetaFSM. Must be called before the raft log starts replaying. nil means
// BucketPolicy* commands are safe no-ops.
func (f *MetaFSM) SetBucketPolicyStore(s *bucketpolicy.InMemoryStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bucketPolicyStore = s
}

// SetMountSAStore wires the NFS/9P mount service account store into the
// MetaFSM. Must be called before the raft log starts replaying. nil means
// MountSA* commands return an error.
func (f *MetaFSM) SetMountSAStore(s *mountsastore.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mountSAStore = s
}

// SetMigration wires the migration job store into the MetaFSM. Must be called
// before raft Start so apply does not race with replay.
func (f *MetaFSM) SetMigration(store *migration.JobStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.migrationStore = store
}

// applyCmd decodes a MetaCmd FlatBuffers envelope, mutates state, and fires
// post-commit hooks on success. Called by MetaRaft.runApplyLoop on each
// committed log entry.
func (f *MetaFSM) applyCmd(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: empty command")
	}
	var (
		cmd    *clusterpb.MetaCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaCmd flatbuffer: %v", r)
			}
		}()
		cmd = clusterpb.GetRootAsMetaCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	err := f.applyCmdInner(cmd)
	if err == nil {
		f.firePostCommitHooks(cmd.Type(), cmd.DataBytes())
	}
	return err
}

// applyCmdInner dispatches a decoded MetaCmd to the appropriate apply method.
// Returns an error on failure; the caller (applyCmd) fires post-commit hooks
// only on nil return.
func (f *MetaFSM) applyCmdInner(cmd *clusterpb.MetaCmd) error {
	switch cmd.Type() {
	case clusterpb.MetaCmdTypeNoOp:
		return nil
	case clusterpb.MetaCmdTypeAddNode:
		return f.applyAddNode(cmd.DataBytes())
	case clusterpb.MetaCmdTypeRemoveNode:
		return f.applyRemoveNode(cmd.DataBytes())
	case clusterpb.MetaCmdTypePutShardGroup:
		return f.applyPutShardGroup(cmd.DataBytes())
	case clusterpb.MetaCmdTypePutBucketAssignment:
		return f.applyPutBucketAssignment(cmd.DataBytes())
	case clusterpb.MetaCmdTypePutObjectIndex:
		return f.applyPutObjectIndex(cmd.DataBytes())
	case clusterpb.MetaCmdTypeDeleteObjectIndex:
		return f.applyDeleteObjectIndex(cmd.DataBytes())
	case clusterpb.MetaCmdTypeSetLoadSnapshot:
		return f.applySetLoadSnapshot(cmd.DataBytes())
	case clusterpb.MetaCmdTypeProposeRebalancePlan:
		return f.applyProposeRebalancePlan(cmd.DataBytes())
	case clusterpb.MetaCmdTypeAbortPlan:
		return f.applyAbortPlan(cmd.DataBytes())
	case clusterpb.MetaCmdTypeIcebergCreateNamespace:
		return f.applyIcebergCreateNamespace(cmd.DataBytes())
	case clusterpb.MetaCmdTypeIcebergDeleteNamespace:
		return f.applyIcebergDeleteNamespace(cmd.DataBytes())
	case clusterpb.MetaCmdTypeIcebergCreateTable:
		return f.applyIcebergCreateTable(cmd.DataBytes())
	case clusterpb.MetaCmdTypeIcebergCommitTable:
		return f.applyIcebergCommitTable(cmd.DataBytes())
	case clusterpb.MetaCmdTypeIcebergDeleteTable:
		return f.applyIcebergDeleteTable(cmd.DataBytes())
	case clusterpb.MetaCmdTypeRotateKeyBegin:
		return f.applyRotateKeyBegin(cmd.DataBytes())
	case clusterpb.MetaCmdTypeRotateKeySwitch:
		return f.applyRotateKeySwitch(cmd.DataBytes())
	case clusterpb.MetaCmdTypeRotateKeyDrop:
		return f.applyRotateKeyDrop(cmd.DataBytes())
	case clusterpb.MetaCmdTypeRotateKeyAbort:
		return f.applyRotateKeyAbort(cmd.DataBytes())
	case clusterpb.MetaCmdTypeScrubTrigger:
		return f.applyScrubTrigger(cmd.DataBytes())
	case clusterpb.MetaCmdTypeIAMSACreate:
		if f.iamApplier == nil {
			return fmt.Errorf("meta_fsm: IAM applier not configured")
		}
		wasEmpty := f.iamStore.IsEmpty()
		if err := f.iamApplier.ApplySACreate(cmd.DataBytes()); err != nil {
			return err
		}
		// D#3 + F#16: first SA create atomically flips iam.anon-enabled → false.
		// Subsequent SA creates leave the flag untouched so an operator who
		// re-enables anon stays in control. ApplySACreate is idempotent on
		// duplicate sa_id (returns nil without inserting), so we guard the
		// store is actually non-empty after the call.
		if wasEmpty && f.cfgStore != nil && !f.iamStore.IsEmpty() {
			if err := f.cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
				log.Warn().Err(err).Msg("meta_fsm: failed to flip iam.anon-enabled on first SA create")
				// Don't fail the apply — the SA is committed. Operator can re-set manually.
			} else if f.policyResolver != nil {
				f.policyResolver.Invalidate(nil, nil)
			}
		}
		return nil
	case clusterpb.MetaCmdTypeIAMSADelete:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplySADelete)
	case clusterpb.MetaCmdTypeIAMKeyCreate:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyKeyCreate)
	case clusterpb.MetaCmdTypeIAMKeyCreateScoped:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyKeyCreateScoped)
	case clusterpb.MetaCmdTypeIAMKeyRevoke:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyKeyRevoke)
	// IAMGrantPut/Delete/WildcardPut/WildcardDelete/InitFirstSA (enum 25-31) are
	// retained in cluster.fbs for backcompat with pre-§2 snapshots but no longer
	// have apply branches. Legacy cmd types fall through to default (skip).
	case clusterpb.MetaCmdTypeIAMBucketUpstreamPut:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyBucketUpstreamPut)
	case clusterpb.MetaCmdTypeIAMBucketUpstreamDelete:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyBucketUpstreamDelete)
	case clusterpb.MetaCmdTypeBucketLifecyclePut:
		return f.applyBucketLifecyclePut(cmd.DataBytes())
	case clusterpb.MetaCmdTypeBucketLifecycleDelete:
		return f.applyBucketLifecycleDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeNfsExportUpsert:
		return f.applyNfsExportUpsert(cmd.DataBytes())
	case clusterpb.MetaCmdTypeNfsExportCreate:
		return f.applyNfsExportCreate(cmd.DataBytes())
	case clusterpb.MetaCmdTypeNfsExportDelete:
		return f.applyNfsExportDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade:
		return f.applyNfsExportBucketDeleteCascade(cmd.DataBytes())
	case clusterpb.MetaCmdTypeClusterConfigPatch:
		return f.applyClusterConfigPatch(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMigrationJobStart:
		return f.applyMigrationJobStart(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMigrationJobDone:
		return f.applyMigrationJobDone(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMigrationJobFailed:
		return f.applyMigrationJobFailed(cmd.DataBytes())
	case clusterpb.MetaCmdTypeCapabilityActivate:
		return f.applyCapabilityActivate(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMigrationCutover:
		return f.applyMigrationCutover(cmd.DataBytes())
	case clusterpb.MetaCmdTypeConfigPut:
		return f.applyConfigPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypeConfigDelete:
		return f.applyConfigDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypePolicyPut:
		return f.applyPolicyPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypePolicyDelete:
		return f.applyPolicyDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeGroupPut:
		return f.applyGroupPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypeGroupDelete:
		return f.applyGroupDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeGroupMemberPut:
		return f.applyGroupMemberPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypeGroupMemberDelete:
		return f.applyGroupMemberDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypePolicyAttachToSAPut:
		return f.applyPolicyAttachToSAPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypePolicyAttachToSADelete:
		return f.applyPolicyAttachToSADelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypePolicyAttachToGroupPut:
		return f.applyPolicyAttachToGroupPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypePolicyAttachToGroupDelete:
		return f.applyPolicyAttachToGroupDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeBucketPolicyPut:
		return f.applyBucketPolicyPut(cmd.DataBytes())
	case clusterpb.MetaCmdTypeBucketPolicyDelete:
		return f.applyBucketPolicyDelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMountSACreate:
		return f.applyMountSACreate(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMountSADelete:
		return f.applyMountSADelete(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMountSAAttachPolicy:
		return f.applyMountSAAttachPolicy(cmd.DataBytes())
	case clusterpb.MetaCmdTypeMountSADetachPolicy:
		return f.applyMountSADetachPolicy(cmd.DataBytes())
	case clusterpb.MetaCmdTypeCreateBucketWithPolicyAttach:
		return f.applyCreateBucketWithPolicyAttach(cmd.DataBytes())
	case clusterpb.MetaCmdTypeDEKRotate:
		return f.applyDEKRotate()
	case clusterpb.MetaCmdTypeDEKVersionPrune:
		return f.applyDEKVersionPrune(cmd.DataBytes())
	case clusterpb.MetaCmdTypeJWTSigningKeyRotate:
		return f.applyJWTSigningKeyRotate(cmd.DataBytes())
	case clusterpb.MetaCmdTypeJWTSigningKeyPrune:
		return f.applyJWTSigningKeyPrune(cmd.DataBytes())
	default:
		metrics.UnknownMetaCmdTotal.WithLabelValues(strconv.Itoa(int(cmd.Type()))).Inc()
		log.Warn().Stringer("type", cmd.Type()).Msg("meta_fsm: unknown command type, ignoring")
		return nil
	}
}

// SetOnShardGroupAdded registers a callback fired after each PutShardGroup is applied.
// The callback fires on every applied entry (including idempotent overwrites with
// identical PeerIDs) — caller must dedupe if needed.
// Must not block. Set before Start() to avoid races with the apply loop.
func (f *MetaFSM) SetOnShardGroupAdded(fn func(ShardGroupEntry)) {
	f.mu.Lock()
	f.onShardGroupAdded = fn
	f.mu.Unlock()
}

// SetOnBucketAssigned registers a callback fired after each PutBucketAssignment is applied.
// Must be called before MetaRaft.Start() to avoid a data race with the apply loop.
func (f *MetaFSM) SetOnBucketAssigned(fn func(bucket, groupID string)) {
	f.mu.Lock()
	f.onBucketAssigned = fn
	f.mu.Unlock()
}

func (f *MetaFSM) SetOnIcebergApplyResult(fn func(requestID string, err error)) {
	f.mu.Lock()
	f.onIcebergResult = fn
	f.mu.Unlock()
}

func (f *MetaFSM) publishIcebergResult(requestID string, err error) {
	f.mu.RLock()
	cb := f.onIcebergResult
	f.mu.RUnlock()
	if cb != nil && requestID != "" {
		cb(requestID, err)
	}
}

// SetOnScrubTrigger registers a callback fired when a non-stale
// MetaScrubTriggerCmd is applied. Stale entries (requested_at > scrubTriggerMaxAge
// ago) are skipped with a log line so fresh nodes joining via raft replay do
// not re-run ancient scrubs. Must not block — the callback runs on the apply
// loop.
func (f *MetaFSM) SetOnScrubTrigger(fn func(scrubber.ScrubTriggerEntry)) {
	f.mu.Lock()
	f.onScrubTrigger = fn
	f.mu.Unlock()
}

// scrubTriggerMaxAge bounds how old a replayed trigger may be before it's
// silently dropped. Triggers within this window apply normally on every node.
const scrubTriggerMaxAge = 1 * time.Hour

func (f *MetaFSM) applyScrubTrigger(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: applyScrubTrigger: empty payload")
	}
	var (
		c      *clusterpb.MetaScrubTriggerCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaScrubTriggerCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaScrubTriggerCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}
	if c == nil {
		return fmt.Errorf("meta_fsm: applyScrubTrigger: nil cmd")
	}
	requestedAt := time.Unix(c.RequestedAt(), 0)
	if time.Since(requestedAt) > scrubTriggerMaxAge {
		log.Warn().
			Str("session_id", string(c.SessionId())).
			Str("bucket", string(c.Bucket())).
			Time("requested_at", requestedAt).
			Msg("meta_fsm: applyScrubTrigger skipping stale entry (>1h old)")
		return nil
	}
	f.mu.RLock()
	cb := f.onScrubTrigger
	f.mu.RUnlock()
	if cb == nil {
		return nil
	}
	cb(scrubber.ScrubTriggerEntry{
		SessionID:        string(c.SessionId()),
		Bucket:           string(c.Bucket()),
		KeyPrefix:        string(c.KeyPrefix()),
		Scope:            scrubber.ScrubScope(c.Scope()),
		DryRun:           c.DryRun(),
		RequestedAt:      c.RequestedAt(),
		OriginatorNodeID: string(c.OriginatorNodeId()),
	})
	return nil
}

// Snapshot serializes current state as FlatBuffers MetaStateSnapshot.
func (f *MetaFSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	nodes := make([]MetaNodeEntry, 0, len(f.nodes))
	for _, n := range f.nodes {
		nodes = append(nodes, n)
	}
	shardGroups := make([]ShardGroupEntry, 0, len(f.shardGroups))
	for _, sg := range f.shardGroups {
		shardGroups = append(shardGroups, sg)
	}
	type bucketKV struct{ bucket, groupID string }
	buckets := make([]bucketKV, 0, len(f.bucketAssignments))
	for bucket, groupID := range f.bucketAssignments {
		buckets = append(buckets, bucketKV{bucket, groupID})
	}
	objectEntries := make([]objectIndexSnapshotEntry, 0, len(f.objectIndex))
	for versionKey, entry := range f.objectIndex {
		latestVersionID := f.objectLatest[objectIndexLatestKey(entry.Bucket, entry.Key)]
		objectEntries = append(objectEntries, objectIndexSnapshotEntry{
			ObjectIndexEntry: cloneObjectIndexEntry(entry),
			IsLatest:         latestVersionID == entry.VersionID,
			sortKey:          versionKey,
		})
	}
	lsEntries := make([]LoadStatEntry, 0, len(f.loadSnapshot))
	for _, v := range f.loadSnapshot {
		lsEntries = append(lsEntries, v)
	}
	var activePlanCopy *RebalancePlan
	if f.activePlan != nil {
		cp := *f.activePlan
		activePlanCopy = &cp
	}
	var icebergNamespacesCount int
	for _, m := range f.icebergNamespaces {
		icebergNamespacesCount += len(m)
	}
	icebergNamespaces := make([]IcebergNamespaceEntry, 0, icebergNamespacesCount)
	for wh, nsMap := range f.icebergNamespaces {
		for _, entry := range nsMap {
			icebergNamespaces = append(icebergNamespaces, IcebergNamespaceEntry{
				Warehouse:  wh,
				Namespace:  cloneStringSlice(entry.Namespace),
				Properties: cloneStringMap(entry.Properties),
			})
		}
	}
	var icebergTablesCount int
	for _, m := range f.icebergTables {
		icebergTablesCount += len(m)
	}
	icebergTables := make([]IcebergTableEntry, 0, icebergTablesCount)
	for wh, tblMap := range f.icebergTables {
		for _, entry := range tblMap {
			e := cloneIcebergTableEntry(entry)
			e.Warehouse = wh
			icebergTables = append(icebergTables, e)
		}
	}
	exportStore := f.exportStore
	// Snapshot dekRefCounts while holding the read lock.
	dekRefCountsCopy := make(map[uint32]uint64, len(f.dekRefCounts))
	for g, c := range f.dekRefCounts {
		if c > 0 {
			dekRefCountsCopy[g] = c
		}
	}
	f.mu.RUnlock()

	nfsExports := map[string]nfsexport.Config(nil)
	if exportStore != nil {
		nfsExports = exportStore.Snapshot().Entries()
	}

	b := clusterBuilderPool.Get()

	// All nested objects must be built BEFORE MetaStateSnapshotStart (A1).

	// Build ShardGroupEntry offsets
	sgOffs := make([]flatbuffers.UOffsetT, len(shardGroups))
	for i := len(shardGroups) - 1; i >= 0; i-- {
		sg := shardGroups[i]
		idOff := b.CreateString(sg.ID)
		peerOffs := make([]flatbuffers.UOffsetT, len(sg.PeerIDs))
		for j := len(sg.PeerIDs) - 1; j >= 0; j-- {
			peerOffs[j] = b.CreateString(sg.PeerIDs[j])
		}
		clusterpb.ShardGroupEntryStartPeerIdsVector(b, len(peerOffs))
		for j := len(peerOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(peerOffs[j])
		}
		peerVec := b.EndVector(len(peerOffs))
		clusterpb.ShardGroupEntryStart(b)
		clusterpb.ShardGroupEntryAddId(b, idOff)
		clusterpb.ShardGroupEntryAddPeerIds(b, peerVec)
		sgOffs[i] = clusterpb.ShardGroupEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartShardGroupsVector(b, len(sgOffs))
	for i := len(sgOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(sgOffs[i])
	}
	sgVec := b.EndVector(len(sgOffs))

	// Build MetaNodeEntry offsets
	nodeOffs := make([]flatbuffers.UOffsetT, len(nodes))
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		idOff := b.CreateString(n.ID)
		addrOff := b.CreateString(n.Address)
		clusterpb.MetaNodeEntryStart(b)
		clusterpb.MetaNodeEntryAddId(b, idOff)
		clusterpb.MetaNodeEntryAddAddress(b, addrOff)
		clusterpb.MetaNodeEntryAddRole(b, n.Role)
		nodeOffs[i] = clusterpb.MetaNodeEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartNodesVector(b, len(nodeOffs))
	for i := len(nodeOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(nodeOffs[i])
	}
	nodesVec := b.EndVector(len(nodeOffs))

	// Build BucketAssignmentEntry offsets
	baOffs := make([]flatbuffers.UOffsetT, len(buckets))
	for i := len(buckets) - 1; i >= 0; i-- {
		bkt := buckets[i]
		bucketOff := b.CreateString(bkt.bucket)
		groupIDOff := b.CreateString(bkt.groupID)
		clusterpb.BucketAssignmentEntryStart(b)
		clusterpb.BucketAssignmentEntryAddBucket(b, bucketOff)
		clusterpb.BucketAssignmentEntryAddGroupId(b, groupIDOff)
		baOffs[i] = clusterpb.BucketAssignmentEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartBucketAssignmentsVector(b, len(baOffs))
	for i := len(baOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(baOffs[i])
	}
	baVec := b.EndVector(len(baOffs))

	// Build LoadStatEntry offsets (lsVec must be complete before MetaStateSnapshotStart)
	lsOffs := make([]flatbuffers.UOffsetT, len(lsEntries))
	for i := len(lsEntries) - 1; i >= 0; i-- {
		e := lsEntries[i]
		nodeIDOff := b.CreateString(e.NodeID)
		clusterpb.LoadStatEntryStart(b)
		clusterpb.LoadStatEntryAddNodeId(b, nodeIDOff)
		clusterpb.LoadStatEntryAddDiskUsedPct(b, e.DiskUsedPct)
		clusterpb.LoadStatEntryAddDiskAvailBytes(b, e.DiskAvailBytes)
		clusterpb.LoadStatEntryAddRequestsPerSec(b, e.RequestsPerSec)
		clusterpb.LoadStatEntryAddUpdatedAtUnix(b, e.UpdatedAt.Unix())
		lsOffs[i] = clusterpb.LoadStatEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartLoadSnapshotVector(b, len(lsOffs))
	for i := len(lsOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(lsOffs[i])
	}
	lsVec := b.EndVector(len(lsOffs))

	// Build active plan offset (activePlanOff must be complete before MetaStateSnapshotStart)
	var activePlanOff flatbuffers.UOffsetT
	if activePlanCopy != nil {
		planIDOff := b.CreateString(activePlanCopy.PlanID)
		groupIDOff := b.CreateString(activePlanCopy.GroupID)
		fromOff := b.CreateString(activePlanCopy.FromNode)
		toOff := b.CreateString(activePlanCopy.ToNode)
		clusterpb.RebalancePlanStart(b)
		clusterpb.RebalancePlanAddPlanId(b, planIDOff)
		clusterpb.RebalancePlanAddGroupId(b, groupIDOff)
		clusterpb.RebalancePlanAddFromNode(b, fromOff)
		clusterpb.RebalancePlanAddToNode(b, toOff)
		clusterpb.RebalancePlanAddCreatedAtUnix(b, activePlanCopy.CreatedAt.Unix())
		activePlanOff = clusterpb.RebalancePlanEnd(b)
	}

	icebergNamespaceVec := buildIcebergNamespaceEntriesVector(b, icebergNamespaces)
	icebergTableVec := buildIcebergTableEntriesVector(b, icebergTables)
	objectIndexVec := buildMetaObjectIndexEntriesVector(b, objectEntries)
	nfsExportVec := buildNfsExportEntriesVector(b, nfsExports)

	// ClusterConfig: serialize the wrapper's current snap into a stand-alone
	// FBS buffer and embed it as a [ubyte] vector. Always emit — the inner
	// buffer is small (~tens of bytes) and a zero-rev empty config round-trips
	// to the same zero clusterConfigSnap on Restore.
	ccBytes := serializeClusterConfig(f.clusterCfg)
	clusterConfigVec := b.CreateByteVector(ccBytes)

	clusterpb.MetaStateSnapshotStart(b)
	clusterpb.MetaStateSnapshotAddNodes(b, nodesVec)
	clusterpb.MetaStateSnapshotAddShardGroups(b, sgVec)
	clusterpb.MetaStateSnapshotAddBucketAssignments(b, baVec)
	clusterpb.MetaStateSnapshotAddLoadSnapshot(b, lsVec)
	if activePlanCopy != nil {
		clusterpb.MetaStateSnapshotAddActivePlan(b, activePlanOff)
	}
	clusterpb.MetaStateSnapshotAddIcebergNamespaces(b, icebergNamespaceVec)
	clusterpb.MetaStateSnapshotAddIcebergTables(b, icebergTableVec)
	clusterpb.MetaStateSnapshotAddObjectIndex(b, objectIndexVec)
	clusterpb.MetaStateSnapshotAddClusterConfig(b, clusterConfigVec)
	clusterpb.MetaStateSnapshotAddNfsExports(b, nfsExportVec)
	clusterpb.MetaStateSnapshotAddIcebergSchemaVersion(b, 2)
	root := clusterpb.MetaStateSnapshotEnd(b)
	bs := fbFinish(b, root)

	return f.appendSnapshotTrailers(bs, dekRefCountsCopy)
}

// Restore deserializes a MetaStateSnapshot and replaces current state. The
// store-meta record (meta) carries the snapshot FormatVersion; the meta-Raft
// FSM has its own in-payload versioning (FlatBuffers + IAM trailer) and accepts
// any FormatVersion for backward compatibility with pre-C2-P3 data dirs.
func (f *MetaFSM) Restore(_ raft.SnapshotMeta, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: Restore: empty snapshot")
	}

	trailers, err := peelMetaSnapshotTrailers(data)
	if err != nil {
		return err
	}

	var (
		snap   *clusterpb.MetaStateSnapshot
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaStateSnapshot flatbuffer: %v", r)
			}
		}()
		snap = clusterpb.GetRootAsMetaStateSnapshot(trailers.fbData, 0)
	}()
	if decErr != nil {
		return decErr
	}

	newNodes := make(map[string]MetaNodeEntry, snap.NodesLength())
	var nodeEntry clusterpb.MetaNodeEntry
	for i := 0; i < snap.NodesLength(); i++ {
		if snap.Nodes(&nodeEntry, i) {
			e := MetaNodeEntry{
				ID:      string(nodeEntry.Id()),
				Address: string(nodeEntry.Address()),
				Role:    nodeEntry.Role(),
			}
			newNodes[e.ID] = e
		}
	}

	newShardGroups := make(map[string]ShardGroupEntry, snap.ShardGroupsLength())
	var sgEntry clusterpb.ShardGroupEntry
	for i := 0; i < snap.ShardGroupsLength(); i++ {
		if !snap.ShardGroups(&sgEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: shard group %d decode failed", i)
		}
		peers := make([]string, sgEntry.PeerIdsLength())
		for j := 0; j < sgEntry.PeerIdsLength(); j++ {
			peers[j] = string(sgEntry.PeerIds(j))
		}
		e := ShardGroupEntry{
			ID:      string(sgEntry.Id()),
			PeerIDs: peers,
		}
		// Mirror applyPutShardGroup: drop reserved IDs so log-replay and
		// snapshot-restore land on the same FSM state. Without this, a node
		// that joins via snapshot install would carry a reserved ID while a
		// peer that replayed from log would not — silent quorum divergence.
		// Pre-v0.0.19 snapshots may still contain such IDs; we skip them.
		if err := raft.ValidateGroupID(e.ID); err != nil {
			log.Warn().Err(err).Str("group_id", e.ID).Msg("meta_fsm: Restore: dropping reserved group ID from snapshot")
			continue
		}
		newShardGroups[e.ID] = e
	}

	newBucketAssignments := make(map[string]string, snap.BucketAssignmentsLength())
	var baEntry clusterpb.BucketAssignmentEntry
	for i := 0; i < snap.BucketAssignmentsLength(); i++ {
		if !snap.BucketAssignments(&baEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: bucket assignment %d decode failed", i)
		}
		bucket := string(baEntry.Bucket())
		newBucketAssignments[bucket] = string(baEntry.GroupId())
	}

	newLoadSnapshot := make(map[string]LoadStatEntry, snap.LoadSnapshotLength())
	var lsEntry clusterpb.LoadStatEntry
	for i := 0; i < snap.LoadSnapshotLength(); i++ {
		if !snap.LoadSnapshot(&lsEntry, i) {
			continue
		}
		e := LoadStatEntry{
			NodeID:         string(lsEntry.NodeId()),
			DiskUsedPct:    lsEntry.DiskUsedPct(),
			DiskAvailBytes: lsEntry.DiskAvailBytes(),
			RequestsPerSec: lsEntry.RequestsPerSec(),
			UpdatedAt:      time.Unix(lsEntry.UpdatedAtUnix(), 0),
		}
		newLoadSnapshot[e.NodeID] = e
	}

	var newActivePlan *RebalancePlan
	var planFB clusterpb.RebalancePlan
	if p := snap.ActivePlan(&planFB); p != nil && len(p.PlanId()) > 0 {
		newActivePlan = &RebalancePlan{
			PlanID:    string(p.PlanId()),
			GroupID:   string(p.GroupId()),
			FromNode:  string(p.FromNode()),
			ToNode:    string(p.ToNode()),
			CreatedAt: time.Unix(p.CreatedAtUnix(), 0),
		}
	}

	// iceberg_schema_version tracks the Iceberg section format:
	//   0 = pre-T38 / pre-Commit-3 (no warehouse field in entries); only safe when no entries present
	//   2 = warehouse-aware (D#14 T38 Commit 3)
	//
	// Any other value (e.g. 1, 3, …) is unknown — fail loud so a future format
	// change is never silently misread as version-2.
	icebergSchemaVersion := snap.IcebergSchemaVersion()
	hasIcebergData := snap.IcebergNamespacesLength() > 0 || snap.IcebergTablesLength() > 0
	if icebergSchemaVersion == 0 && hasIcebergData {
		return fmt.Errorf("meta_fsm: Restore: iceberg_schema_version=0 with %d namespaces and %d tables: "+
			"snapshot was written by a pre-T38 node; cannot safely determine warehouse assignments — "+
			"re-snapshot from a T38+ node before restore",
			snap.IcebergNamespacesLength(), snap.IcebergTablesLength())
	}
	if icebergSchemaVersion != 0 && icebergSchemaVersion != 2 {
		return fmt.Errorf("meta_fsm: Restore: unsupported iceberg_schema_version=%d (expected 0 for legacy or 2 for warehouse-aware)", icebergSchemaVersion)
	}

	newIcebergNamespaces := make(map[string]map[string]IcebergNamespaceEntry)
	var nsFB clusterpb.IcebergNamespaceEntry
	for i := 0; i < snap.IcebergNamespacesLength(); i++ {
		if !snap.IcebergNamespaces(&nsFB, i) {
			return fmt.Errorf("meta_fsm: Restore: iceberg namespace %d decode failed", i)
		}
		wh := string(nsFB.Warehouse())
		if wh == "" {
			wh = icebergDefaultWarehouse
		}
		entry := IcebergNamespaceEntry{
			Warehouse:  wh,
			Namespace:  readStringVector(nsFB.NamespaceLength(), nsFB.Namespace),
			Properties: readKeyValueProperties(nsFB.PropertiesLength(), nsFB.Properties),
		}
		if m := newIcebergNamespaces[wh]; m == nil {
			newIcebergNamespaces[wh] = make(map[string]IcebergNamespaceEntry)
		}
		newIcebergNamespaces[wh][icebergNamespaceKey(entry.Namespace)] = entry
	}

	newIcebergTables := make(map[string]map[string]IcebergTableEntry)
	var tableFB clusterpb.IcebergTableEntry
	for i := 0; i < snap.IcebergTablesLength(); i++ {
		if !snap.IcebergTables(&tableFB, i) {
			return fmt.Errorf("meta_fsm: Restore: iceberg table %d decode failed", i)
		}
		identFB := tableFB.Identifier(nil)
		if identFB == nil {
			return fmt.Errorf("meta_fsm: Restore: iceberg table %d missing identifier", i)
		}
		ident := icebergcatalog.Identifier{
			Namespace: readStringVector(identFB.NamespaceLength(), identFB.Namespace),
			Name:      string(identFB.Name()),
		}
		wh := string(tableFB.Warehouse())
		if wh == "" {
			wh = icebergDefaultWarehouse
		}
		entry := IcebergTableEntry{
			Warehouse:        wh,
			Identifier:       ident,
			MetadataLocation: string(tableFB.MetadataLocation()),
			Properties:       readKeyValueProperties(tableFB.PropertiesLength(), tableFB.Properties),
		}
		if m := newIcebergTables[wh]; m == nil {
			newIcebergTables[wh] = make(map[string]IcebergTableEntry)
		}
		newIcebergTables[wh][icebergTableKey(ident)] = entry
	}

	newObjectIndex := make(map[string]ObjectIndexEntry, snap.ObjectIndexLength())
	newObjectLatest := make(map[string]string)
	var objFB clusterpb.MetaObjectIndexEntry
	for i := 0; i < snap.ObjectIndexLength(); i++ {
		if !snap.ObjectIndex(&objFB, i) {
			return fmt.Errorf("meta_fsm: Restore: object index %d decode failed", i)
		}
		entry := readMetaObjectIndexEntry(&objFB)
		vkey := objectIndexVersionKey(entry.Bucket, entry.Key, entry.VersionID)
		newObjectIndex[vkey] = entry
		if objFB.IsLatest() {
			newObjectLatest[objectIndexLatestKey(entry.Bucket, entry.Key)] = entry.VersionID
		}
	}

	hasNfsExports := snap.NfsExportsPresent()
	newNfsExports := make(map[string]nfsexport.Config, snap.NfsExportsLength())
	var exportFB clusterpb.NfsExportUpsertCmd
	for i := 0; i < snap.NfsExportsLength(); i++ {
		if !snap.NfsExports(&exportFB, i) {
			return fmt.Errorf("meta_fsm: Restore: NFS export %d decode failed", i)
		}
		bucket := string(exportFB.Bucket())
		cfgFB := exportFB.Config(nil)
		if bucket == "" || cfgFB == nil {
			return fmt.Errorf("meta_fsm: Restore: NFS export %d missing bucket/config", i)
		}
		newNfsExports[bucket] = nfsexport.Config{
			ReadOnly:   cfgFB.ReadOnly(),
			FsidMajor:  cfgFB.FsidMajor(),
			FsidMinor:  cfgFB.FsidMinor(),
			Generation: cfgFB.Generation(),
		}
	}

	// ClusterConfig: decode the embedded FBS blob and atomically swap into
	// f.clusterCfg via ReplaceSnap so the outer *ClusterConfig handle held
	// by consumers (e.g. balancer, alerts, disk monitor) stays valid across
	// snapshot install. Empty/missing blob (legacy pre-Slice-1 snapshots)
	// leaves the existing clusterCfg untouched.
	var newClusterCfgSnap *clusterConfigSnap
	if snap.ClusterConfigLength() > 0 {
		cs, err := deserializeClusterConfig(snap.ClusterConfigBytes())
		if err != nil {
			return fmt.Errorf("meta_fsm: Restore: decode cluster config: %w", err)
		}
		newClusterCfgSnap = cs
	}

	// --- DECODE PHASE ---
	// Decode all trailers into local variables BEFORE touching any f.* field.
	// If any decode fails, Restore returns an error with f.* completely untouched.

	// IAM: validate by decoding into a temporary store; commit via RestoreFrom later.
	// (F17: iamEnc is no longer needed at commit time — RestoreFrom swaps the state
	// pointer atomically without re-parsing the snapshot bytes.)
	var iamTempStore *iam.Store
	if len(trailers.iamData) > 0 {
		if f.iamStore == nil || f.iamApplier == nil {
			log.Warn().Int("iam_len", len(trailers.iamData)).Msg("meta_fsm: Restore: snapshot contains IAM section but IAM not wired; skipping IAM restore")
		} else {
			enc := f.iamApplier.Encryptor()
			if enc == nil {
				log.Warn().Msg("meta_fsm: Restore: IAM applier has no encryptor; skipping IAM restore")
			} else {
				tmp := iam.NewStore()
				if err := iam.ReadSnapshot(bytes.NewReader(trailers.iamData), tmp, enc); err != nil {
					return fmt.Errorf("meta_fsm: Restore: decode IAM: %w", err)
				}
				iamTempStore = tmp
			}
		}
	}

	// GCFG: decode config values.
	var newCfgValues map[string]string
	if len(trailers.cfgData) > 0 {
		if f.cfgStore == nil {
			log.Warn().Int("cfg_len", len(trailers.cfgData)).Msg("meta_fsm: Restore: snapshot contains config section but config store not wired; skipping")
		} else {
			values, err := decodeMetaConfigSnapshot(trailers.cfgData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode config: %w", err)
			}
			newCfgValues = values
		}
	}

	// DKVS: decode DEK version snapshot.
	var (
		newDEKVersions map[uint32][]byte
		newDEKActive   uint32
		newDEKRefs     map[uint32]uint64
		hasDEKData     bool
	)
	if len(trailers.dekData) > 0 {
		versions, active, refs, err := decodeMetaDEKVersionSnapshot(trailers.dekData)
		if err != nil {
			return fmt.Errorf("meta_fsm: Restore: decode DEK versions: %w", err)
		}
		newDEKVersions = versions
		newDEKActive = active
		newDEKRefs = refs
		hasDEKData = true
	}

	// IPST: decode IAM policy stores snapshot (§2 + §A stores).
	type ipstDecoded struct {
		polSnap    []policystore.PolicyEntry
		grpSnap    []group.GroupEntry
		attachSnap policyattach.AttachSnapshot
		bpSnap     []bucketpolicy.BucketPolicyEntry
		mountSAs   []mountsastore.MountSA
	}
	var newIPST *ipstDecoded
	if len(trailers.ipstData) > 0 {
		if f.policyStore == nil && f.groupStore == nil && f.policyAttachStore == nil && f.bucketPolicyStore == nil && f.mountSAStore == nil {
			log.Warn().Int("ipst_len", len(trailers.ipstData)).Msg("meta_fsm: Restore: snapshot contains IPST section but no policy stores wired; skipping")
		} else {
			polSnap, grpSnap, attachSnap, bpSnap, mountSAs, err := decodeMetaIAMPolicyStoresSnapshot(trailers.ipstData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode IAM policy stores: %w", err)
			}
			newIPST = &ipstDecoded{polSnap, grpSnap, attachSnap, bpSnap, mountSAs}
		}
	}

	// JKEY: decode JWT signing keys.
	// F9: if JKEY data is present but DEK keeper is not wired, the keys cannot be
	// unwrapped after Restore — fail loud rather than silently leaving jwtKeys empty.
	// F14: stage LoadFromSeeds against a scratch KeySet BEFORE touching f.jwtKeyStore /
	// f.jwtKeys so that a partial-unwrap failure leaves both fields untouched (atomic).
	var (
		newJkeyCurrent  *iamjwt.KeySeed
		newJkeyPrevious *iamjwt.KeySeed
		scratchJWTKeys  *iamjwt.KeySet
		hasJKEYData     = len(trailers.jkeyData) > 0
	)
	if hasJKEYData {
		if f.dekKeeper == nil {
			return fmt.Errorf("meta_fsm: Restore: JKEY trailer present but DEK keeper not wired — cannot unwrap signing keys")
		}
		cur, prev, err := decodeJWTKeyStore(trailers.jkeyData)
		if err != nil {
			return fmt.Errorf("meta_fsm: Restore: decode JKEY: %w", err)
		}
		var seeds []iamjwt.KeySeed
		if cur != nil {
			seeds = append(seeds, *cur)
		}
		if prev != nil {
			seeds = append(seeds, *prev)
		}
		scratch := iamjwt.NewKeySet()
		if err := scratch.LoadFromSeeds(seeds, f.dekKeeper); err != nil {
			return fmt.Errorf("meta_fsm: Restore: JKEY LoadFromSeeds: %w", err)
		}
		newJkeyCurrent = cur
		newJkeyPrevious = prev
		scratchJWTKeys = scratch
	}

	// --- COMMIT PHASE ---
	// All decodes succeeded. Commit to f.* fields. No error returns below.

	f.mu.Lock()
	f.nodes = newNodes
	f.shardGroups = newShardGroups
	f.bucketAssignments = newBucketAssignments
	f.objectIndex = newObjectIndex
	f.objectLatest = newObjectLatest
	f.loadSnapshot = newLoadSnapshot
	f.activePlan = newActivePlan
	f.icebergNamespaces = newIcebergNamespaces
	f.icebergTables = newIcebergTables
	if hasDEKData {
		f.pendingDEKVersions = newDEKVersions
		f.pendingDEKActive = newDEKActive
		if newDEKRefs != nil {
			f.dekRefCounts = newDEKRefs
		} else {
			// Pre-Task-12 snapshot: no ref_counts trailer field. Rebuild from the
			// just-restored objectIndex so DEK prune-safety sees accurate counts.
			// All legacy entries decode dek_gen=0 via FlatBuffer default.
			f.dekRefCounts = make(map[uint32]uint64, len(f.objectIndex))
			for _, e := range f.objectIndex {
				f.dekRefCounts[e.DekGen]++
			}
		}
	}
	cb := f.onBucketAssigned
	f.mu.Unlock()

	if newClusterCfgSnap != nil {
		f.clusterCfg.ReplaceSnap(newClusterCfgSnap)
	}
	restoredNfsExports := false
	// NOTE: nfsexport.Store.ReplaceAll touches BadgerDB and may return an error
	// after the core FSM fields are already committed. Making this fully atomic
	// requires refactoring nfsexport.Store behind an interface for staged-commit.
	// Deferred to a follow-up — see TODOS for the design discussion. In practice
	// a BadgerDB error here would indicate disk failure during snapshot restore,
	// which warrants operator intervention regardless of atomicity.
	if f.exportStore != nil && hasNfsExports {
		if err := f.exportStore.ReplaceAll(newNfsExports); err != nil {
			return fmt.Errorf("meta_fsm: Restore: NFS exports: %w", err)
		}
		restoredNfsExports = true
	} else if f.exportStore == nil && len(newNfsExports) > 0 {
		return fmt.Errorf("meta_fsm: Restore: snapshot contains NFS exports but export store is not wired")
	}
	if restoredNfsExports {
		f.publishNfsExportChange()
	}
	if cb != nil {
		for bucket, groupID := range newBucketAssignments {
			cb(bucket, groupID)
		}
	}
	// onRebalancePlan is intentionally NOT called here.
	// Rebalancer handles resume on next tick by checking ActivePlan().

	// IAM commit — iamTempStore holds the fully-decoded snapshot; swap it in atomically.
	// RestoreFrom copies the state pointer from iamTempStore into f.iamStore in one
	// atomic store — no second decode/parse is needed, so no error is possible here
	// (F17: eliminates the error-returning ReadSnapshot call after core fields commit).
	if iamTempStore != nil {
		f.iamStore.RestoreFrom(iamTempStore)
	}

	// GCFG commit.
	if newCfgValues != nil {
		f.cfgStore.Restore(newCfgValues)
	}

	// IPST commit — apply all §2 + §A policy stores.
	if newIPST != nil {
		// Warn per nil store. The stores form a single coherent unit
		// (group memberships, attached policies, bucket policies all reference
		// each other); silently dropping one half desyncs the others against
		// the snapshot. The all-nil path warns once above; here we surface
		// per-store gaps so the operator sees exactly what was lost.
		if f.policyStore == nil {
			log.Warn().Int("entries", len(newIPST.polSnap)).Msg("meta_fsm: Restore: IPST has policy entries but policyStore not wired; entries dropped")
		} else {
			f.policyStore.ReplaceAll(newIPST.polSnap)
		}
		if f.groupStore == nil {
			log.Warn().Int("entries", len(newIPST.grpSnap)).Msg("meta_fsm: Restore: IPST has group entries but groupStore not wired; entries dropped")
		} else {
			f.groupStore.ReplaceAll(newIPST.grpSnap)
		}
		if f.policyAttachStore == nil {
			log.Warn().Int("sa_entries", len(newIPST.attachSnap.SAAttachments)).Int("group_entries", len(newIPST.attachSnap.GroupAttachments)).Msg("meta_fsm: Restore: IPST has policy-attach entries but policyAttachStore not wired; entries dropped")
		} else {
			f.policyAttachStore.ReplaceAll(newIPST.attachSnap)
		}
		if f.bucketPolicyStore == nil {
			log.Warn().Int("entries", len(newIPST.bpSnap)).Msg("meta_fsm: Restore: IPST has bucket-policy entries but bucketPolicyStore not wired; entries dropped")
		} else {
			f.bucketPolicyStore.ReplaceAll(newIPST.bpSnap)
		}
		if f.mountSAStore == nil {
			if len(newIPST.mountSAs) > 0 {
				log.Warn().Int("entries", len(newIPST.mountSAs)).Msg("meta_fsm: Restore: IPST has MountSA entries but mountSAStore not wired; entries dropped")
			}
		} else {
			if err := f.mountSAStore.ReplaceAll(newIPST.mountSAs); err != nil {
				return fmt.Errorf("meta_fsm: Restore: MountSA store ReplaceAll: %w", err)
			}
		}
		// Invalidate the resolver cache so stale pre-restore entries don't
		// survive the snapshot install. Empty saIDs+buckets nukes the full cache.
		if f.policyResolver != nil {
			f.policyResolver.Invalidate(nil, nil)
		}
	}

	// JKEY commit — both f.jwtKeyStore and f.jwtKeys are updated together.
	// scratchJWTKeys was built (and LoadFromSeeds succeeded) in the decode phase
	// above, so no error is possible here (F14: atomic commit).
	if hasJKEYData {
		f.jwtKeyStore.ReplaceAll(newJkeyCurrent, newJkeyPrevious)
		f.jwtKeys = scratchJWTKeys
	} else {
		f.jwtKeyStore.ReplaceAll(nil, nil)
	}
	return nil
}

// --- encoding helpers ---

// encodeMetaCmd wraps a typed payload in a MetaCmd FlatBuffers envelope.
func encodeMetaCmd(cmdType MetaCmdType, payload []byte) ([]byte, error) {
	b := clusterBuilderPool.Get()
	var dataOff flatbuffers.UOffsetT
	if len(payload) > 0 {
		dataOff = b.CreateByteVector(payload)
	}
	clusterpb.MetaCmdStart(b)
	clusterpb.MetaCmdAddType(b, cmdType)
	if len(payload) > 0 {
		clusterpb.MetaCmdAddData(b, dataOff)
	}
	return fbFinish(b, clusterpb.MetaCmdEnd(b)), nil
}

func encodeMetaAddNodeCmd(node MetaNodeEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(node.ID)
	addrOff := b.CreateString(node.Address)
	clusterpb.MetaNodeEntryStart(b)
	clusterpb.MetaNodeEntryAddId(b, idOff)
	clusterpb.MetaNodeEntryAddAddress(b, addrOff)
	clusterpb.MetaNodeEntryAddRole(b, node.Role)
	nodeOff := clusterpb.MetaNodeEntryEnd(b)

	clusterpb.MetaAddNodeCmdStart(b)
	clusterpb.MetaAddNodeCmdAddNode(b, nodeOff)
	return fbFinish(b, clusterpb.MetaAddNodeCmdEnd(b)), nil
}

//nolint:unused // package tests pin meta-FSM command compatibility.
func encodeMetaRemoveNodeCmd(nodeID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(nodeID)
	clusterpb.MetaRemoveNodeCmdStart(b)
	clusterpb.MetaRemoveNodeCmdAddNodeId(b, idOff)
	return fbFinish(b, clusterpb.MetaRemoveNodeCmdEnd(b)), nil
}

//nolint:unused // referenced by meta_fsm_capability_test.go.
func buildMetaCapabilityActivatePayload(capability string) []byte {
	b := flatbuffers.NewBuilder(128)
	capOff := b.CreateString(capability)
	clusterpb.MetaCapabilityActivateCmdStart(b)
	clusterpb.MetaCapabilityActivateCmdAddCapability(b, capOff)
	root := clusterpb.MetaCapabilityActivateCmdEnd(b)
	return fbFinish(b, root)
}

func encodeMetaScrubTriggerCmd(entry scrubber.ScrubTriggerEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()
	sidOff := b.CreateString(entry.SessionID)
	bktOff := b.CreateString(entry.Bucket)
	pfxOff := b.CreateString(entry.KeyPrefix)
	nodeOff := b.CreateString(entry.OriginatorNodeID)
	clusterpb.MetaScrubTriggerCmdStart(b)
	clusterpb.MetaScrubTriggerCmdAddSessionId(b, sidOff)
	clusterpb.MetaScrubTriggerCmdAddBucket(b, bktOff)
	clusterpb.MetaScrubTriggerCmdAddKeyPrefix(b, pfxOff)
	clusterpb.MetaScrubTriggerCmdAddScope(b, int32(entry.Scope))
	clusterpb.MetaScrubTriggerCmdAddDryRun(b, entry.DryRun)
	clusterpb.MetaScrubTriggerCmdAddRequestedAt(b, entry.RequestedAt)
	clusterpb.MetaScrubTriggerCmdAddOriginatorNodeId(b, nodeOff)
	return fbFinish(b, clusterpb.MetaScrubTriggerCmdEnd(b)), nil
}

func encodeMetaPutShardGroupCmd(sg ShardGroupEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()

	idOff := b.CreateString(sg.ID)
	peerOffs := make([]flatbuffers.UOffsetT, len(sg.PeerIDs))
	for i := len(sg.PeerIDs) - 1; i >= 0; i-- {
		peerOffs[i] = b.CreateString(sg.PeerIDs[i])
	}
	clusterpb.ShardGroupEntryStartPeerIdsVector(b, len(peerOffs))
	for i := len(peerOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(peerOffs[i])
	}
	peerVec := b.EndVector(len(peerOffs))

	clusterpb.ShardGroupEntryStart(b)
	clusterpb.ShardGroupEntryAddId(b, idOff)
	clusterpb.ShardGroupEntryAddPeerIds(b, peerVec)
	sgOff := clusterpb.ShardGroupEntryEnd(b)

	clusterpb.MetaPutShardGroupCmdStart(b)
	clusterpb.MetaPutShardGroupCmdAddGroup(b, sgOff)
	return fbFinish(b, clusterpb.MetaPutShardGroupCmdEnd(b)), nil
}

func cloneStringSlice(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneObjectIndexEntry(in ObjectIndexEntry) ObjectIndexEntry {
	in.NodeIDs = cloneStringSlice(in.NodeIDs)
	if len(in.Parts) > 0 {
		cp := make([]storage.MultipartPartEntry, len(in.Parts))
		copy(cp, in.Parts)
		in.Parts = cp
	} else {
		in.Parts = nil
	}
	return in
}

func readStringVector(n int, at func(int) []byte) []string {
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = string(at(i))
	}
	return out
}

func readKeyValueProperties(n int, at func(*clusterpb.KeyValue, int) bool) map[string]string {
	if n == 0 {
		return nil
	}
	out := make(map[string]string, n)
	var kv clusterpb.KeyValue
	for i := 0; i < n; i++ {
		if at(&kv, i) {
			out[string(kv.Key())] = string(kv.ValueBytes())
		}
	}
	return out
}

func sortedPropertyKeys(properties map[string]string) []string {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func buildKeyValuePropertiesVector(
	b *flatbuffers.Builder,
	properties map[string]string,
	startVector func(*flatbuffers.Builder, int) flatbuffers.UOffsetT,
) flatbuffers.UOffsetT {
	keys := sortedPropertyKeys(properties)
	offsets := make([]flatbuffers.UOffsetT, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		keyOff := b.CreateString(keys[i])
		valueOff := b.CreateByteVector([]byte(properties[keys[i]]))
		clusterpb.KeyValueStart(b)
		clusterpb.KeyValueAddKey(b, keyOff)
		clusterpb.KeyValueAddValue(b, valueOff)
		offsets[i] = clusterpb.KeyValueEnd(b)
	}
	startVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}
