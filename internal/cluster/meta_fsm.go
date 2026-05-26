package cluster

import (
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

	// activeKEKVersion is the cluster-wide KEK version that current wrap[gen]
	// entries are sealed under. Phase A pins this to 0 (no rotation yet);
	// Phase B will mutate it via MetaCmdTypeKEKRotate Apply. Persisted in the
	// DKVS snapshot trailer alongside DEK versions.
	activeKEKVersion uint32

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

	// lastRotationRequests is a bounded FIFO ring of KEK rotation request
	// idempotency records. Capped at maxRotationRequestStatusEntries; oldest
	// evicted on overflow. Insertion-order only — reads must NOT reorder.
	lastRotationRequests []rotationRequestRecord

	// kekStatuses holds the lifecycle status of each KEK version, sorted by
	// version ascending. Upserted via SetKEKStatus.
	kekStatuses []kekStatusRecord
}

// RotationStatus is the outcome code stored for each KEK rotation request.
type RotationStatus uint8

const (
	RotationStatusApplied   RotationStatus = 0
	RotationStatusStaleNoOp RotationStatus = 1
	RotationStatusRejected  RotationStatus = 2
)

// KEKLifecycleStatus is the lifecycle stage of a KEK version.
type KEKLifecycleStatus uint8

const (
	KEKLifecycleActive   KEKLifecycleStatus = 0
	KEKLifecycleRetiring KEKLifecycleStatus = 1
	KEKLifecyclePruned   KEKLifecycleStatus = 2
)

// maxRotationRequestStatusEntries is the FIFO ring capacity.
const maxRotationRequestStatusEntries = 1024

type rotationRequestRecord struct {
	requestID  [16]byte
	status     RotationStatus
	applyIndex uint64
}

type kekStatusRecord struct {
	version           uint32
	status            KEKLifecycleStatus
	retireCommitIndex uint64
}

// RecordRotationRequestStatus appends a rotation request outcome to the FIFO
// ring. Acquires the write lock. Evicts the oldest entry when the ring is full.
func (f *MetaFSM) RecordRotationRequestStatus(requestID [16]byte, status RotationStatus, applyIndex uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordRotationRequestStatusLocked(requestID, status, applyIndex)
}

func (f *MetaFSM) recordRotationRequestStatusLocked(requestID [16]byte, status RotationStatus, applyIndex uint64) {
	if len(f.lastRotationRequests) >= maxRotationRequestStatusEntries {
		f.lastRotationRequests = f.lastRotationRequests[1:]
	}
	f.lastRotationRequests = append(f.lastRotationRequests, rotationRequestRecord{
		requestID:  requestID,
		status:     status,
		applyIndex: applyIndex,
	})
}

// LookupRotationRequestStatus returns the status of the given request ID,
// scanning newest-first. Returns (0, false) if not found. Read-only; does NOT
// reorder entries.
func (f *MetaFSM) LookupRotationRequestStatus(requestID [16]byte) (RotationStatus, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lookupRotationRequestStatusLocked(requestID)
}

func (f *MetaFSM) lookupRotationRequestStatusLocked(requestID [16]byte) (RotationStatus, bool) {
	for i := len(f.lastRotationRequests) - 1; i >= 0; i-- {
		if f.lastRotationRequests[i].requestID == requestID {
			return f.lastRotationRequests[i].status, true
		}
	}
	return 0, false
}

// SetKEKStatus upserts the lifecycle status for a KEK version. The slice is
// kept sorted by version ascending. Acquires the write lock.
func (f *MetaFSM) SetKEKStatus(version uint32, status KEKLifecycleStatus, retireCommitIndex uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, e := range f.kekStatuses {
		if e.version == version {
			f.kekStatuses[i].status = status
			f.kekStatuses[i].retireCommitIndex = retireCommitIndex
			return
		}
	}
	f.kekStatuses = append(f.kekStatuses, kekStatusRecord{
		version:           version,
		status:            status,
		retireCommitIndex: retireCommitIndex,
	})
	sort.Slice(f.kekStatuses, func(i, j int) bool {
		return f.kekStatuses[i].version < f.kekStatuses[j].version
	})
}

// LookupKEKStatus returns the lifecycle status for the given KEK version.
// Returns (0, 0, 0, false) if not found.
func (f *MetaFSM) LookupKEKStatus(version uint32) (v uint32, status KEKLifecycleStatus, retireCommitIndex uint64, ok bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, e := range f.kekStatuses {
		if e.version == version {
			return e.version, e.status, e.retireCommitIndex, true
		}
	}
	return 0, 0, 0, false
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

//nolint:unused // referenced by meta_fsm_capability_test.go.
func buildMetaCapabilityActivatePayload(capability string) []byte {
	b := flatbuffers.NewBuilder(128)
	capOff := b.CreateString(capability)
	clusterpb.MetaCapabilityActivateCmdStart(b)
	clusterpb.MetaCapabilityActivateCmdAddCapability(b, capOff)
	root := clusterpb.MetaCapabilityActivateCmdEnd(b)
	return fbFinish(b, root)
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
