package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/reservedname"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// iamSnapshotTrailerMagic is appended after the IAM section so post-fix
// readers can distinguish "new snapshot with IAM trailer" from a legacy
// snapshot that ends at the FlatBuffer root. ASCII "IAMG" (0x47414D49 little-endian).
const iamSnapshotTrailerMagic uint32 = 0x47414D49

// iamSnapshotTrailerLen is the on-disk size of the trailer footer:
// [u32 iam_len][u32 magic]. Always 8 bytes when present.
const iamSnapshotTrailerLen = 8

// cfgSnapshotTrailerMagic identifies the GCFG trailer appended after the IAM
// trailer (or after the FB root when IAM is absent). Hex pairs spell "GCFG"
// (0x47=G, 0x43=C, 0x46=F, 0x47=G, little-endian uint32 = 0x47464347).
//
// Wire layout (appended after existing trailers):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[configPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic GCFG  LE]
//
// Restore reads from the end: check last 4 bytes for GCFG magic, if present
// read payloadLen, strip GCFG payload+footer, then continue IAM detection on
// the remaining bytes.
const cfgSnapshotTrailerMagic uint32 = 0x47464347

// cfgSnapshotTrailerLen is the on-disk size of the GCFG footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const cfgSnapshotTrailerLen = 8

// dekSnapshotTrailerMagic identifies the DKVS trailer appended after the GCFG
// trailer (or after IAM when GCFG absent, or after FB root when both absent).
// Hex pairs spell "DKVS" (0x44=D, 0x4B=K, 0x56=V, 0x53=S, little-endian uint32).
//
// Wire layout (appended after existing trailers):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[GCFG trailer bytes]      (optional, magic 0x47464347)
//	[dekPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic DKVS  LE]
//
// Restore reads from the end: check last 4 bytes for DKVS magic, if present
// read payloadLen, strip DKVS payload+footer, then continue GCFG/IAM detection.
const dekSnapshotTrailerMagic uint32 = 0x53564B44

// dekSnapshotTrailerLen is the on-disk size of the DKVS footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const dekSnapshotTrailerLen = 8

// ipstSnapshotTrailerMagic identifies the IPST trailer appended after the DKVS
// trailer (or after whichever trailers precede it). Hex pairs spell "IPST"
// (0x49=I, 0x50=P, 0x53=S, 0x54=T → little-endian uint32 = 0x54535049).
//
// Carries: PolicyStore + GroupStore + PolicyAttachStore + BucketPolicyStore in
// a single FlatBuffers payload. One trailer for all 4 §2 stores keeps the chain
// depth manageable and reflects that the stores always serialize together.
//
// Wire layout (appended before JKEY; JKEY is now the outermost trailer):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[GCFG trailer bytes]      (optional, magic 0x47464347)
//	[DKVS trailer bytes]      (optional, magic 0x53564B44)
//	[ipstPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic IPST  LE]
//
// Restore reads from the end: peel JKEY first, then check for IPST magic,
// if present read payloadLen, strip IPST payload+footer, then continue DKVS/GCFG/IAM peel.
const ipstSnapshotTrailerMagic uint32 = 0x54535049

// ipstSnapshotTrailerLen is the on-disk size of the IPST footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const ipstSnapshotTrailerLen = 8

// jkeySnapshotTrailerMagic identifies the JKEY trailer appended after the IPST
// trailer (outermost trailer). Hex pairs spell "JKEY"
// (0x4A=J, 0x4B=K, 0x45=E, 0x59=Y, little-endian uint32 = 0x59454B4A).
//
// Carries: JWTKeyStore — current + previous wrapped JWT signing keys.
// Only appended when at least one key exists (nil/nil is skipped for back-compat).
//
// Wire layout (JKEY is the newest/outermost trailer):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[GCFG trailer bytes]      (optional, magic 0x47464347)
//	[DKVS trailer bytes]      (optional, magic 0x53564B44)
//	[IPST trailer bytes]      (optional, magic 0x54535049)
//	[jkeyPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic JKEY  LE]
//
// Restore reads from the end: check last 4 bytes for JKEY magic, if present
// read payloadLen, strip JKEY payload+footer, then continue IPST/DKVS/GCFG/IAM peel.
const jkeySnapshotTrailerMagic uint32 = 0x59454B4A

// jkeySnapshotTrailerLen is the on-disk size of the JKEY footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const jkeySnapshotTrailerLen = 8

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

	// policyAttachStore is the SA/group→policy attachment store. nil until
	// SetPolicyAttachStore is called; PolicyAttach* commands are safe no-ops when nil.
	policyAttachStore *policyattach.InMemoryStore

	// bucketPolicyStore is the per-bucket policy document store. nil until
	// SetBucketPolicyStore is called; BucketPolicy* commands are safe no-ops when nil.
	bucketPolicyStore *bucketpolicy.InMemoryStore

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

// SetEncryptor wires the cluster-wide encryptor used to gate cluster-config
// patches carrying wrapped secrets. Must be called before the raft log starts
// replaying. nil means cluster-config patches with a wrapped secret will be
// rejected at apply.
func (f *MetaFSM) SetEncryptor(e *encrypt.Encryptor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.encryptor = e
}

// Encryptor returns the registered encryptor, or nil if it has not been wired.
func (f *MetaFSM) Encryptor() *encrypt.Encryptor { return f.encryptor }

// SetConfigStore wires the cluster-wide config registry into the MetaFSM.
// Must be called before the raft log starts replaying. nil means
// ConfigPut/ConfigDelete commands are safe no-ops (not configured yet).
func (f *MetaFSM) SetConfigStore(s *config.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgStore = s
}

// SetDEKKeeper wires the DEK keeper into the MetaFSM. Must be called before
// the raft log starts replaying. nil means DEKRotate/DEKVersionPrune are safe
// no-ops (not configured yet).
func (f *MetaFSM) SetDEKKeeper(k *encrypt.DEKKeeper) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dekKeeper = k
}

// SetJWTKeySet replaces the in-process JWT KeySet used by the FSM apply path
// to reflect newly installed/demoted keys into memory. Must be called before
// the raft log starts replaying. Passing nil resets to the internal default.
func (f *MetaFSM) SetJWTKeySet(ks *iamjwt.KeySet) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ks == nil {
		f.jwtKeys = iamjwt.NewKeySet()
	} else {
		f.jwtKeys = ks
	}
}

// JWTKeySet returns the KeySet currently wired into the FSM.
// It is always non-nil (NewMetaFSM seeds a default KeySet).
// Callers should not modify the returned value directly; use SetJWTKeySet.
func (f *MetaFSM) JWTKeySet() *iamjwt.KeySet {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.jwtKeys
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

// dekRefCount returns the number of ObjectIndexEntry records that reference
// the given DEK generation. Returns 0 if the generation has no entries.
func (f *MetaFSM) dekRefCount(gen uint32) uint64 {
	return f.dekRefCounts[gen]
}

// incDEKRef increments the ref count for the given DEK generation.
// Must be called with f.mu held.
func (f *MetaFSM) incDEKRef(gen uint32) {
	f.dekRefCounts[gen]++
}

// decDEKRef decrements the ref count for the given DEK generation.
// Clamps at zero to guard against double-decrement on buggy replay.
// Must be called with f.mu held.
func (f *MetaFSM) decDEKRef(gen uint32) {
	if f.dekRefCounts[gen] > 0 {
		f.dekRefCounts[gen]--
		if f.dekRefCounts[gen] == 0 {
			delete(f.dekRefCounts, gen)
		}
	}
}

// PendingDEKVersions returns the DEK versions decoded during the last Restore
// call, along with the active generation. The runtime calls this after Restore
// to construct a DEKKeeper via encrypt.LoadFromFSM(kek, versions).
// Returns nil, 0 if no DKVS trailer was present in the snapshot.
func (f *MetaFSM) PendingDEKVersions() (map[uint32][]byte, uint32) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pendingDEKVersions, f.pendingDEKActive
}

// SetLifecycle wires the lifecycle store into the MetaFSM. Must be called
// before raft Start so apply does not race with replay.
func (f *MetaFSM) SetLifecycle(store *lifecycle.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lifecycleStore = store
}

// SetExportStore wires the NFS export registry store into the MetaFSM. Must be
// called before raft Start so apply does not race with replay.
func (f *MetaFSM) SetExportStore(store *nfsexport.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exportStore = store
	if f.exportFsidMajor == 0 {
		f.exportFsidMajor = 1
	}
}

// SetExportFsidMajor sets the cluster-wide fsid namespace used when the
// MetaFSM assigns fsid minors during NFS export upsert apply.
func (f *MetaFSM) SetExportFsidMajor(v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exportFsidMajor = v
}

// SetMigration wires the migration job store into the MetaFSM. Must be called
// before raft Start so apply does not race with replay.
func (f *MetaFSM) SetMigration(store *migration.JobStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.migrationStore = store
}

// Rotation returns the rotation sub-FSM. State is decoupled from the rest of
// MetaFSM and has its own RWMutex; callers can read snapshots concurrently.
func (f *MetaFSM) Rotation() *RotationFSM { return f.rotation }

// SetOnRotationApplied wires a side-effect callback fired after each rotation
// command commits. Called from the FSM apply goroutine; the callback runs disk
// I/O and transport identity swaps. Set before MetaRaft.Start().
func (f *MetaFSM) SetOnRotationApplied(fn func(RotationState)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRotationApplied = fn
}

// SetRotationSteady seeds the rotation FSM with the active SPKI on startup.
// Called by meta_raft initialization once the local PSK has been resolved.
func (f *MetaFSM) SetRotationSteady(activeSPKI [32]byte) {
	f.rotation.SetSteady(activeSPKI)
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
	case clusterpb.MetaCmdTypeCreateBucketWithPolicyAttach:
		return f.applyCreateBucketWithPolicyAttach(cmd.DataBytes())
	case clusterpb.MetaCmdTypeDEKRotate:
		if f.dekKeeper == nil {
			return nil
		}
		return f.dekKeeper.Rotate()
	case clusterpb.MetaCmdTypeDEKVersionPrune:
		if f.dekKeeper == nil {
			return nil
		}
		gen, err := decodeMetaDEKVersionPruneCmd(cmd.DataBytes())
		if err != nil {
			return fmt.Errorf("meta_fsm: DEKVersionPrune: %w", err)
		}
		safe := f.dekRefCount(gen) == 0
		return f.dekKeeper.Prune(gen, safe)
	case clusterpb.MetaCmdTypeJWTSigningKeyRotate:
		if f.dekKeeper == nil {
			return fmt.Errorf("meta_fsm: JWTSigningKeyRotate: DEK keeper not wired")
		}
		kid, wrapped, dekGen, demotedAtUnix, err := decodeMetaJWTSigningKeyRotateCmd(cmd.DataBytes())
		if err != nil {
			return fmt.Errorf("meta_fsm: JWTSigningKeyRotate: decode: %w", err)
		}
		demotedAt := time.Unix(demotedAtUnix, 0)
		// Demote old current in the persistent store
		f.jwtKeyStore.Demote(demotedAt)
		// Install new current in the persistent store
		seed := iamjwt.KeySeed{Kid: kid, WrappedSecret: wrapped, DekGen: dekGen, Role: "current"}
		f.jwtKeyStore.Put(seed)
		// Reflect into the local in-process KeySet
		f.jwtKeys.DemoteCurrentToPrevious(demotedAt)
		if err := f.jwtKeys.InstallCurrent(seed, f.dekKeeper); err != nil {
			return fmt.Errorf("meta_fsm: JWTSigningKeyRotate: install jwt key locally: %w", err)
		}
		return nil
	case clusterpb.MetaCmdTypeJWTSigningKeyPrune:
		pruneAtUnix, err := decodeMetaJWTSigningKeyPruneCmd(cmd.DataBytes())
		if err != nil {
			return fmt.Errorf("meta_fsm: JWTSigningKeyPrune: decode: %w", err)
		}
		pruneAt := time.Unix(pruneAtUnix, 0)
		if !f.jwtKeyStore.PrunePrevSafe(pruneAt) {
			return iamjwt.ErrPrunePrev
		}
		f.jwtKeyStore.RemovePrev()
		_ = f.jwtKeys.Prune(true)
		return nil
	default:
		metrics.UnknownMetaCmdTotal.WithLabelValues(strconv.Itoa(int(cmd.Type()))).Inc()
		log.Warn().Stringer("type", cmd.Type()).Msg("meta_fsm: unknown command type, ignoring")
		return nil
	}
}

// applyIAM dispatches an IAM command to the configured iam.Applier. Returns
// an error if IAM was not wired (Phase 1: IAM defaults nil, set via SetIAM).
func (f *MetaFSM) applyIAM(payload []byte, fn func(*iam.Applier, []byte) error) error {
	if f.iamApplier == nil {
		return fmt.Errorf("meta_fsm: IAM applier not configured")
	}
	return fn(f.iamApplier, payload)
}

func (f *MetaFSM) applyBucketLifecyclePut(payload []byte) error {
	if f.lifecycleStore == nil {
		return fmt.Errorf("meta_fsm: lifecycle store not wired")
	}
	bucket, raw, err := lifecycle.DecodePutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketLifecyclePut: %w", err)
	}
	return f.lifecycleStore.PutRaw(bucket, raw)
}

func (f *MetaFSM) applyBucketLifecycleDelete(payload []byte) error {
	if f.lifecycleStore == nil {
		return fmt.Errorf("meta_fsm: lifecycle store not wired")
	}
	bucket, err := lifecycle.DecodeDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketLifecycleDelete: %w", err)
	}
	return f.lifecycleStore.Delete(bucket)
}

func (f *MetaFSM) applyNfsExportUpsert(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, cfg, err := nfsexport.DecodeUpsertPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportUpsert: %w", err)
	}
	if cfg.FsidMinor != 0 || cfg.Generation != 0 {
		if cfg.FsidMajor == 0 {
			cfg.FsidMajor = f.exportFsidMajor
		}
		if err := f.exportStore.Put(bucket, cfg); err != nil {
			return err
		}
	} else {
		if _, err := f.exportStore.ApplyUpsert(bucket, cfg.ReadOnly, f.exportFsidMajor); err != nil {
			return err
		}
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyNfsExportCreate(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, cfg, err := nfsexport.DecodeUpsertPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportCreate: %w", err)
	}
	if _, err := f.exportStore.ApplyCreate(bucket, cfg.ReadOnly, f.exportFsidMajor); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyNfsExportDelete(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, err := nfsexport.DecodeDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportDelete: %w", err)
	}
	if err := f.exportStore.Delete(bucket); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyNfsExportBucketDeleteCascade(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, _, err := nfsexport.DecodeBucketDeleteCascadePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportBucketDeleteCascade: %w", err)
	}
	if err := f.exportStore.Delete(bucket); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}

func (f *MetaFSM) applyMigrationJobStart(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	bucket, startedAt, err := migration.DecodeJobStartPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobStart: %w", err)
	}
	ts := time.Unix(0, startedAt)
	return f.migrationStore.SaveJob(&migration.JobState{
		Bucket:    bucket,
		Status:    migration.StatusRunning,
		StartedAt: ts,
		UpdatedAt: ts,
	})
}

func (f *MetaFSM) applyMigrationJobDone(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	bucket, copied, errors, updatedAt, err := migration.DecodeJobDonePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobDone: %w", err)
	}
	job, err := f.migrationStore.GetJob(bucket)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobDone: get job: %w", err)
	}
	if job == nil {
		job = &migration.JobState{Bucket: bucket}
	}
	job.Status = migration.StatusComplete
	job.Copied = copied
	job.Errors = errors
	job.UpdatedAt = time.Unix(0, updatedAt)
	return f.migrationStore.SaveJob(job)
}

func (f *MetaFSM) applyMigrationJobFailed(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	bucket, reason, errors, updatedAt, err := migration.DecodeJobFailedPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobFailed: %w", err)
	}
	job, err := f.migrationStore.GetJob(bucket)
	if err != nil {
		return fmt.Errorf("meta_fsm: MigrationJobFailed: get job: %w", err)
	}
	if job == nil {
		job = &migration.JobState{Bucket: bucket}
	}
	job.Status = migration.StatusFailed
	job.Reason = reason
	job.Errors = errors
	job.UpdatedAt = time.Unix(0, updatedAt)
	return f.migrationStore.SaveJob(job)
}

func (f *MetaFSM) ActiveFeatures() compat.ActiveFeatures {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.activeFeatures
}

func (f *MetaFSM) CapabilityEvidence(nodeID string, now time.Time) compat.Evidence {
	f.mu.RLock()
	defer f.mu.RUnlock()
	caps := map[string]bool{}
	if f.iamApplier != nil && f.migrationStore != nil {
		caps[compat.CapabilityMigrationCutoverV1] = true
	}
	if f.exportStore != nil {
		caps[compat.CapabilityNfsExportCreateV1] = true
	}
	caps[compat.CapabilityMultipartListingV1] = true
	return compat.Evidence{
		NodeID:       compat.NodeID(nodeID),
		Capabilities: caps,
		LastSeen:     now,
		Ready:        true,
	}
}

func (f *MetaFSM) applyCapabilityActivate(payload []byte) error {
	cmd := clusterpb.GetRootAsMetaCapabilityActivateCmd(payload, 0)
	capability := string(cmd.Capability())
	if capability == "" {
		return fmt.Errorf("meta_fsm: CapabilityActivate missing capability")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeFeatures = f.activeFeatures.With(capability)
	return nil
}

func (f *MetaFSM) applyMigrationCutover(payload []byte) error {
	if f.migrationStore == nil {
		return fmt.Errorf("meta_fsm: migration store not wired")
	}
	if f.iamApplier == nil {
		return fmt.Errorf("meta_fsm: IAM applier not configured")
	}
	cmd := clusterpb.GetRootAsMetaMigrationCutoverCmd(payload, 0)
	bucket := string(cmd.Bucket())
	if bucket == "" {
		return fmt.Errorf("meta_fsm: MigrationCutover missing bucket")
	}
	if err := f.iamApplier.ApplyBucketUpstreamStatusSet(bucket, iam.BucketUpstreamStatusCutover); err != nil {
		return fmt.Errorf("meta_fsm: MigrationCutover upstream status: %w", err)
	}
	f.mu.Lock()
	f.activeFeatures = f.activeFeatures.With(compat.CapabilityMigrationCutoverV1)
	f.mu.Unlock()
	return f.migrationStore.SaveJob(&migration.JobState{
		Bucket:    bucket,
		Status:    migration.StatusComplete,
		UpdatedAt: time.Unix(0, cmd.UpdatedAtUnixNs()),
	})
}

func (f *MetaFSM) applyConfigPut(payload []byte) error {
	if f.cfgStore == nil {
		return nil // safe no-op until wired
	}
	key, value, err := decodeMetaConfigPutCmd(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: ConfigPut: %w", err)
	}
	return f.cfgStore.Set(context.Background(), key, value)
}

func (f *MetaFSM) applyConfigDelete(payload []byte) error {
	if f.cfgStore == nil {
		return nil // safe no-op until wired
	}
	key, err := decodeMetaConfigDeleteCmd(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: ConfigDelete: %w", err)
	}
	return f.cfgStore.Unset(context.Background(), key)
}

func (f *MetaFSM) applyPolicyPut(payload []byte) error {
	if f.policyStore == nil {
		return nil // safe no-op until wired
	}
	name, docJSON, builtin, err := DecodePolicyPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyPut: %w", err)
	}
	if err := f.policyStore.Put(context.Background(), name, docJSON, builtin); err != nil {
		return fmt.Errorf("meta_fsm: PolicyPut store: %w", err)
	}
	if f.policyResolver != nil {
		// A policy doc body change can affect any cached entry that references it;
		// invalidate the entire cache (passing both nil slices nukes all entries).
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyDelete(payload []byte) error {
	if f.policyStore == nil {
		return nil // safe no-op until wired
	}
	name, err := DecodePolicyDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyDelete: %w", err)
	}
	if err := f.policyStore.Delete(context.Background(), name); err != nil {
		return fmt.Errorf("meta_fsm: PolicyDelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupPut(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	name, policies, err := DecodeGroupPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupPut: %w", err)
	}
	if err := f.groupStore.Put(context.Background(), name, policies); err != nil {
		return fmt.Errorf("meta_fsm: GroupPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Group policy attachment changes can affect any cached entry; nuke all.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupDelete(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	name, err := DecodeGroupDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupDelete: %w", err)
	}
	if err := f.groupStore.Delete(context.Background(), name); err != nil {
		return fmt.Errorf("meta_fsm: GroupDelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupMemberPut(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	grp, saID, err := DecodeGroupMemberPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberPut: %w", err)
	}
	if err := f.groupStore.AddMember(context.Background(), grp, saID); err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Only the affected SA's cached entries need to be dropped.
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyGroupMemberDelete(payload []byte) error {
	if f.groupStore == nil {
		return nil // safe no-op until wired
	}
	grp, saID, err := DecodeGroupMemberDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberDelete: %w", err)
	}
	if err := f.groupStore.RemoveMember(context.Background(), grp, saID); err != nil {
		return fmt.Errorf("meta_fsm: GroupMemberDelete store: %w", err)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToSAPut(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	saID, pol, err := DecodePolicyAttachToSAPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSAPut: %w", err)
	}
	if err := f.policyAttachStore.AttachToSA(context.Background(), saID, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSAPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Only the affected SA's cached entries need to be dropped.
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToSADelete(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	saID, pol, err := DecodePolicyAttachToSADeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSADelete: %w", err)
	}
	if err := f.policyAttachStore.DetachFromSA(context.Background(), saID, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToSADelete store: %w", err)
	}
	if f.policyResolver != nil {
		// Only the affected SA's cached entries need to be dropped.
		f.policyResolver.Invalidate([]string{saID}, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToGroupPut(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	grp, pol, err := DecodePolicyAttachToGroupPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupPut: %w", err)
	}
	if err := f.policyAttachStore.AttachToGroup(context.Background(), grp, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupPut store: %w", err)
	}
	if f.policyResolver != nil {
		// TODO(opt): nuke only SAs that are members of grp once we can enumerate
		// them cheaply from this apply path. For now a nuclear invalidate is safe
		// and cache rebuild is cheap.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyPolicyAttachToGroupDelete(payload []byte) error {
	if f.policyAttachStore == nil {
		return nil // safe no-op until wired
	}
	grp, pol, err := DecodePolicyAttachToGroupDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupDelete: %w", err)
	}
	if err := f.policyAttachStore.DetachFromGroup(context.Background(), grp, pol); err != nil {
		return fmt.Errorf("meta_fsm: PolicyAttachToGroupDelete store: %w", err)
	}
	if f.policyResolver != nil {
		// TODO(opt): nuke only SAs that are members of grp once we can enumerate
		// them cheaply from this apply path. For now a nuclear invalidate is safe
		// and cache rebuild is cheap.
		f.policyResolver.Invalidate(nil, nil)
	}
	return nil
}

func (f *MetaFSM) applyBucketPolicyPut(payload []byte) error {
	if f.bucketPolicyStore == nil {
		return nil // safe no-op until wired
	}
	bucket, docJSON, err := DecodeBucketPolicyPutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyPut: %w", err)
	}
	if reservedname.IsInternalBucket(bucket) {
		return fmt.Errorf("meta_fsm: BucketPolicyPut: bucket %q is internal and cannot receive policy mutations via public API", bucket)
	}
	if err := f.bucketPolicyStore.Put(context.Background(), bucket, docJSON); err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyPut store: %w", err)
	}
	if f.policyResolver != nil {
		// Only cache entries for this bucket are stale.
		f.policyResolver.Invalidate(nil, []string{bucket})
	}
	return nil
}

func (f *MetaFSM) applyBucketPolicyDelete(payload []byte) error {
	if f.bucketPolicyStore == nil {
		return nil // safe no-op until wired
	}
	bucket, err := DecodeBucketPolicyDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyDelete: %w", err)
	}
	if reservedname.IsInternalBucket(bucket) {
		return fmt.Errorf("meta_fsm: BucketPolicyDelete: bucket %q is internal and cannot receive policy mutations via public API", bucket)
	}
	if err := f.bucketPolicyStore.Delete(context.Background(), bucket); err != nil {
		return fmt.Errorf("meta_fsm: BucketPolicyDelete store: %w", err)
	}
	if f.policyResolver != nil {
		// Only cache entries for this bucket are stale.
		f.policyResolver.Invalidate(nil, []string{bucket})
	}
	return nil
}

// applyCreateBucketWithPolicyAttach handles MetaCmd 62 — the IAM half of the
// sequenced bucket-create + policy-attach operation (D#13, F#2).
//
// Approach: sequenced (not cross-FSM atomic). The bucket itself is created by
// the data-plane FSM via the existing CreateBucket path; this MetaCmd only
// handles the IAM side: validate SA + policy existence, then attach the policy
// to the SA. The admin handler is responsible for rolling back via DeleteBucket
// if this propose fails.
//
// If both attach_sa and attach_policy are empty, this is a no-op (create-only
// caller path; the bucket was already created by the prior CreateBucket propose).
func (f *MetaFSM) applyCreateBucketWithPolicyAttach(payload []byte) error {
	bucket, sa, pol, err := decodeMetaCreateBucketWithPolicyAttachCmd(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach decode: %w", err)
	}
	if sa == "" {
		// create-only path: no IAM half to apply.
		return nil
	}
	// F#2: validate SA existence before any mutation.
	if f.iamApplier == nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: iam applier not configured")
	}
	if !f.iamApplier.SAExists(sa) {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: SA %q does not exist (F#2)", sa)
	}
	// F#2: validate policy existence before any mutation.
	if f.policyStore == nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: policy store not configured")
	}
	if _, perr := f.policyStore.GetRaw(context.Background(), pol); perr != nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: policy %q does not exist: %w", pol, perr)
	}
	// Attach policy to SA.
	if f.policyAttachStore == nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: policy attach store not configured")
	}
	if attachErr := f.policyAttachStore.AttachToSA(context.Background(), sa, pol); attachErr != nil {
		return fmt.Errorf("meta_fsm: CreateBucketWithPolicyAttach: attach: %w", attachErr)
	}
	if f.policyResolver != nil {
		f.policyResolver.Invalidate([]string{sa}, []string{bucket})
	}
	return nil
}

func (f *MetaFSM) applyAddNode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: AddNode: empty payload")
	}
	var (
		c      *clusterpb.MetaAddNodeCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaAddNodeCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaAddNodeCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	node := c.Node(nil)
	if node == nil {
		return fmt.Errorf("meta_fsm: AddNode: nil node")
	}
	entry := MetaNodeEntry{
		ID:      string(node.Id()),
		Address: string(node.Address()),
		Role:    node.Role(),
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: AddNode: empty node ID")
	}
	f.mu.Lock()
	f.nodes[entry.ID] = entry
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyRemoveNode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: RemoveNode: empty payload")
	}
	var (
		c      *clusterpb.MetaRemoveNodeCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaRemoveNodeCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaRemoveNodeCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	nodeID := string(c.NodeId())
	f.mu.Lock()
	delete(f.nodes, nodeID)
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyPutShardGroup(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutShardGroup: empty payload")
	}
	var (
		c      *clusterpb.MetaPutShardGroupCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutShardGroupCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutShardGroupCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	sg := c.Group(nil)
	if sg == nil {
		return fmt.Errorf("meta_fsm: PutShardGroup: nil group")
	}
	peers := make([]string, sg.PeerIdsLength())
	for i := 0; i < sg.PeerIdsLength(); i++ {
		peers[i] = string(sg.PeerIds(i))
	}
	entry := ShardGroupEntry{
		ID:      string(sg.Id()),
		PeerIDs: peers,
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: PutShardGroup: empty group ID")
	}
	// Reserved-namespace check. apply runs on log replay too; warn-and-skip
	// (rather than error-and-crash) so an old log entry containing a name
	// that became reserved later doesn't poison startup. Proposals are
	// rejected upstream in MetaRaft.ProposeShardGroup.
	if err := raft.ValidateGroupID(entry.ID); err != nil {
		log.Warn().Err(err).Str("group_id", entry.ID).Msg("meta_fsm: PutShardGroup: rejecting reserved group ID; entry will not be applied")
		return nil
	}
	if len(peers) == 0 {
		return fmt.Errorf("meta_fsm: PutShardGroup: group %q has no peers", entry.ID)
	}
	f.mu.Lock()
	f.shardGroups[entry.ID] = entry
	cbPeers := f.normalizeShardGroupPeersLocked(entry.PeerIDs)
	cb := f.onShardGroupAdded
	f.mu.Unlock()
	if cb != nil {
		// Defensive copy of peers — callback may keep references.
		cb(ShardGroupEntry{ID: entry.ID, PeerIDs: cbPeers})
	}
	return nil
}

func (f *MetaFSM) applyPutBucketAssignment(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty payload")
	}
	var (
		c      *clusterpb.MetaPutBucketAssignmentCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutBucketAssignmentCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutBucketAssignmentCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	entry := c.Entry(nil)
	if entry == nil {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: nil entry")
	}
	bucket := string(entry.Bucket())
	groupID := string(entry.GroupId())
	if bucket == "" {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty bucket")
	}
	if groupID == "" {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty groupID")
	}

	f.mu.Lock()
	f.bucketAssignments[bucket] = groupID
	cb := f.onBucketAssigned
	f.mu.Unlock()

	if cb != nil {
		cb(bucket, groupID)
	}
	return nil
}

type metaPutObjectIndexCmd struct {
	Entry          ObjectIndexEntry
	PreserveLatest bool
}

type objectIndexSnapshotEntry struct {
	ObjectIndexEntry
	IsLatest bool
}

func objectIndexLatestKey(bucket, key string) string {
	return bucket + "\x00" + key
}

func objectIndexVersionKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

func (f *MetaFSM) applyPutObjectIndex(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutObjectIndex: empty payload")
	}
	c, err := decodeMetaPutObjectIndexCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: PutObjectIndex: %w", err)
	}
	e := c.Entry
	if e.Bucket == "" || e.Key == "" || e.VersionID == "" {
		return fmt.Errorf("meta_fsm: PutObjectIndex: empty bucket/key/version")
	}
	if e.PlacementGroupID == "" {
		return fmt.Errorf("meta_fsm: PutObjectIndex: empty placement_group_id")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	vkey := objectIndexVersionKey(e.Bucket, e.Key, e.VersionID)
	// Overwrite: decrement ref for old entry's generation before replacing.
	if old, ok := f.objectIndex[vkey]; ok {
		f.decDEKRef(old.DekGen)
	}
	f.objectIndex[vkey] = cloneObjectIndexEntry(e)
	f.incDEKRef(e.DekGen)
	if !c.PreserveLatest {
		f.objectLatest[objectIndexLatestKey(e.Bucket, e.Key)] = e.VersionID
	}
	return nil
}

func (f *MetaFSM) applyDeleteObjectIndex(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: DeleteObjectIndex: empty payload")
	}
	bucket, key, versionID, err := decodeMetaDeleteObjectIndexCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: DeleteObjectIndex: %w", err)
	}
	if bucket == "" || key == "" || versionID == "" {
		return fmt.Errorf("meta_fsm: DeleteObjectIndex: empty bucket/key/version")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	vkey := objectIndexVersionKey(bucket, key, versionID)
	if old, ok := f.objectIndex[vkey]; ok {
		f.decDEKRef(old.DekGen)
	}
	delete(f.objectIndex, vkey)
	lkey := objectIndexLatestKey(bucket, key)
	if f.objectLatest[lkey] != versionID {
		return nil
	}

	var latest ObjectIndexEntry
	found := false
	for _, entry := range f.objectIndex {
		if entry.Bucket != bucket || entry.Key != key {
			continue
		}
		if !found || entry.ModTime > latest.ModTime || (entry.ModTime == latest.ModTime && entry.VersionID > latest.VersionID) {
			latest = entry
			found = true
		}
	}
	if !found {
		delete(f.objectLatest, lkey)
		return nil
	}
	f.objectLatest[lkey] = latest.VersionID
	return nil
}

func (f *MetaFSM) applySetLoadSnapshot(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: SetLoadSnapshot: empty payload")
	}
	var (
		c      *clusterpb.MetaSetLoadSnapshotCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaSetLoadSnapshotCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaSetLoadSnapshotCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	newSnap := make(map[string]LoadStatEntry, c.EntriesLength())
	var e clusterpb.LoadStatEntry
	for i := 0; i < c.EntriesLength(); i++ {
		if !c.Entries(&e, i) {
			continue
		}
		entry := LoadStatEntry{
			NodeID:         string(e.NodeId()),
			DiskUsedPct:    e.DiskUsedPct(),
			DiskAvailBytes: e.DiskAvailBytes(),
			RequestsPerSec: e.RequestsPerSec(),
			UpdatedAt:      time.Unix(e.UpdatedAtUnix(), 0),
		}
		newSnap[entry.NodeID] = entry
	}
	f.mu.Lock()
	f.loadSnapshot = newSnap
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyProposeRebalancePlan(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: empty payload")
	}
	var (
		c      *clusterpb.MetaProposeRebalancePlanCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaProposeRebalancePlanCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaProposeRebalancePlanCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	p := c.Plan(nil)
	if p == nil {
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: nil plan")
	}
	plan := &RebalancePlan{
		PlanID:    string(p.PlanId()),
		GroupID:   string(p.GroupId()),
		FromNode:  string(p.FromNode()),
		ToNode:    string(p.ToNode()),
		CreatedAt: time.Unix(p.CreatedAtUnix(), 0),
	}
	if plan.PlanID == "" {
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: empty plan ID")
	}

	f.mu.Lock()
	if f.activePlan != nil {
		f.mu.Unlock()
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: active plan %q already exists", f.activePlan.PlanID)
	}
	f.activePlan = plan
	cb := f.onRebalancePlan
	f.mu.Unlock()

	if cb != nil {
		cb(plan)
	}
	return nil
}

func (f *MetaFSM) applyAbortPlan(data []byte) error {
	if len(data) == 0 {
		return nil // idempotent: empty payload treated as no-op
	}
	var (
		c      *clusterpb.MetaAbortPlanCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaAbortPlanCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaAbortPlanCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	planID := string(c.PlanId())
	reason := c.Reason()
	f.mu.Lock()
	if f.activePlan == nil || f.activePlan.PlanID != planID {
		f.mu.Unlock()
		return nil // idempotent: no-op if plan absent or ID mismatch (M5)
	}
	log.Info().Str("plan_id", planID).Str("reason", reason.String()).Msg("meta_fsm: aborting active plan")
	f.activePlan = nil
	f.mu.Unlock()
	return nil
}

// icebergNSMap returns the per-warehouse namespace map, creating it if absent.
// Caller must hold f.mu (write lock).
func (f *MetaFSM) icebergNSMap(warehouse string) map[string]IcebergNamespaceEntry {
	m := f.icebergNamespaces[warehouse]
	if m == nil {
		m = make(map[string]IcebergNamespaceEntry)
		f.icebergNamespaces[warehouse] = m
	}
	return m
}

// icebergTblMap returns the per-warehouse table map, creating it if absent.
// Caller must hold f.mu (write lock).
func (f *MetaFSM) icebergTblMap(warehouse string) map[string]IcebergTableEntry {
	m := f.icebergTables[warehouse]
	if m == nil {
		m = make(map[string]IcebergTableEntry)
		f.icebergTables[warehouse] = m
	}
	return m
}

func (f *MetaFSM) applyIcebergCreateNamespace(data []byte) error {
	c, err := decodeMetaIcebergCreateNamespaceCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCreateNamespace: %w", err)
	}
	key := icebergNamespaceKey(c.Namespace)
	var result error
	f.mu.Lock()
	wh := icebergWarehouseKey(c.Warehouse)
	nsMap := f.icebergNSMap(wh)
	if _, ok := nsMap[key]; ok {
		result = icebergcatalog.ErrNamespaceExists
	} else {
		nsMap[key] = IcebergNamespaceEntry{
			Warehouse:  wh,
			Namespace:  cloneStringSlice(c.Namespace),
			Properties: cloneStringMap(c.Properties),
		}
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergDeleteNamespace(data []byte) error {
	c, err := decodeMetaIcebergDeleteNamespaceCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergDeleteNamespace: %w", err)
	}
	key := icebergNamespaceKey(c.Namespace)
	var result error
	wh := icebergWarehouseKey(c.Warehouse)
	f.mu.Lock()
	nsMap := f.icebergNSMap(wh)
	tblMap := f.icebergTblMap(wh)
	if _, ok := nsMap[key]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else {
		prefix := key + "\x1f"
		for tableKey := range tblMap {
			if strings.HasPrefix(tableKey, prefix) {
				result = icebergcatalog.ErrNamespaceNotEmpty
				break
			}
		}
		if result == nil {
			delete(nsMap, key)
		}
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergCreateTable(data []byte) error {
	c, err := decodeMetaIcebergCreateTableCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCreateTable: %w", err)
	}
	nsKey := icebergNamespaceKey(c.Identifier.Namespace)
	tableKey := icebergTableKey(c.Identifier)
	var result error
	wh := icebergWarehouseKey(c.Warehouse)
	f.mu.Lock()
	nsMap := f.icebergNSMap(wh)
	tblMap := f.icebergTblMap(wh)
	if _, ok := nsMap[nsKey]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else if _, ok := tblMap[tableKey]; ok {
		result = icebergcatalog.ErrTableExists
	} else {
		tblMap[tableKey] = IcebergTableEntry{
			Warehouse:        wh,
			Identifier:       cloneIcebergIdent(c.Identifier),
			MetadataLocation: c.MetadataLocation,
			Properties:       cloneStringMap(c.Properties),
		}
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergCommitTable(data []byte) error {
	c, err := decodeMetaIcebergCommitTableCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCommitTable: %w", err)
	}
	tableKey := icebergTableKey(c.Identifier)
	var result error
	f.mu.Lock()
	tblMap := f.icebergTblMap(icebergWarehouseKey(c.Warehouse))
	entry, ok := tblMap[tableKey]
	if !ok {
		result = icebergcatalog.ErrTableNotFound
	} else if entry.MetadataLocation != c.ExpectedMetadataLocation {
		result = icebergcatalog.ErrCommitFailed
	} else {
		entry.MetadataLocation = c.NewMetadataLocation
		tblMap[tableKey] = entry
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergDeleteTable(data []byte) error {
	c, err := decodeMetaIcebergDeleteTableCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergDeleteTable: %w", err)
	}
	nsKey := icebergNamespaceKey(c.Identifier.Namespace)
	tableKey := icebergTableKey(c.Identifier)
	var result error
	wh2 := icebergWarehouseKey(c.Warehouse)
	f.mu.Lock()
	nsMap := f.icebergNSMap(wh2)
	tblMap := f.icebergTblMap(wh2)
	if _, ok := nsMap[nsKey]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else if _, ok := tblMap[tableKey]; !ok {
		result = icebergcatalog.ErrTableNotFound
	} else {
		delete(tblMap, tableKey)
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

// applyRotateKeyBegin commits phase 1 → 2 transition. The rotation FSM
// validates capabilities, idempotency, and phase preconditions; on success
// the side-effect callback is invoked with the new state so the worker can
// load keys.d/next.key, verify SPKI, and swap the transport accept set.
func (f *MetaFSM) applyRotateKeyBegin(data []byte) error {
	c, err := decodeMetaRotateKeyBeginCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeyBegin: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		// FSM rejected (capability missing, conflicting rotation in progress).
		// Log but do not crash apply loop — followers must converge with leader.
		log.Warn().Err(err).Msg("meta_fsm: RotateKeyBegin rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

func (f *MetaFSM) applyRotateKeySwitch(data []byte) error {
	c, err := decodeMetaRotateKeySwitchCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeySwitch: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		log.Warn().Err(err).Msg("meta_fsm: RotateKeySwitch rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

func (f *MetaFSM) applyRotateKeyDrop(data []byte) error {
	c, err := decodeMetaRotateKeyDropCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeyDrop: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		log.Warn().Err(err).Msg("meta_fsm: RotateKeyDrop rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

func (f *MetaFSM) applyRotateKeyAbort(data []byte) error {
	c, err := decodeMetaRotateKeyAbortCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeyAbort: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		log.Warn().Err(err).Msg("meta_fsm: RotateKeyAbort rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

// fireRotationApplied snapshots state and invokes the callback outside any
// FSM lock. The callback (RotationWorker.OnPhaseChange) does disk I/O and
// transport mutation — must not run under MetaFSM.mu.
func (f *MetaFSM) fireRotationApplied() {
	f.mu.RLock()
	cb := f.onRotationApplied
	f.mu.RUnlock()
	if cb == nil {
		return
	}
	cb(f.rotation.State())
}

// LoadSnapshot returns a copy of the current per-node load statistics.
func (f *MetaFSM) LoadSnapshot() map[string]LoadStatEntry {
	f.mu.RLock()
	out := make(map[string]LoadStatEntry, len(f.loadSnapshot))
	for k, v := range f.loadSnapshot {
		out[k] = v
	}
	f.mu.RUnlock()
	return out
}

// ActivePlanID returns the plan ID of the currently active rebalance plan, or "".
func (f *MetaFSM) ActivePlanID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.activePlan == nil {
		return ""
	}
	return f.activePlan.PlanID
}

// ActivePlan returns a copy of the currently active rebalance plan, or nil.
func (f *MetaFSM) ActivePlan() *RebalancePlan {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.activePlan == nil {
		return nil
	}
	cp := *f.activePlan
	return &cp
}

// SetOnRebalancePlan registers a callback fired after each ProposeRebalancePlan is applied.
// The callback must not block; it is called with f.mu released.
// Must be called before MetaRaft.Start() to avoid a data race with the apply loop.
func (f *MetaFSM) SetOnRebalancePlan(fn func(*RebalancePlan)) {
	f.mu.Lock()
	f.onRebalancePlan = fn
	f.mu.Unlock()
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

// SetOnNfsExportChange registers a callback fired after each NFS export
// registry upsert/delete is applied. The callback must not block.
func (f *MetaFSM) SetOnNfsExportChange(fn func()) {
	f.mu.Lock()
	f.onNfsExportChange = fn
	f.mu.Unlock()
}

func (f *MetaFSM) publishNfsExportChange() {
	f.mu.RLock()
	cb := f.onNfsExportChange
	f.mu.RUnlock()
	if cb != nil {
		cb()
	}
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

// BucketAssignments returns a copy of the current bucket→group_id map.
func (f *MetaFSM) BucketAssignments() map[string]string {
	f.mu.RLock()
	out := make(map[string]string, len(f.bucketAssignments))
	for k, v := range f.bucketAssignments {
		out[k] = v
	}
	f.mu.RUnlock()
	return out
}

// HasUserData reports whether the FSM holds any user-created buckets.
// Used by the join handler to guard against accidental data loss.
func (f *MetaFSM) HasUserData() bool {
	f.mu.RLock()
	has := len(f.bucketAssignments) > 0
	f.mu.RUnlock()
	return has
}

func (f *MetaFSM) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	versionID, ok := f.objectLatest[objectIndexLatestKey(bucket, key)]
	if !ok {
		return ObjectIndexEntry{}, false
	}
	entry, ok := f.objectIndex[objectIndexVersionKey(bucket, key, versionID)]
	if !ok {
		return ObjectIndexEntry{}, false
	}
	return cloneObjectIndexEntry(entry), true
}

func (f *MetaFSM) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, ok := f.objectIndex[objectIndexVersionKey(bucket, key, versionID)]
	if !ok {
		return ObjectIndexEntry{}, false
	}
	return cloneObjectIndexEntry(entry), true
}

func (f *MetaFSM) ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	entries, _ := f.ObjectIndexLatestEntriesPage(bucket, prefix, "", maxKeys)
	return entries
}

// ObjectIndexLatestEntriesPage returns objects ordered by key for a single
// pagination page. Entries whose key is greater than `marker` (excluding the
// marker itself) up to `maxKeys` results are returned. `truncated` reports
// whether more entries match beyond the returned slice — callers use it to
// emit S3's IsTruncated/NextMarker fields. `maxKeys <= 0` disables the cap
// (used by WalkObjects-style callers that want every match).
func (f *MetaFSM) ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) (entries []ObjectIndexEntry, truncated bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries = make([]ObjectIndexEntry, 0)
	for lkey, versionID := range f.objectLatest {
		parts := strings.SplitN(lkey, "\x00", 2)
		if len(parts) != 2 || parts[0] != bucket || !strings.HasPrefix(parts[1], prefix) {
			continue
		}
		if marker != "" && parts[1] <= marker {
			continue
		}
		entry, ok := f.objectIndex[objectIndexVersionKey(bucket, parts[1], versionID)]
		if !ok || entry.IsDeleteMarker {
			continue
		}
		entries = append(entries, cloneObjectIndexEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	if maxKeys > 0 && len(entries) > maxKeys {
		entries = entries[:maxKeys]
		truncated = true
	}
	return entries, truncated
}

func (f *MetaFSM) ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries := make([]ObjectIndexEntry, 0)
	for _, entry := range f.objectIndex {
		if entry.Bucket != bucket || !strings.HasPrefix(entry.Key, prefix) {
			continue
		}
		entries = append(entries, cloneObjectIndexEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key != entries[j].Key {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].VersionID > entries[j].VersionID
	})
	if maxKeys > 0 && len(entries) > maxKeys {
		entries = entries[:maxKeys]
	}
	return entries
}

func (f *MetaFSM) ObjectIndexSummary(bucket string) ObjectIndexSummary {
	f.mu.RLock()
	defer f.mu.RUnlock()
	counts := make(map[string]int)
	for _, entry := range f.objectIndex {
		if bucket != "" && entry.Bucket != bucket {
			continue
		}
		counts[entry.PlacementGroupID]++
	}
	return ObjectIndexSummary{
		Bucket:               bucket,
		PlacementGroupCounts: counts,
	}
}

func (f *MetaFSM) PlacementReport(bucket, key string, maxRows int) PlacementReport {
	f.mu.RLock()
	defer f.mu.RUnlock()

	groups := make(map[string]ShardGroupEntry, len(f.shardGroups))
	for id, sg := range f.shardGroups {
		groups[id] = ShardGroupEntry{ID: sg.ID, PeerIDs: f.normalizeShardGroupPeersLocked(sg.PeerIDs)}
	}

	entries := make([]ObjectIndexEntry, 0)
	for _, entry := range f.objectIndex {
		if bucket != "" && entry.Bucket != bucket {
			continue
		}
		if key != "" && entry.Key != key {
			continue
		}
		if entry.IsDeleteMarker {
			continue
		}
		entries = append(entries, cloneObjectIndexEntry(entry))
	}
	return BuildPlacementReport(entries, groups, PlacementReportOptions{
		Bucket:  bucket,
		Key:     key,
		MaxRows: maxRows,
	})
}

// ShardGroups returns a deep copy of current shard groups.
// PeerIDs slices are copied so callers cannot mutate FSM state.
func (f *MetaFSM) ShardGroups() []ShardGroupEntry {
	f.mu.RLock()
	out := make([]ShardGroupEntry, 0, len(f.shardGroups))
	for _, sg := range f.shardGroups {
		peers := f.normalizeShardGroupPeersLocked(sg.PeerIDs)
		out = append(out, ShardGroupEntry{ID: sg.ID, PeerIDs: peers})
	}
	f.mu.RUnlock()
	return out
}

// ShardGroup returns the entry for id and true, or zero-value and false if not found.
// Returned PeerIDs is a defensive copy.
func (f *MetaFSM) ShardGroup(id string) (ShardGroupEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	g, ok := f.shardGroups[id]
	if !ok {
		return ShardGroupEntry{}, false
	}
	peers := f.normalizeShardGroupPeersLocked(g.PeerIDs)
	return ShardGroupEntry{ID: g.ID, PeerIDs: peers}, true
}

func (f *MetaFSM) normalizeShardGroupPeersLocked(peers []string) []string {
	out := make([]string, len(peers))
	for i, peer := range peers {
		out[i] = peer
		if nodeID, ok := f.resolveNodeIDByAddressLocked(peer); ok {
			out[i] = nodeID
		}
	}
	return out
}

func (f *MetaFSM) resolveNodeIDByAddressLocked(addr string) (string, bool) {
	for _, node := range f.nodes {
		if addr == node.Address && node.ID != "" {
			return node.ID, true
		}
	}
	return "", false
}

func (f *MetaFSM) IcebergNamespace(warehouse string, namespace []string) (IcebergNamespaceEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	wh := icebergWarehouseKey(warehouse)
	nsMap := f.icebergNamespaces[wh]
	if nsMap == nil {
		return IcebergNamespaceEntry{}, false
	}
	entry, ok := nsMap[icebergNamespaceKey(namespace)]
	if !ok {
		return IcebergNamespaceEntry{}, false
	}
	return IcebergNamespaceEntry{
		Warehouse:  wh,
		Namespace:  cloneStringSlice(entry.Namespace),
		Properties: cloneStringMap(entry.Properties),
	}, true
}

func (f *MetaFSM) IcebergNamespaces(warehouse string) []IcebergNamespaceEntry {
	wh := icebergWarehouseKey(warehouse)
	f.mu.RLock()
	nsMap := f.icebergNamespaces[wh]
	out := make([]IcebergNamespaceEntry, 0, len(nsMap))
	for _, entry := range nsMap {
		out = append(out, IcebergNamespaceEntry{
			Warehouse:  wh,
			Namespace:  cloneStringSlice(entry.Namespace),
			Properties: cloneStringMap(entry.Properties),
		})
	}
	f.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return icebergNamespaceKey(out[i].Namespace) < icebergNamespaceKey(out[j].Namespace)
	})
	return out
}

func (f *MetaFSM) IcebergTable(warehouse string, ident icebergcatalog.Identifier) (IcebergTableEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	tblMap := f.icebergTables[icebergWarehouseKey(warehouse)]
	if tblMap == nil {
		return IcebergTableEntry{}, false
	}
	entry, ok := tblMap[icebergTableKey(ident)]
	if !ok {
		return IcebergTableEntry{}, false
	}
	return cloneIcebergTableEntry(entry), true
}

func (f *MetaFSM) IcebergTables(warehouse string, namespace []string) []IcebergTableEntry {
	prefix := icebergNamespaceKey(namespace) + "\x1f"
	f.mu.RLock()
	tblMap := f.icebergTables[icebergWarehouseKey(warehouse)]
	out := make([]IcebergTableEntry, 0)
	for key, entry := range tblMap {
		if strings.HasPrefix(key, prefix) {
			out = append(out, cloneIcebergTableEntry(entry))
		}
	}
	f.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].Identifier.Name < out[j].Identifier.Name
	})
	return out
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
	for _, entry := range f.objectIndex {
		latestVersionID := f.objectLatest[objectIndexLatestKey(entry.Bucket, entry.Key)]
		objectEntries = append(objectEntries, objectIndexSnapshotEntry{
			ObjectIndexEntry: cloneObjectIndexEntry(entry),
			IsLatest:         latestVersionID == entry.VersionID,
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

	// Append IAM section as a trailer AFTER the FlatBuffer root. Older
	// readers that pre-date this trailer ignore everything past the FB root
	// (FlatBuffer Go runtime walks from the root and tolerates trailing
	// bytes); newer readers detect the trailer via the magic footer.
	//
	// Layout: [FB bytes][iam bytes][u32 iam_len][u32 magic].
	// iam_len == 0 is valid (e.g., MetaFSM with no IAM applier wired).
	var iamBytes []byte
	if f.iamStore != nil {
		var buf bytes.Buffer
		if err := iam.WriteSnapshot(&buf, f.iamStore); err != nil {
			return nil, fmt.Errorf("meta_fsm: Snapshot: encode IAM: %w", err)
		}
		iamBytes = buf.Bytes()
	}
	out := make([]byte, 0, len(bs)+len(iamBytes)+iamSnapshotTrailerLen)
	out = append(out, bs...)
	out = append(out, iamBytes...)
	var footer [iamSnapshotTrailerLen]byte
	binary.LittleEndian.PutUint32(footer[0:4], uint32(len(iamBytes)))
	binary.LittleEndian.PutUint32(footer[4:8], iamSnapshotTrailerMagic)
	out = append(out, footer[:]...)

	// Append GCFG trailer after the IAM trailer. Only emit when there are
	// explicit config values to persist — an empty map produces no trailer so
	// old snapshots (taken from a fresh cluster with no overrides) remain
	// indistinguishable from pre-Task-10 snapshots.
	if f.cfgStore != nil {
		if cfgValues := f.cfgStore.Snapshot(); len(cfgValues) > 0 {
			cfgPayload, err := encodeMetaConfigSnapshot(cfgValues)
			if err != nil {
				return nil, fmt.Errorf("meta_fsm: Snapshot: encode config: %w", err)
			}
			var cfgFooter [cfgSnapshotTrailerLen]byte
			binary.LittleEndian.PutUint32(cfgFooter[0:4], uint32(len(cfgPayload)))
			binary.LittleEndian.PutUint32(cfgFooter[4:8], cfgSnapshotTrailerMagic)
			out = append(out, cfgPayload...)
			out = append(out, cfgFooter[:]...)
		}
	}

	// Append DKVS trailer after the GCFG trailer. Only emit when a DEKKeeper
	// is wired — absent keeper or empty versions skips the trailer for
	// forward-compat with nodes that have not yet wired a keeper.
	if f.dekKeeper != nil {
		// VersionsAndActive snapshots both fields under a single RLock so a
		// concurrent Rotate() can't insert a new gen between the two reads
		// (TOCTOU — active would reference a gen absent from versions map).
		dekVersions, dekActive := f.dekKeeper.VersionsAndActive()
		if len(dekVersions) > 0 {
			dekPayload, err := encodeMetaDEKVersionSnapshot(dekVersions, dekActive, dekRefCountsCopy)
			if err != nil {
				return nil, fmt.Errorf("meta_fsm: Snapshot: encode DEK versions: %w", err)
			}
			var dekFooter [dekSnapshotTrailerLen]byte
			binary.LittleEndian.PutUint32(dekFooter[0:4], uint32(len(dekPayload)))
			binary.LittleEndian.PutUint32(dekFooter[4:8], dekSnapshotTrailerMagic)
			out = append(out, dekPayload...)
			out = append(out, dekFooter[:]...)
		}
	}

	// Append IPST trailer after the DKVS trailer. Emit when any of the 4 §2
	// policy stores is wired. An empty payload (all stores nil or all empty)
	// is still emitted so the trailer presence signals "new-format snapshot"
	// to Restore, which can safely skip decode on an empty payload. Only
	// skip the trailer entirely when all 4 stores are nil (not yet wired).
	if f.policyStore != nil || f.groupStore != nil || f.policyAttachStore != nil || f.bucketPolicyStore != nil {
		var polSnap []policystore.PolicyEntry
		if f.policyStore != nil {
			polSnap = f.policyStore.Snapshot()
		}
		var grpSnap []group.GroupEntry
		if f.groupStore != nil {
			grpSnap = f.groupStore.Snapshot()
		}
		var attachSnap policyattach.AttachSnapshot
		if f.policyAttachStore != nil {
			attachSnap = f.policyAttachStore.Snapshot()
		}
		var bpSnap []bucketpolicy.BucketPolicyEntry
		if f.bucketPolicyStore != nil {
			bpSnap = f.bucketPolicyStore.Snapshot()
		}
		ipstPayload, err := encodeMetaIAMPolicyStoresSnapshot(polSnap, grpSnap, attachSnap, bpSnap)
		if err != nil {
			return nil, fmt.Errorf("meta_fsm: Snapshot: encode IAM policy stores: %w", err)
		}
		var ipstFooter [ipstSnapshotTrailerLen]byte
		binary.LittleEndian.PutUint32(ipstFooter[0:4], uint32(len(ipstPayload)))
		binary.LittleEndian.PutUint32(ipstFooter[4:8], ipstSnapshotTrailerMagic)
		out = append(out, ipstPayload...)
		out = append(out, ipstFooter[:]...)
	}

	// Append JKEY trailer after the IPST trailer. Only emit when at least one
	// JWT signing key exists — an empty key store produces no trailer for
	// back-compat with nodes that have not yet run JWTSigningKeyRotate.
	jkeyCurrent, jkeyPrevious := f.jwtKeyStore.Snapshot()
	if jkeyCurrent != nil || jkeyPrevious != nil {
		jkeyPayload := encodeJWTKeyStore(jkeyCurrent, jkeyPrevious)
		var jkeyFooter [jkeySnapshotTrailerLen]byte
		binary.LittleEndian.PutUint32(jkeyFooter[0:4], uint32(len(jkeyPayload)))
		binary.LittleEndian.PutUint32(jkeyFooter[4:8], jkeySnapshotTrailerMagic)
		out = append(out, jkeyPayload...)
		out = append(out, jkeyFooter[:]...)
	}
	return out, nil
}

// Restore deserializes a MetaStateSnapshot and replaces current state. The
// store-meta record (meta) carries the snapshot FormatVersion; the meta-Raft
// FSM has its own in-payload versioning (FlatBuffers + IAM trailer) and accepts
// any FormatVersion for backward compatibility with pre-C2-P3 data dirs.
func (f *MetaFSM) Restore(_ raft.SnapshotMeta, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: Restore: empty snapshot")
	}

	// Peel JKEY trailer first (it is the absolute newest / outermost trailer when present).
	// Layout: [FB bytes][IAM][GCFG][DKVS][IPST][jkeyPayload][u32 jkeyLen][u32 jkeyMagic].
	// After stripping JKEY, the remaining bytes look like a pre-T35 snapshot.
	remaining := data
	var jkeyData []byte
	if len(remaining) >= jkeySnapshotTrailerLen {
		jkeyFooter := remaining[len(remaining)-jkeySnapshotTrailerLen:]
		if binary.LittleEndian.Uint32(jkeyFooter[4:8]) == jkeySnapshotTrailerMagic {
			jkeyLen := binary.LittleEndian.Uint32(jkeyFooter[0:4])
			if int(jkeyLen)+jkeySnapshotTrailerLen > len(remaining) {
				return fmt.Errorf("meta_fsm: Restore: JKEY trailer length %d exceeds snapshot size %d", jkeyLen, len(remaining))
			}
			jkeyEnd := len(remaining) - jkeySnapshotTrailerLen
			jkeyStart := jkeyEnd - int(jkeyLen)
			jkeyData = remaining[jkeyStart:jkeyEnd]
			remaining = remaining[:jkeyStart]
		}
	}

	// Peel IPST trailer next (second-newest trailer when present).
	// Layout: [FB bytes][IAM trailer][GCFG trailer][DKVS trailer][ipstPayload][u32 ipstLen][u32 ipstMagic].
	// After stripping IPST, the remaining bytes look like a pre-C1 snapshot.
	var ipstData []byte
	if len(remaining) >= ipstSnapshotTrailerLen {
		ipstFooter := remaining[len(remaining)-ipstSnapshotTrailerLen:]
		if binary.LittleEndian.Uint32(ipstFooter[4:8]) == ipstSnapshotTrailerMagic {
			ipstLen := binary.LittleEndian.Uint32(ipstFooter[0:4])
			if int(ipstLen)+ipstSnapshotTrailerLen > len(remaining) {
				return fmt.Errorf("meta_fsm: Restore: IPST trailer length %d exceeds snapshot size %d", ipstLen, len(remaining))
			}
			ipstEnd := len(remaining) - ipstSnapshotTrailerLen
			ipstStart := ipstEnd - int(ipstLen)
			ipstData = remaining[ipstStart:ipstEnd]
			remaining = remaining[:ipstStart]
		}
	}

	// Peel DKVS trailer next (second-newest trailer when present).
	// Layout: [FB bytes][IAM trailer][GCFG trailer][dekPayload][u32 dekLen][u32 dekMagic].
	// After stripping DKVS, the remaining bytes look like a pre-Task-11 snapshot.
	var dekData []byte
	if len(remaining) >= dekSnapshotTrailerLen {
		dekFooter := remaining[len(remaining)-dekSnapshotTrailerLen:]
		if binary.LittleEndian.Uint32(dekFooter[4:8]) == dekSnapshotTrailerMagic {
			dekLen := binary.LittleEndian.Uint32(dekFooter[0:4])
			if int(dekLen)+dekSnapshotTrailerLen > len(remaining) {
				return fmt.Errorf("meta_fsm: Restore: DKVS trailer length %d exceeds snapshot size %d", dekLen, len(remaining))
			}
			dekEnd := len(remaining) - dekSnapshotTrailerLen
			dekStart := dekEnd - int(dekLen)
			dekData = remaining[dekStart:dekEnd]
			remaining = remaining[:dekStart]
		}
	}

	// Peel GCFG trailer next (it is at the end after DKVS is stripped).
	// Layout: [FB bytes][IAM trailer][cfgPayload][u32 cfgLen][u32 cfgMagic].
	// After stripping GCFG, the remaining bytes look exactly like a pre-Task-10
	// snapshot, so the IAM detection below runs unmodified.
	var cfgData []byte
	if len(remaining) >= cfgSnapshotTrailerLen {
		cfgFooter := remaining[len(remaining)-cfgSnapshotTrailerLen:]
		if binary.LittleEndian.Uint32(cfgFooter[4:8]) == cfgSnapshotTrailerMagic {
			cfgLen := binary.LittleEndian.Uint32(cfgFooter[0:4])
			if int(cfgLen)+cfgSnapshotTrailerLen > len(remaining) {
				return fmt.Errorf("meta_fsm: Restore: GCFG trailer length %d exceeds snapshot size %d", cfgLen, len(remaining))
			}
			cfgEnd := len(remaining) - cfgSnapshotTrailerLen
			cfgStart := cfgEnd - int(cfgLen)
			cfgData = remaining[cfgStart:cfgEnd]
			remaining = remaining[:cfgStart]
		}
	}

	// Detect post-Phase-5d IAM trailer (footer = [u32 iam_len][u32 magic=IAMG]).
	// Layout when present: [FB bytes][iam bytes][u32 iam_len][u32 magic].
	// Legacy snapshots end at the FB root and are accepted unchanged.
	fbData := remaining
	var iamData []byte
	if len(remaining) >= iamSnapshotTrailerLen {
		footer := remaining[len(remaining)-iamSnapshotTrailerLen:]
		magic := binary.LittleEndian.Uint32(footer[4:8])
		if magic == iamSnapshotTrailerMagic {
			iamLen := binary.LittleEndian.Uint32(footer[0:4])
			if int(iamLen)+iamSnapshotTrailerLen > len(remaining) {
				return fmt.Errorf("meta_fsm: Restore: IAM trailer length %d exceeds snapshot size %d", iamLen, len(remaining))
			}
			iamEnd := len(remaining) - iamSnapshotTrailerLen
			iamStart := iamEnd - int(iamLen)
			iamData = remaining[iamStart:iamEnd]
			fbData = remaining[:iamStart]
		}
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
		snap = clusterpb.GetRootAsMetaStateSnapshot(fbData, 0)
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
	if len(iamData) > 0 {
		if f.iamStore == nil || f.iamApplier == nil {
			log.Warn().Int("iam_len", len(iamData)).Msg("meta_fsm: Restore: snapshot contains IAM section but IAM not wired; skipping IAM restore")
		} else {
			enc := f.iamApplier.Encryptor()
			if enc == nil {
				log.Warn().Msg("meta_fsm: Restore: IAM applier has no encryptor; skipping IAM restore")
			} else {
				tmp := iam.NewStore()
				if err := iam.ReadSnapshot(bytes.NewReader(iamData), tmp, enc); err != nil {
					return fmt.Errorf("meta_fsm: Restore: decode IAM: %w", err)
				}
				iamTempStore = tmp
			}
		}
	}

	// GCFG: decode config values.
	var newCfgValues map[string]string
	if len(cfgData) > 0 {
		if f.cfgStore == nil {
			log.Warn().Int("cfg_len", len(cfgData)).Msg("meta_fsm: Restore: snapshot contains config section but config store not wired; skipping")
		} else {
			values, err := decodeMetaConfigSnapshot(cfgData)
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
	if len(dekData) > 0 {
		versions, active, refs, err := decodeMetaDEKVersionSnapshot(dekData)
		if err != nil {
			return fmt.Errorf("meta_fsm: Restore: decode DEK versions: %w", err)
		}
		newDEKVersions = versions
		newDEKActive = active
		newDEKRefs = refs
		hasDEKData = true
	}

	// IPST: decode IAM policy stores snapshot.
	type ipstDecoded struct {
		polSnap    []policystore.PolicyEntry
		grpSnap    []group.GroupEntry
		attachSnap policyattach.AttachSnapshot
		bpSnap     []bucketpolicy.BucketPolicyEntry
	}
	var newIPST *ipstDecoded
	if len(ipstData) > 0 {
		if f.policyStore == nil && f.groupStore == nil && f.policyAttachStore == nil && f.bucketPolicyStore == nil {
			log.Warn().Int("ipst_len", len(ipstData)).Msg("meta_fsm: Restore: snapshot contains IPST section but no policy stores wired; skipping")
		} else {
			polSnap, grpSnap, attachSnap, bpSnap, err := decodeMetaIAMPolicyStoresSnapshot(ipstData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode IAM policy stores: %w", err)
			}
			newIPST = &ipstDecoded{polSnap, grpSnap, attachSnap, bpSnap}
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
		hasJKEYData     = len(jkeyData) > 0
	)
	if hasJKEYData {
		if f.dekKeeper == nil {
			return fmt.Errorf("meta_fsm: Restore: JKEY trailer present but DEK keeper not wired — cannot unwrap signing keys")
		}
		cur, prev, err := decodeJWTKeyStore(jkeyData)
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

	// IPST commit — apply all 4 policy stores.
	if newIPST != nil {
		// Warn per nil store. The 4 stores form a single coherent unit
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

// Nodes returns a copy of current cluster members.
func (f *MetaFSM) Nodes() []MetaNodeEntry {
	f.mu.RLock()
	out := make([]MetaNodeEntry, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, n)
	}
	f.mu.RUnlock()
	return out
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

func encodeMetaIcebergCreateNamespaceCmd(c IcebergCreateNamespaceCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	namespaceVec := buildStringVector(b, c.Namespace, clusterpb.MetaIcebergCreateNamespaceCmdStartNamespaceVector)
	propsVec := buildKeyValuePropertiesVector(b, c.Properties, clusterpb.MetaIcebergCreateNamespaceCmdStartPropertiesVector)
	clusterpb.MetaIcebergCreateNamespaceCmdStart(b)
	clusterpb.MetaIcebergCreateNamespaceCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCreateNamespaceCmdAddNamespace(b, namespaceVec)
	clusterpb.MetaIcebergCreateNamespaceCmdAddProperties(b, propsVec)
	clusterpb.MetaIcebergCreateNamespaceCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergCreateNamespaceCmdEnd(b)), nil
}

func decodeMetaIcebergCreateNamespaceCmd(data []byte) (IcebergCreateNamespaceCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergCreateNamespaceCmd {
		return clusterpb.GetRootAsMetaIcebergCreateNamespaceCmd(d, 0)
	})
	if err != nil {
		return IcebergCreateNamespaceCmd{}, err
	}
	return IcebergCreateNamespaceCmd{
		RequestID:  string(t.RequestId()),
		Warehouse:  string(t.Warehouse()),
		Namespace:  readStringVector(t.NamespaceLength(), t.Namespace),
		Properties: readKeyValueProperties(t.PropertiesLength(), t.Properties),
	}, nil
}

func encodeMetaIcebergDeleteNamespaceCmd(c IcebergDeleteNamespaceCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	namespaceVec := buildStringVector(b, c.Namespace, clusterpb.MetaIcebergDeleteNamespaceCmdStartNamespaceVector)
	clusterpb.MetaIcebergDeleteNamespaceCmdStart(b)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddNamespace(b, namespaceVec)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergDeleteNamespaceCmdEnd(b)), nil
}

func decodeMetaIcebergDeleteNamespaceCmd(data []byte) (IcebergDeleteNamespaceCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergDeleteNamespaceCmd {
		return clusterpb.GetRootAsMetaIcebergDeleteNamespaceCmd(d, 0)
	})
	if err != nil {
		return IcebergDeleteNamespaceCmd{}, err
	}
	return IcebergDeleteNamespaceCmd{
		RequestID: string(t.RequestId()),
		Warehouse: string(t.Warehouse()),
		Namespace: readStringVector(t.NamespaceLength(), t.Namespace),
	}, nil
}

func encodeMetaIcebergCreateTableCmd(c IcebergCreateTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	locationOff := b.CreateString(c.MetadataLocation)
	propsVec := buildKeyValuePropertiesVector(b, c.Properties, clusterpb.MetaIcebergCreateTableCmdStartPropertiesVector)
	clusterpb.MetaIcebergCreateTableCmdStart(b)
	clusterpb.MetaIcebergCreateTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCreateTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergCreateTableCmdAddMetadataLocation(b, locationOff)
	clusterpb.MetaIcebergCreateTableCmdAddProperties(b, propsVec)
	clusterpb.MetaIcebergCreateTableCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergCreateTableCmdEnd(b)), nil
}

func decodeMetaIcebergCreateTableCmd(data []byte) (IcebergCreateTableCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergCreateTableCmd {
		return clusterpb.GetRootAsMetaIcebergCreateTableCmd(d, 0)
	})
	if err != nil {
		return IcebergCreateTableCmd{}, err
	}
	ident, err := readIcebergIdentifier(t.Identifier(nil))
	if err != nil {
		return IcebergCreateTableCmd{}, err
	}
	return IcebergCreateTableCmd{
		RequestID:        string(t.RequestId()),
		Warehouse:        string(t.Warehouse()),
		Identifier:       ident,
		MetadataLocation: string(t.MetadataLocation()),
		Properties:       readKeyValueProperties(t.PropertiesLength(), t.Properties),
	}, nil
}

func encodeMetaIcebergCommitTableCmd(c IcebergCommitTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	expectedOff := b.CreateString(c.ExpectedMetadataLocation)
	nextOff := b.CreateString(c.NewMetadataLocation)
	clusterpb.MetaIcebergCommitTableCmdStart(b)
	clusterpb.MetaIcebergCommitTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCommitTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergCommitTableCmdAddExpectedMetadataLocation(b, expectedOff)
	clusterpb.MetaIcebergCommitTableCmdAddNewMetadataLocation(b, nextOff)
	clusterpb.MetaIcebergCommitTableCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergCommitTableCmdEnd(b)), nil
}

func decodeMetaIcebergCommitTableCmd(data []byte) (IcebergCommitTableCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergCommitTableCmd {
		return clusterpb.GetRootAsMetaIcebergCommitTableCmd(d, 0)
	})
	if err != nil {
		return IcebergCommitTableCmd{}, err
	}
	ident, err := readIcebergIdentifier(t.Identifier(nil))
	if err != nil {
		return IcebergCommitTableCmd{}, err
	}
	return IcebergCommitTableCmd{
		RequestID:                string(t.RequestId()),
		Warehouse:                string(t.Warehouse()),
		Identifier:               ident,
		ExpectedMetadataLocation: string(t.ExpectedMetadataLocation()),
		NewMetadataLocation:      string(t.NewMetadataLocation()),
	}, nil
}

func encodeMetaIcebergDeleteTableCmd(c IcebergDeleteTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	clusterpb.MetaIcebergDeleteTableCmdStart(b)
	clusterpb.MetaIcebergDeleteTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergDeleteTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergDeleteTableCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergDeleteTableCmdEnd(b)), nil
}

func decodeMetaIcebergDeleteTableCmd(data []byte) (IcebergDeleteTableCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergDeleteTableCmd {
		return clusterpb.GetRootAsMetaIcebergDeleteTableCmd(d, 0)
	})
	if err != nil {
		return IcebergDeleteTableCmd{}, err
	}
	ident, err := readIcebergIdentifier(t.Identifier(nil))
	if err != nil {
		return IcebergDeleteTableCmd{}, err
	}
	return IcebergDeleteTableCmd{
		RequestID:  string(t.RequestId()),
		Warehouse:  string(t.Warehouse()),
		Identifier: ident,
	}, nil
}

func encodeMetaPutBucketAssignmentCmd(bucket, groupID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	groupIDOff := b.CreateString(groupID)
	clusterpb.BucketAssignmentEntryStart(b)
	clusterpb.BucketAssignmentEntryAddBucket(b, bucketOff)
	clusterpb.BucketAssignmentEntryAddGroupId(b, groupIDOff)
	entryOff := clusterpb.BucketAssignmentEntryEnd(b)
	clusterpb.MetaPutBucketAssignmentCmdStart(b)
	clusterpb.MetaPutBucketAssignmentCmdAddEntry(b, entryOff)
	return fbFinish(b, clusterpb.MetaPutBucketAssignmentCmdEnd(b)), nil
}

func encodeMetaPutObjectIndexCmd(entry ObjectIndexEntry, preserveLatest bool) ([]byte, error) {
	b := clusterBuilderPool.Get()
	entryOff := buildMetaObjectIndexEntry(b, objectIndexSnapshotEntry{ObjectIndexEntry: entry})
	clusterpb.MetaPutObjectIndexCmdStart(b)
	clusterpb.MetaPutObjectIndexCmdAddEntry(b, entryOff)
	if preserveLatest {
		clusterpb.MetaPutObjectIndexCmdAddPreserveLatest(b, true)
	}
	return fbFinish(b, clusterpb.MetaPutObjectIndexCmdEnd(b)), nil
}

func decodeMetaPutObjectIndexCmd(data []byte) (metaPutObjectIndexCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPutObjectIndexCmd {
		return clusterpb.GetRootAsMetaPutObjectIndexCmd(d, 0)
	})
	if err != nil {
		return metaPutObjectIndexCmd{}, err
	}
	entry := t.Entry(nil)
	if entry == nil {
		return metaPutObjectIndexCmd{}, fmt.Errorf("nil entry")
	}
	return metaPutObjectIndexCmd{
		Entry:          readMetaObjectIndexEntry(entry),
		PreserveLatest: t.PreserveLatest(),
	}, nil
}

func encodeMetaDeleteObjectIndexCmd(bucket, key, versionID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	keyOff := b.CreateString(key)
	versionOff := b.CreateString(versionID)
	clusterpb.MetaDeleteObjectIndexCmdStart(b)
	clusterpb.MetaDeleteObjectIndexCmdAddBucket(b, bucketOff)
	clusterpb.MetaDeleteObjectIndexCmdAddKey(b, keyOff)
	clusterpb.MetaDeleteObjectIndexCmdAddVersionId(b, versionOff)
	return fbFinish(b, clusterpb.MetaDeleteObjectIndexCmdEnd(b)), nil
}

func decodeMetaDeleteObjectIndexCmd(data []byte) (bucket, key, versionID string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaDeleteObjectIndexCmd {
		return clusterpb.GetRootAsMetaDeleteObjectIndexCmd(d, 0)
	})
	if err != nil {
		return "", "", "", err
	}
	return string(t.Bucket()), string(t.Key()), string(t.VersionId()), nil
}

func buildMetaObjectIndexEntry(b *flatbuffers.Builder, entry objectIndexSnapshotEntry) flatbuffers.UOffsetT {
	e := entry.ObjectIndexEntry
	bucketOff := b.CreateString(e.Bucket)
	keyOff := b.CreateString(e.Key)
	versionOff := b.CreateString(e.VersionID)
	groupOff := b.CreateString(e.PlacementGroupID)
	contentTypeOff := b.CreateString(e.ContentType)
	etagOff := b.CreateString(e.ETag)
	var nodeIDsOff flatbuffers.UOffsetT
	if len(e.NodeIDs) > 0 {
		nodeIDsOff = buildStringVector(b, e.NodeIDs, clusterpb.MetaObjectIndexEntryStartNodeIdsVector)
	}
	// parts — build child MultipartPartEntry tables BEFORE MetaObjectIndexEntryStart.
	var partsOff flatbuffers.UOffsetT
	if len(e.Parts) > 0 {
		partOffs := make([]flatbuffers.UOffsetT, len(e.Parts))
		for i, p := range e.Parts {
			etOff := b.CreateString(p.ETag)
			clusterpb.MultipartPartEntryStart(b)
			clusterpb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
			clusterpb.MultipartPartEntryAddSize(b, p.Size)
			clusterpb.MultipartPartEntryAddEtag(b, etOff)
			partOffs[i] = clusterpb.MultipartPartEntryEnd(b)
		}
		clusterpb.MetaObjectIndexEntryStartPartsVector(b, len(partOffs))
		for i := len(partOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(partOffs[i])
		}
		partsOff = b.EndVector(len(partOffs))
	}
	clusterpb.MetaObjectIndexEntryStart(b)
	clusterpb.MetaObjectIndexEntryAddBucket(b, bucketOff)
	clusterpb.MetaObjectIndexEntryAddKey(b, keyOff)
	clusterpb.MetaObjectIndexEntryAddVersionId(b, versionOff)
	clusterpb.MetaObjectIndexEntryAddPlacementGroupId(b, groupOff)
	clusterpb.MetaObjectIndexEntryAddSize(b, e.Size)
	clusterpb.MetaObjectIndexEntryAddContentType(b, contentTypeOff)
	clusterpb.MetaObjectIndexEntryAddEtag(b, etagOff)
	clusterpb.MetaObjectIndexEntryAddModTime(b, e.ModTime)
	clusterpb.MetaObjectIndexEntryAddEcData(b, e.ECData)
	clusterpb.MetaObjectIndexEntryAddEcParity(b, e.ECParity)
	if nodeIDsOff != 0 {
		clusterpb.MetaObjectIndexEntryAddNodeIds(b, nodeIDsOff)
	}
	if e.IsDeleteMarker {
		clusterpb.MetaObjectIndexEntryAddIsDeleteMarker(b, true)
	}
	if entry.IsLatest {
		clusterpb.MetaObjectIndexEntryAddIsLatest(b, true)
	}
	if partsOff != 0 {
		clusterpb.MetaObjectIndexEntryAddParts(b, partsOff)
	}
	if e.DekGen != 0 {
		clusterpb.MetaObjectIndexEntryAddDekGen(b, e.DekGen)
	}
	return clusterpb.MetaObjectIndexEntryEnd(b)
}

func buildMetaObjectIndexEntriesVector(b *flatbuffers.Builder, entries []objectIndexSnapshotEntry) flatbuffers.UOffsetT {
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].ObjectIndexEntry, entries[j].ObjectIndexEntry
		return objectIndexVersionKey(a.Bucket, a.Key, a.VersionID) < objectIndexVersionKey(b.Bucket, b.Key, b.VersionID)
	})
	offsets := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		offsets[i] = buildMetaObjectIndexEntry(b, entries[i])
	}
	clusterpb.MetaStateSnapshotStartObjectIndexVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

func readMetaObjectIndexEntry(entry *clusterpb.MetaObjectIndexEntry) ObjectIndexEntry {
	var parts []storage.MultipartPartEntry
	if n := entry.PartsLength(); n > 0 {
		parts = make([]storage.MultipartPartEntry, n)
		var pe clusterpb.MultipartPartEntry
		for i := 0; i < n; i++ {
			if !entry.Parts(&pe, i) {
				continue
			}
			parts[i] = storage.MultipartPartEntry{
				PartNumber: int(pe.PartNumber()),
				Size:       pe.Size(),
				ETag:       string(pe.Etag()),
			}
		}
	}
	return ObjectIndexEntry{
		Bucket:           string(entry.Bucket()),
		Key:              string(entry.Key()),
		VersionID:        string(entry.VersionId()),
		PlacementGroupID: string(entry.PlacementGroupId()),
		Size:             entry.Size(),
		ContentType:      string(entry.ContentType()),
		ETag:             string(entry.Etag()),
		ModTime:          entry.ModTime(),
		ECData:           entry.EcData(),
		ECParity:         entry.EcParity(),
		NodeIDs:          readStringVector(entry.NodeIdsLength(), entry.NodeIds),
		IsDeleteMarker:   entry.IsDeleteMarker(),
		Parts:            parts,
		DekGen:           entry.DekGen(),
	}
}

func buildNfsExportEntriesVector(b *flatbuffers.Builder, entries map[string]nfsexport.Config) flatbuffers.UOffsetT {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	offsets := make([]flatbuffers.UOffsetT, len(names))
	for i := len(names) - 1; i >= 0; i-- {
		bucket := names[i]
		cfg := entries[bucket]
		bucketOff := b.CreateString(bucket)
		clusterpb.NfsExportConfigStart(b)
		clusterpb.NfsExportConfigAddReadOnly(b, cfg.ReadOnly)
		clusterpb.NfsExportConfigAddFsidMajor(b, cfg.FsidMajor)
		clusterpb.NfsExportConfigAddFsidMinor(b, cfg.FsidMinor)
		clusterpb.NfsExportConfigAddGeneration(b, cfg.Generation)
		cfgOff := clusterpb.NfsExportConfigEnd(b)
		clusterpb.NfsExportUpsertCmdStart(b)
		clusterpb.NfsExportUpsertCmdAddBucket(b, bucketOff)
		clusterpb.NfsExportUpsertCmdAddConfig(b, cfgOff)
		offsets[i] = clusterpb.NfsExportUpsertCmdEnd(b)
	}
	clusterpb.MetaStateSnapshotStartNfsExportsVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

func encodeMetaSetLoadSnapshotCmd(entries []LoadStatEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()
	entryOffs := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		nodeIDOff := b.CreateString(e.NodeID)
		clusterpb.LoadStatEntryStart(b)
		clusterpb.LoadStatEntryAddNodeId(b, nodeIDOff)
		clusterpb.LoadStatEntryAddDiskUsedPct(b, e.DiskUsedPct)
		clusterpb.LoadStatEntryAddDiskAvailBytes(b, e.DiskAvailBytes)
		clusterpb.LoadStatEntryAddRequestsPerSec(b, e.RequestsPerSec)
		clusterpb.LoadStatEntryAddUpdatedAtUnix(b, e.UpdatedAt.Unix())
		entryOffs[i] = clusterpb.LoadStatEntryEnd(b)
	}
	clusterpb.MetaSetLoadSnapshotCmdStartEntriesVector(b, len(entryOffs))
	for i := len(entryOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(entryOffs[i])
	}
	entriesVec := b.EndVector(len(entryOffs))
	clusterpb.MetaSetLoadSnapshotCmdStart(b)
	clusterpb.MetaSetLoadSnapshotCmdAddEntries(b, entriesVec)
	return fbFinish(b, clusterpb.MetaSetLoadSnapshotCmdEnd(b)), nil
}

func encodeMetaProposeRebalancePlanCmd(plan RebalancePlan) ([]byte, error) {
	b := clusterBuilderPool.Get()
	planIDOff := b.CreateString(plan.PlanID)
	groupIDOff := b.CreateString(plan.GroupID)
	fromOff := b.CreateString(plan.FromNode)
	toOff := b.CreateString(plan.ToNode)
	clusterpb.RebalancePlanStart(b)
	clusterpb.RebalancePlanAddPlanId(b, planIDOff)
	clusterpb.RebalancePlanAddGroupId(b, groupIDOff)
	clusterpb.RebalancePlanAddFromNode(b, fromOff)
	clusterpb.RebalancePlanAddToNode(b, toOff)
	clusterpb.RebalancePlanAddCreatedAtUnix(b, plan.CreatedAt.Unix())
	planOff := clusterpb.RebalancePlanEnd(b)
	clusterpb.MetaProposeRebalancePlanCmdStart(b)
	clusterpb.MetaProposeRebalancePlanCmdAddPlan(b, planOff)
	return fbFinish(b, clusterpb.MetaProposeRebalancePlanCmdEnd(b)), nil
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

func encodeMetaAbortPlanCmd(planID string, reason clusterpb.AbortPlanReason) ([]byte, error) {
	b := clusterBuilderPool.Get()
	planIDOff := b.CreateString(planID)
	clusterpb.MetaAbortPlanCmdStart(b)
	clusterpb.MetaAbortPlanCmdAddPlanId(b, planIDOff)
	clusterpb.MetaAbortPlanCmdAddReason(b, reason)
	return fbFinish(b, clusterpb.MetaAbortPlanCmdEnd(b)), nil
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

// icebergDefaultWarehouse is the warehouse key used for legacy single-warehouse
// installations and for commands that carry an empty warehouse string (pre-T38
// raft log entries replayed after upgrade).
const icebergDefaultWarehouse = "default"

// icebergWarehouseKey returns warehouse if non-empty, falling back to
// icebergDefaultWarehouse for legacy / single-warehouse mode.
func icebergWarehouseKey(warehouse string) string {
	if warehouse == "" {
		return icebergDefaultWarehouse
	}
	return warehouse
}

func icebergNamespaceKey(namespace []string) string {
	return strings.Join(namespace, "\x1f")
}

func icebergTableKey(ident icebergcatalog.Identifier) string {
	return icebergNamespaceKey(ident.Namespace) + "\x1f" + ident.Name
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

func cloneIcebergIdent(in icebergcatalog.Identifier) icebergcatalog.Identifier {
	return icebergcatalog.Identifier{Namespace: cloneStringSlice(in.Namespace), Name: in.Name}
}

func cloneIcebergTableEntry(in IcebergTableEntry) IcebergTableEntry {
	return IcebergTableEntry{
		Warehouse:        in.Warehouse,
		Identifier:       cloneIcebergIdent(in.Identifier),
		MetadataLocation: in.MetadataLocation,
		Properties:       cloneStringMap(in.Properties),
	}
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

func buildIcebergIdentifier(b *flatbuffers.Builder, ident icebergcatalog.Identifier) flatbuffers.UOffsetT {
	namespaceVec := buildStringVector(b, ident.Namespace, clusterpb.IcebergIdentifierStartNamespaceVector)
	nameOff := b.CreateString(ident.Name)
	clusterpb.IcebergIdentifierStart(b)
	clusterpb.IcebergIdentifierAddNamespace(b, namespaceVec)
	clusterpb.IcebergIdentifierAddName(b, nameOff)
	return clusterpb.IcebergIdentifierEnd(b)
}

func readIcebergIdentifier(ident *clusterpb.IcebergIdentifier) (icebergcatalog.Identifier, error) {
	if ident == nil {
		return icebergcatalog.Identifier{}, fmt.Errorf("missing iceberg identifier")
	}
	return icebergcatalog.Identifier{
		Namespace: readStringVector(ident.NamespaceLength(), ident.Namespace),
		Name:      string(ident.Name()),
	}, nil
}

func buildIcebergNamespaceEntriesVector(b *flatbuffers.Builder, entries []IcebergNamespaceEntry) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		namespaceVec := buildStringVector(b, entries[i].Namespace, clusterpb.IcebergNamespaceEntryStartNamespaceVector)
		propsVec := buildKeyValuePropertiesVector(b, entries[i].Properties, clusterpb.IcebergNamespaceEntryStartPropertiesVector)
		warehouseOff := b.CreateString(entries[i].Warehouse)
		clusterpb.IcebergNamespaceEntryStart(b)
		clusterpb.IcebergNamespaceEntryAddNamespace(b, namespaceVec)
		clusterpb.IcebergNamespaceEntryAddProperties(b, propsVec)
		clusterpb.IcebergNamespaceEntryAddWarehouse(b, warehouseOff)
		offsets[i] = clusterpb.IcebergNamespaceEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartIcebergNamespacesVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

func buildIcebergTableEntriesVector(b *flatbuffers.Builder, entries []IcebergTableEntry) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		identOff := buildIcebergIdentifier(b, entries[i].Identifier)
		locationOff := b.CreateString(entries[i].MetadataLocation)
		propsVec := buildKeyValuePropertiesVector(b, entries[i].Properties, clusterpb.IcebergTableEntryStartPropertiesVector)
		warehouseOff := b.CreateString(entries[i].Warehouse)
		clusterpb.IcebergTableEntryStart(b)
		clusterpb.IcebergTableEntryAddIdentifier(b, identOff)
		clusterpb.IcebergTableEntryAddMetadataLocation(b, locationOff)
		clusterpb.IcebergTableEntryAddProperties(b, propsVec)
		clusterpb.IcebergTableEntryAddWarehouse(b, warehouseOff)
		offsets[i] = clusterpb.IcebergTableEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartIcebergTablesVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}
