package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// iamSnapshotTrailerMagic is appended after the IAM section so post-fix
// readers can distinguish "new snapshot with IAM trailer" from a legacy
// snapshot that ends at the FlatBuffer root. ASCII "IAMG" (0x47414D49 little-endian).
const iamSnapshotTrailerMagic uint32 = 0x47414D49

// iamSnapshotTrailerLen is the on-disk size of the trailer footer:
// [u32 iam_len][u32 magic]. Always 8 bytes when present.
const iamSnapshotTrailerLen = 8

// MetaCmdType aliases the FlatBuffers-generated type for use within this package.
type MetaCmdType = clusterpb.MetaCmdType

const (
	MetaCmdTypeNoOp                   = clusterpb.MetaCmdTypeNoOp
	MetaCmdTypeAddNode                = clusterpb.MetaCmdTypeAddNode
	MetaCmdTypeRemoveNode             = clusterpb.MetaCmdTypeRemoveNode
	MetaCmdTypePutShardGroup          = clusterpb.MetaCmdTypePutShardGroup        // PR-C
	MetaCmdTypePutBucketAssignment    = clusterpb.MetaCmdTypePutBucketAssignment  // PR-D
	MetaCmdTypeSetLoadSnapshot        = clusterpb.MetaCmdTypeSetLoadSnapshot      // PR-D
	MetaCmdTypeProposeRebalancePlan   = clusterpb.MetaCmdTypeProposeRebalancePlan // PR-D
	MetaCmdTypeAbortPlan              = clusterpb.MetaCmdTypeAbortPlan            // PR-D
	MetaCmdTypeIcebergCreateNamespace = clusterpb.MetaCmdTypeIcebergCreateNamespace
	MetaCmdTypeIcebergDeleteNamespace = clusterpb.MetaCmdTypeIcebergDeleteNamespace
	MetaCmdTypeIcebergCreateTable     = clusterpb.MetaCmdTypeIcebergCreateTable
	MetaCmdTypeIcebergCommitTable     = clusterpb.MetaCmdTypeIcebergCommitTable
	MetaCmdTypeIcebergDeleteTable     = clusterpb.MetaCmdTypeIcebergDeleteTable
	MetaCmdTypeRotateKeyBegin         = clusterpb.MetaCmdTypeRotateKeyBegin
	MetaCmdTypeRotateKeySwitch        = clusterpb.MetaCmdTypeRotateKeySwitch
	MetaCmdTypeRotateKeyDrop          = clusterpb.MetaCmdTypeRotateKeyDrop
	MetaCmdTypeRotateKeyAbort         = clusterpb.MetaCmdTypeRotateKeyAbort
	MetaCmdTypeScrubTrigger           = clusterpb.MetaCmdTypeScrubTrigger // PR4
	MetaCmdTypePutObjectIndex         = clusterpb.MetaCmdTypePutObjectIndex
	MetaCmdTypeDeleteObjectIndex      = clusterpb.MetaCmdTypeDeleteObjectIndex
	MetaCmdTypeIAMSACreate            = clusterpb.MetaCmdTypeIAMSACreate
	MetaCmdTypeIAMSADelete            = clusterpb.MetaCmdTypeIAMSADelete
	MetaCmdTypeIAMKeyCreate           = clusterpb.MetaCmdTypeIAMKeyCreate
	MetaCmdTypeIAMKeyCreateScoped     = clusterpb.MetaCmdTypeIAMKeyCreateScoped
	MetaCmdTypeIAMKeyRevoke           = clusterpb.MetaCmdTypeIAMKeyRevoke
	MetaCmdTypeIAMGrantPut            = clusterpb.MetaCmdTypeIAMGrantPut
	MetaCmdTypeIAMGrantDelete         = clusterpb.MetaCmdTypeIAMGrantDelete
	MetaCmdTypeIAMGrantWildcardPut    = clusterpb.MetaCmdTypeIAMGrantWildcardPut
	MetaCmdTypeIAMAuthEnable          = clusterpb.MetaCmdTypeIAMAuthEnable
	MetaCmdTypeIAMGrantWildcardDelete = clusterpb.MetaCmdTypeIAMGrantWildcardDelete
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
	Namespace  []string
	Properties map[string]string
}

type IcebergTableEntry struct {
	Identifier       icebergcatalog.Identifier
	MetadataLocation string
	Properties       map[string]string
}

type IcebergCreateNamespaceCmd struct {
	RequestID  string
	Namespace  []string
	Properties map[string]string
}

type IcebergDeleteNamespaceCmd struct {
	RequestID string
	Namespace []string
}

type IcebergCreateTableCmd struct {
	RequestID        string
	Identifier       icebergcatalog.Identifier
	MetadataLocation string
	Properties       map[string]string
}

type IcebergCommitTableCmd struct {
	RequestID                string
	Identifier               icebergcatalog.Identifier
	ExpectedMetadataLocation string
	NewMetadataLocation      string
}

type IcebergDeleteTableCmd struct {
	RequestID  string
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
	loadSnapshot      map[string]LoadStatEntry // node_id → stats (PR-D)
	activePlan        *RebalancePlan           // nil = no active plan (PR-D)
	icebergNamespaces map[string]IcebergNamespaceEntry
	icebergTables     map[string]IcebergTableEntry
	onBucketAssigned  func(string, string)             // protected by mu; set before Start() (PR-D)
	onRebalancePlan   func(*RebalancePlan)             // must not block; set before Start() (PR-D)
	onShardGroupAdded func(ShardGroupEntry)            // fired after PutShardGroup applies; protected by mu (v0.0.7.0)
	onIcebergResult   func(string, error)              // requestID, typed catalog result; must not block
	onScrubTrigger    func(scrubber.ScrubTriggerEntry) // PR4: cluster-wide scrub trigger applied; must not block

	// 클러스터 키 회전 — 결정론적 FSM은 여기, side-effect (디스크 I/O,
	// transport identity swap)는 onRotationApplied 콜백으로 분리 (D16).
	rotation          *RotationFSM
	onRotationApplied func(RotationState) // 매 phase 변경 commit 후 호출; nil 이면 no-op

	// IAM sub-FSM — wired after construction via SetIAM (Phase 1). iamStore is
	// always non-nil (default empty); iamApplier is nil until SetIAM is called.
	iamStore   *iam.Store
	iamApplier *iam.Applier
}

func NewMetaFSM() *MetaFSM {
	return &MetaFSM{
		nodes:             make(map[string]MetaNodeEntry),
		shardGroups:       make(map[string]ShardGroupEntry),
		bucketAssignments: make(map[string]string),
		objectIndex:       make(map[string]ObjectIndexEntry),
		objectLatest:      make(map[string]string),
		loadSnapshot:      make(map[string]LoadStatEntry),
		icebergNamespaces: make(map[string]IcebergNamespaceEntry),
		icebergTables:     make(map[string]IcebergTableEntry),
		rotation:          NewRotationFSM(),
		iamStore:          iam.NewStore(),
	}
}

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

// applyCmd decodes a MetaCmd FlatBuffers envelope and mutates state.
// Called by MetaRaft.runApplyLoop on each committed log entry.
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
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplySACreate)
	case clusterpb.MetaCmdTypeIAMSADelete:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplySADelete)
	case clusterpb.MetaCmdTypeIAMKeyCreate:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyKeyCreate)
	case clusterpb.MetaCmdTypeIAMKeyCreateScoped:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyKeyCreateScoped)
	case clusterpb.MetaCmdTypeIAMKeyRevoke:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyKeyRevoke)
	case clusterpb.MetaCmdTypeIAMGrantPut:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyGrantPut)
	case clusterpb.MetaCmdTypeIAMGrantDelete:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyGrantDelete)
	case clusterpb.MetaCmdTypeIAMGrantWildcardPut:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyGrantWildcardPut)
	case clusterpb.MetaCmdTypeIAMGrantWildcardDelete:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyGrantWildcardDelete)
	case clusterpb.MetaCmdTypeIAMAuthEnable:
		return f.applyIAM(cmd.DataBytes(), (*iam.Applier).ApplyAuthEnable)
	default:
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
	f.objectIndex[vkey] = cloneObjectIndexEntry(e)
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
	delete(f.objectIndex, objectIndexVersionKey(bucket, key, versionID))
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

func (f *MetaFSM) applyIcebergCreateNamespace(data []byte) error {
	c, err := decodeMetaIcebergCreateNamespaceCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCreateNamespace: %w", err)
	}
	key := icebergNamespaceKey(c.Namespace)
	var result error
	f.mu.Lock()
	if _, ok := f.icebergNamespaces[key]; ok {
		result = icebergcatalog.ErrNamespaceExists
	} else {
		f.icebergNamespaces[key] = IcebergNamespaceEntry{
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
	f.mu.Lock()
	if _, ok := f.icebergNamespaces[key]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else {
		prefix := key + "\x1f"
		for tableKey := range f.icebergTables {
			if strings.HasPrefix(tableKey, prefix) {
				result = icebergcatalog.ErrNamespaceNotEmpty
				break
			}
		}
		if result == nil {
			delete(f.icebergNamespaces, key)
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
	f.mu.Lock()
	if _, ok := f.icebergNamespaces[nsKey]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else if _, ok := f.icebergTables[tableKey]; ok {
		result = icebergcatalog.ErrTableExists
	} else {
		f.icebergTables[tableKey] = IcebergTableEntry{
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
	entry, ok := f.icebergTables[tableKey]
	if !ok {
		result = icebergcatalog.ErrTableNotFound
	} else if entry.MetadataLocation != c.ExpectedMetadataLocation {
		result = icebergcatalog.ErrCommitFailed
	} else {
		entry.MetadataLocation = c.NewMetadataLocation
		f.icebergTables[tableKey] = entry
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
	f.mu.Lock()
	if _, ok := f.icebergNamespaces[nsKey]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else if _, ok := f.icebergTables[tableKey]; !ok {
		result = icebergcatalog.ErrTableNotFound
	} else {
		delete(f.icebergTables, tableKey)
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
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries := make([]ObjectIndexEntry, 0)
	for lkey, versionID := range f.objectLatest {
		parts := strings.SplitN(lkey, "\x00", 2)
		if len(parts) != 2 || parts[0] != bucket || !strings.HasPrefix(parts[1], prefix) {
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
	}
	return entries
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

func (f *MetaFSM) IcebergNamespace(namespace []string) (IcebergNamespaceEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, ok := f.icebergNamespaces[icebergNamespaceKey(namespace)]
	if !ok {
		return IcebergNamespaceEntry{}, false
	}
	return IcebergNamespaceEntry{
		Namespace:  cloneStringSlice(entry.Namespace),
		Properties: cloneStringMap(entry.Properties),
	}, true
}

func (f *MetaFSM) IcebergNamespaces() []IcebergNamespaceEntry {
	f.mu.RLock()
	out := make([]IcebergNamespaceEntry, 0, len(f.icebergNamespaces))
	for _, entry := range f.icebergNamespaces {
		out = append(out, IcebergNamespaceEntry{
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

func (f *MetaFSM) IcebergTable(ident icebergcatalog.Identifier) (IcebergTableEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, ok := f.icebergTables[icebergTableKey(ident)]
	if !ok {
		return IcebergTableEntry{}, false
	}
	return cloneIcebergTableEntry(entry), true
}

func (f *MetaFSM) IcebergTables(namespace []string) []IcebergTableEntry {
	prefix := icebergNamespaceKey(namespace) + "\x1f"
	f.mu.RLock()
	out := make([]IcebergTableEntry, 0)
	for key, entry := range f.icebergTables {
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
	icebergNamespaces := make([]IcebergNamespaceEntry, 0, len(f.icebergNamespaces))
	for _, entry := range f.icebergNamespaces {
		icebergNamespaces = append(icebergNamespaces, IcebergNamespaceEntry{
			Namespace:  cloneStringSlice(entry.Namespace),
			Properties: cloneStringMap(entry.Properties),
		})
	}
	icebergTables := make([]IcebergTableEntry, 0, len(f.icebergTables))
	for _, entry := range f.icebergTables {
		icebergTables = append(icebergTables, cloneIcebergTableEntry(entry))
	}
	f.mu.RUnlock()

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
	return out, nil
}

// Restore deserializes a MetaStateSnapshot and replaces current state.
func (f *MetaFSM) Restore(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: Restore: empty snapshot")
	}

	// Detect post-Phase-5d trailer (footer = [u32 iam_len][u32 magic=IAMG]).
	// Layout when present: [FB bytes][iam bytes][u32 iam_len][u32 magic].
	// Legacy snapshots end at the FB root and are accepted unchanged.
	fbData := data
	var iamData []byte
	if len(data) >= iamSnapshotTrailerLen {
		footer := data[len(data)-iamSnapshotTrailerLen:]
		magic := binary.LittleEndian.Uint32(footer[4:8])
		if magic == iamSnapshotTrailerMagic {
			iamLen := binary.LittleEndian.Uint32(footer[0:4])
			if int(iamLen)+iamSnapshotTrailerLen > len(data) {
				return fmt.Errorf("meta_fsm: Restore: IAM trailer length %d exceeds snapshot size %d", iamLen, len(data))
			}
			iamEnd := len(data) - iamSnapshotTrailerLen
			iamStart := iamEnd - int(iamLen)
			iamData = data[iamStart:iamEnd]
			fbData = data[:iamStart]
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

	newIcebergNamespaces := make(map[string]IcebergNamespaceEntry, snap.IcebergNamespacesLength())
	var nsFB clusterpb.IcebergNamespaceEntry
	for i := 0; i < snap.IcebergNamespacesLength(); i++ {
		if !snap.IcebergNamespaces(&nsFB, i) {
			return fmt.Errorf("meta_fsm: Restore: iceberg namespace %d decode failed", i)
		}
		entry := IcebergNamespaceEntry{
			Namespace:  readStringVector(nsFB.NamespaceLength(), nsFB.Namespace),
			Properties: readKeyValueProperties(nsFB.PropertiesLength(), nsFB.Properties),
		}
		newIcebergNamespaces[icebergNamespaceKey(entry.Namespace)] = entry
	}

	newIcebergTables := make(map[string]IcebergTableEntry, snap.IcebergTablesLength())
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
		entry := IcebergTableEntry{
			Identifier:       ident,
			MetadataLocation: string(tableFB.MetadataLocation()),
			Properties:       readKeyValueProperties(tableFB.PropertiesLength(), tableFB.Properties),
		}
		newIcebergTables[icebergTableKey(ident)] = entry
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
	cb := f.onBucketAssigned
	f.mu.Unlock()
	if cb != nil {
		for bucket, groupID := range newBucketAssignments {
			cb(bucket, groupID)
		}
	}
	// onRebalancePlan is intentionally NOT called here.
	// Rebalancer handles resume on next tick by checking ActivePlan().

	// IAM restore — replaces in-memory IAM state on the receiving node so
	// raft log compaction (LogGCInterval) does not silently drop SAs/keys/
	// grants/auth_enabled. Skip when there's no trailer (legacy snapshot)
	// or no IAM data, or when IAM is not wired (anonymous/no-encryption mode).
	if len(iamData) > 0 {
		if f.iamStore == nil || f.iamApplier == nil {
			log.Warn().Int("iam_len", len(iamData)).Msg("meta_fsm: Restore: snapshot contains IAM section but IAM not wired; skipping IAM restore")
		} else {
			enc := f.iamApplier.Encryptor()
			if enc == nil {
				log.Warn().Msg("meta_fsm: Restore: IAM applier has no encryptor; skipping IAM restore")
			} else {
				// Wipe pre-restore state so a snapshot install REPLACES rather
				// than MERGES — matches raft Restore semantics for the rest of
				// MetaFSM (which reassigns each map outright above).
				f.iamStore.Reset()
				if err := iam.ReadSnapshot(bytes.NewReader(iamData), f.iamStore, enc); err != nil {
					return fmt.Errorf("meta_fsm: Restore: decode IAM: %w", err)
				}
			}
		}
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

func encodeMetaIcebergCreateNamespaceCmd(c IcebergCreateNamespaceCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	namespaceVec := buildStringVector(b, c.Namespace, clusterpb.MetaIcebergCreateNamespaceCmdStartNamespaceVector)
	propsVec := buildKeyValuePropertiesVector(b, c.Properties, clusterpb.MetaIcebergCreateNamespaceCmdStartPropertiesVector)
	clusterpb.MetaIcebergCreateNamespaceCmdStart(b)
	clusterpb.MetaIcebergCreateNamespaceCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCreateNamespaceCmdAddNamespace(b, namespaceVec)
	clusterpb.MetaIcebergCreateNamespaceCmdAddProperties(b, propsVec)
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
		Namespace:  readStringVector(t.NamespaceLength(), t.Namespace),
		Properties: readKeyValueProperties(t.PropertiesLength(), t.Properties),
	}, nil
}

func encodeMetaIcebergDeleteNamespaceCmd(c IcebergDeleteNamespaceCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	namespaceVec := buildStringVector(b, c.Namespace, clusterpb.MetaIcebergDeleteNamespaceCmdStartNamespaceVector)
	clusterpb.MetaIcebergDeleteNamespaceCmdStart(b)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddNamespace(b, namespaceVec)
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
		Namespace: readStringVector(t.NamespaceLength(), t.Namespace),
	}, nil
}

func encodeMetaIcebergCreateTableCmd(c IcebergCreateTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	locationOff := b.CreateString(c.MetadataLocation)
	propsVec := buildKeyValuePropertiesVector(b, c.Properties, clusterpb.MetaIcebergCreateTableCmdStartPropertiesVector)
	clusterpb.MetaIcebergCreateTableCmdStart(b)
	clusterpb.MetaIcebergCreateTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCreateTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergCreateTableCmdAddMetadataLocation(b, locationOff)
	clusterpb.MetaIcebergCreateTableCmdAddProperties(b, propsVec)
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
		Identifier:       ident,
		MetadataLocation: string(t.MetadataLocation()),
		Properties:       readKeyValueProperties(t.PropertiesLength(), t.Properties),
	}, nil
}

func encodeMetaIcebergCommitTableCmd(c IcebergCommitTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	expectedOff := b.CreateString(c.ExpectedMetadataLocation)
	nextOff := b.CreateString(c.NewMetadataLocation)
	clusterpb.MetaIcebergCommitTableCmdStart(b)
	clusterpb.MetaIcebergCommitTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCommitTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergCommitTableCmdAddExpectedMetadataLocation(b, expectedOff)
	clusterpb.MetaIcebergCommitTableCmdAddNewMetadataLocation(b, nextOff)
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
		Identifier:               ident,
		ExpectedMetadataLocation: string(t.ExpectedMetadataLocation()),
		NewMetadataLocation:      string(t.NewMetadataLocation()),
	}, nil
}

func encodeMetaIcebergDeleteTableCmd(c IcebergDeleteTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	clusterpb.MetaIcebergDeleteTableCmdStart(b)
	clusterpb.MetaIcebergDeleteTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergDeleteTableCmdAddIdentifier(b, identOff)
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
	}
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
	return in
}

func cloneIcebergIdent(in icebergcatalog.Identifier) icebergcatalog.Identifier {
	return icebergcatalog.Identifier{Namespace: cloneStringSlice(in.Namespace), Name: in.Name}
}

func cloneIcebergTableEntry(in IcebergTableEntry) IcebergTableEntry {
	return IcebergTableEntry{
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
		clusterpb.IcebergNamespaceEntryStart(b)
		clusterpb.IcebergNamespaceEntryAddNamespace(b, namespaceVec)
		clusterpb.IcebergNamespaceEntryAddProperties(b, propsVec)
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
		clusterpb.IcebergTableEntryStart(b)
		clusterpb.IcebergTableEntryAddIdentifier(b, identOff)
		clusterpb.IcebergTableEntryAddMetadataLocation(b, locationOff)
		clusterpb.IcebergTableEntryAddProperties(b, propsVec)
		offsets[i] = clusterpb.IcebergTableEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartIcebergTablesVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}
