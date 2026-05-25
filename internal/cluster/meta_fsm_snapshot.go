package cluster

import (
	"bytes"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
)

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
