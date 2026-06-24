package cluster

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// Snapshot serializes current state as FlatBuffers MetaStateSnapshot.
// Returns an error immediately if the FSM has been poisoned by MarkFatalHalted —
// a halted FSM must not produce snapshots that could mask a diverged state.
func (f *MetaFSM) Snapshot() ([]byte, error) {
	if err := f.FatalHaltedErr(); err != nil {
		return nil, fmt.Errorf("FSM halted: %w", err)
	}
	f.mu.RLock()
	nodes := make([]MetaNodeEntry, 0, len(f.nodes))
	for _, n := range f.nodes {
		nodes = append(nodes, n)
	}
	shardGroups := make([]ShardGroupEntry, 0, len(f.shardGroups))
	for _, sg := range f.shardGroups {
		shardGroups = append(shardGroups, sg)
	}
	// Phase 7 topology-generation registry (ordered, deep-copied under RLock).
	placementGenerations := make([]placementGeneration, len(f.placementGenerations))
	for i, g := range f.placementGenerations {
		ids := make([]string, len(g.groupIDs))
		copy(ids, g.groupIDs)
		placementGenerations[i] = placementGeneration{epoch: g.epoch, groupIDs: ids}
	}
	type bucketKV struct {
		bucket     string
		groupID    string
		versioning string
		policy     []byte
	}
	buckets := make([]bucketKV, 0, len(f.bucketRecords))
	for bucket, rec := range f.bucketRecords {
		buckets = append(buckets, bucketKV{
			bucket:     bucket,
			groupID:    rec.GroupID,
			versioning: rec.Versioning,
			policy:     append([]byte(nil), rec.Policy...),
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
	// Snapshot dekRefCounts while holding the read lock.
	dekRefCountsCopy := make(map[uint32]uint64, len(f.dekRefCounts))
	for g, c := range f.dekRefCounts {
		if c > 0 {
			dekRefCountsCopy[g] = c
		}
	}
	// Deep-copy dekRewrapDone in the same RLock window as dekRefCounts so the
	// two are captured atomically (skew between ref counts and done sets would
	// allow prune-safety to race). The inner map must also be copied — after
	// RUnlock a concurrent applyDEKRewrapProgress can mutate the inner set
	// under f.mu.Lock while the encoder iterates the outer-only copy.
	var dekRewrapDoneCopy map[uint32]map[string]uint32
	if len(f.dekRewrapDone) > 0 {
		dekRewrapDoneCopy = make(map[uint32]map[string]uint32, len(f.dekRewrapDone))
		for g, nodeSet := range f.dekRewrapDone {
			innerCopy := make(map[string]uint32, len(nodeSet))
			for n, epoch := range nodeSet {
				innerCopy[n] = epoch
			}
			dekRewrapDoneCopy[g] = innerCopy
		}
	}
	// Capture active KEK version under the same RLock so a concurrent
	// SetActiveKEKVersion can't race the DKVS trailer emission.
	activeKEKVersionCopy := f.activeKEKVersion
	// Capture rotation request ring and KEK status slice (Phase B Task 2).
	lastRotationRequestsCopy := make([]rotationRequestRecord, len(f.lastRotationRequests))
	copy(lastRotationRequestsCopy, f.lastRotationRequests)
	kekStatusesCopy := make([]kekStatusRecord, len(f.kekStatuses))
	copy(kekStatusesCopy, f.kekStatuses)
	// zero-CA peer registry (Task 5): export under the same RLock window so the
	// serialized accept-set is consistent with the rest of the snapshot.
	peersCopy := f.peers.export()
	denyCopy := f.peers.exportDenylist()
	// zero-CA cutover drop bit (slot 15, H3): capture under the same RLock so the
	// serialized bit is consistent with the rest of the snapshot.
	droppedCopy := f.clusterKeyDropped
	// zero-CA present-flip bit (slot 16, PR-2a §8c): capture alongside drop bit
	// for consistency.
	presentFlipBegunCopy := f.presentFlipBegun
	// zero-CA revoked-node set (slot 17): capture under the same RLock so the
	// serialized set is consistent with the rest of the snapshot.
	revokedIDs := make([]string, 0, len(f.revokedNodeIDs))
	for id := range f.revokedNodeIDs {
		revokedIDs = append(revokedIDs, id)
	}
	// Task 4b: capture DEKKeeper wraps inside the same lock window as
	// activeKEKVersion. LOCK ORDER: f.mu → keeper.mu (VersionsAndActive
	// acquires keeper.mu). Releasing f.mu first and then calling
	// VersionsAndActive would allow a concurrent KEK rotation Apply to
	// interleave and produce a torn snapshot (new wraps / old active version).
	var (
		dekVersionsCopy map[uint32][]byte
		dekActiveCopy   uint32
	)
	if f.dekKeeper != nil {
		dekVersionsCopy, dekActiveCopy = f.dekKeeper.VersionsAndActive()
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

	// Index groups removed in Phase 4; encode empty vector for wire compatibility.
	clusterpb.MetaStateSnapshotStartIndexGroupsVector(b, 0)
	igVec := b.EndVector(0)

	// Build PlacementGenerationEntry offsets (Phase 7). Empty for single-generation
	// legacy clusters → zero-length vector (restore reads it back as nil).
	pgOffs := make([]flatbuffers.UOffsetT, len(placementGenerations))
	for i := len(placementGenerations) - 1; i >= 0; i-- {
		pg := placementGenerations[i]
		gidOffs := make([]flatbuffers.UOffsetT, len(pg.groupIDs))
		for j := len(pg.groupIDs) - 1; j >= 0; j-- {
			gidOffs[j] = b.CreateString(pg.groupIDs[j])
		}
		clusterpb.PlacementGenerationEntryStartGroupIdsVector(b, len(gidOffs))
		for j := len(gidOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(gidOffs[j])
		}
		gidVec := b.EndVector(len(gidOffs))
		clusterpb.PlacementGenerationEntryStart(b)
		clusterpb.PlacementGenerationEntryAddEpoch(b, pg.epoch)
		clusterpb.PlacementGenerationEntryAddGroupIds(b, gidVec)
		pgOffs[i] = clusterpb.PlacementGenerationEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartPlacementGenerationsVector(b, len(pgOffs))
	for i := len(pgOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(pgOffs[i])
	}
	pgVec := b.EndVector(len(pgOffs))

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

	// Build BucketAssignmentEntry offsets. versioning and policy are additive
	// fields (Task 1); older readers that don't know them will simply ignore them.
	// All nested objects (strings, byte vectors) must be created before
	// BucketAssignmentEntryStart (FlatBuffers A1 rule).
	baOffs := make([]flatbuffers.UOffsetT, len(buckets))
	for i := len(buckets) - 1; i >= 0; i-- {
		bkt := buckets[i]
		bucketOff := b.CreateString(bkt.bucket)
		groupIDOff := b.CreateString(bkt.groupID)
		versioningOff := b.CreateString(bkt.versioning)
		var policyVec flatbuffers.UOffsetT
		if len(bkt.policy) > 0 {
			policyVec = b.CreateByteVector(bkt.policy)
		}
		clusterpb.BucketAssignmentEntryStart(b)
		clusterpb.BucketAssignmentEntryAddBucket(b, bucketOff)
		clusterpb.BucketAssignmentEntryAddGroupId(b, groupIDOff)
		clusterpb.BucketAssignmentEntryAddVersioning(b, versioningOff)
		if len(bkt.policy) > 0 {
			clusterpb.BucketAssignmentEntryAddPolicy(b, policyVec)
		}
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
	// Object index removed in Phase 4; encode empty vector for wire compatibility.
	clusterpb.MetaStateSnapshotStartObjectIndexVector(b, 0)
	objectIndexVec := b.EndVector(0)

	// ClusterConfig: serialize the wrapper's current snap into a stand-alone
	// FBS buffer and embed it as a [ubyte] vector. Always emit — the inner
	// buffer is small (~tens of bytes) and a zero-rev empty config round-trips
	// to the same zero clusterConfigSnap on Restore.
	ccBytes := serializeClusterConfig(f.clusterCfg)
	clusterConfigVec := b.CreateByteVector(ccBytes)

	// Phase B Task 2: build last_rotation_request_entries vector.
	lrrOffs := make([]flatbuffers.UOffsetT, len(lastRotationRequestsCopy))
	for i := len(lastRotationRequestsCopy) - 1; i >= 0; i-- {
		r := lastRotationRequestsCopy[i]
		ridVec := b.CreateByteVector(r.requestID[:])
		clusterpb.LastRotationRequestEntryStart(b)
		clusterpb.LastRotationRequestEntryAddRequestId(b, ridVec)
		clusterpb.LastRotationRequestEntryAddStatus(b, byte(r.status))
		clusterpb.LastRotationRequestEntryAddApplyIndex(b, r.applyIndex)
		lrrOffs[i] = clusterpb.LastRotationRequestEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartLastRotationRequestEntriesVector(b, len(lrrOffs))
	for i := len(lrrOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(lrrOffs[i])
	}
	lrrVec := b.EndVector(len(lrrOffs))

	// Phase B Task 2: build kek_status_entries vector.
	kekOffs := make([]flatbuffers.UOffsetT, len(kekStatusesCopy))
	for i := len(kekStatusesCopy) - 1; i >= 0; i-- {
		e := kekStatusesCopy[i]
		clusterpb.KEKStatusEntryStart(b)
		clusterpb.KEKStatusEntryAddVersion(b, e.version)
		clusterpb.KEKStatusEntryAddStatus(b, byte(e.status))
		clusterpb.KEKStatusEntryAddRetireCommitIndex(b, e.retireCommitIndex)
		kekOffs[i] = clusterpb.KEKStatusEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartKekStatusEntriesVector(b, len(kekOffs))
	for i := len(kekOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(kekOffs[i])
	}
	kekStatusVec := b.EndVector(len(kekOffs))

	// zero-CA peer registry (Task 5): build PeerEntry offsets + vector. Nested
	// strings/vectors must be created before PeerEntryStart (A1).
	peerOffs := make([]flatbuffers.UOffsetT, len(peersCopy))
	for i := len(peersCopy) - 1; i >= 0; i-- {
		p := peersCopy[i]
		nodeIDOff := b.CreateString(p.NodeID)
		spkiVec := b.CreateByteVector(p.SPKI[:])
		addrOff := b.CreateString(p.Address)
		clusterpb.PeerEntryStart(b)
		clusterpb.PeerEntryAddNodeId(b, nodeIDOff)
		clusterpb.PeerEntryAddSpki(b, spkiVec)
		clusterpb.PeerEntryAddAddress(b, addrOff)
		clusterpb.PeerEntryAddState(b, byte(p.State))
		clusterpb.PeerEntryAddPresentsPerNode(b, p.PresentsPerNode)
		clusterpb.PeerEntryAddNodeKeyKekGen(b, p.NodeKeyKEKGen)
		peerOffs[i] = clusterpb.PeerEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartPeersVector(b, len(peerOffs))
	for i := len(peerOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(peerOffs[i])
	}
	peersVec := b.EndVector(len(peerOffs))

	denyOffs := make([]flatbuffers.UOffsetT, len(denyCopy))
	for i := len(denyCopy) - 1; i >= 0; i-- {
		vec := b.CreateByteVector(denyCopy[i][:])
		clusterpb.SPKIBytesStart(b)
		clusterpb.SPKIBytesAddValue(b, vec)
		denyOffs[i] = clusterpb.SPKIBytesEnd(b)
	}
	clusterpb.MetaStateSnapshotStartRevokedPeerSpkisVector(b, len(denyOffs))
	for i := len(denyOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(denyOffs[i])
	}
	denyVec := b.EndVector(len(denyOffs))

	// zero-CA revoked-node set (slot 17): sorted for determinism (mirrors
	// revoked_peer_spkis). Nested strings must be created before the vector start.
	sort.Strings(revokedIDs)
	revokedOffs := make([]flatbuffers.UOffsetT, len(revokedIDs))
	for i, id := range revokedIDs {
		revokedOffs[i] = b.CreateString(id)
	}
	clusterpb.MetaStateSnapshotStartRevokedNodeIdsVector(b, len(revokedOffs))
	for i := len(revokedOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(revokedOffs[i])
	}
	revokedNodeIDsVec := b.EndVector(len(revokedOffs))

	clusterpb.MetaStateSnapshotStart(b)
	clusterpb.MetaStateSnapshotAddNodes(b, nodesVec)
	clusterpb.MetaStateSnapshotAddShardGroups(b, sgVec)
	clusterpb.MetaStateSnapshotAddIndexGroups(b, igVec)
	clusterpb.MetaStateSnapshotAddPlacementGenerations(b, pgVec)
	clusterpb.MetaStateSnapshotAddBucketAssignments(b, baVec)
	clusterpb.MetaStateSnapshotAddLoadSnapshot(b, lsVec)
	if activePlanCopy != nil {
		clusterpb.MetaStateSnapshotAddActivePlan(b, activePlanOff)
	}
	clusterpb.MetaStateSnapshotAddIcebergNamespaces(b, icebergNamespaceVec)
	clusterpb.MetaStateSnapshotAddIcebergTables(b, icebergTableVec)
	clusterpb.MetaStateSnapshotAddObjectIndex(b, objectIndexVec)
	clusterpb.MetaStateSnapshotAddClusterConfig(b, clusterConfigVec)
	clusterpb.MetaStateSnapshotAddIcebergSchemaVersion(b, 2)
	clusterpb.MetaStateSnapshotAddLastRotationRequestEntries(b, lrrVec)
	clusterpb.MetaStateSnapshotAddKekStatusEntries(b, kekStatusVec)
	clusterpb.MetaStateSnapshotAddPeers(b, peersVec)
	clusterpb.MetaStateSnapshotAddRevokedPeerSpkis(b, denyVec)
	clusterpb.MetaStateSnapshotAddClusterKeyDropped(b, droppedCopy)
	clusterpb.MetaStateSnapshotAddPresentFlipBegun(b, presentFlipBegunCopy)
	clusterpb.MetaStateSnapshotAddRevokedNodeIds(b, revokedNodeIDsVec)
	root := clusterpb.MetaStateSnapshotEnd(b)
	bs := fbFinish(b, root)

	blob, err := f.appendSnapshotTrailers(bs, dekVersionsCopy, dekActiveCopy, dekRefCountsCopy, activeKEKVersionCopy, dekRewrapDoneCopy)
	if err != nil {
		return nil, err
	}
	return f.sealSnapshotEnvelope(blob)
}

// Restore deserializes a MetaStateSnapshot and replaces current state. The
// store-meta record (meta) carries the snapshot FormatVersion; the meta-Raft
// FSM has its own in-payload versioning (FlatBuffers + IAM trailer) and accepts
// any FormatVersion for backward compatibility with pre-C2-P3 data dirs.
func (f *MetaFSM) Restore(_ raft.SnapshotMeta, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: Restore: empty snapshot")
	}

	plain, err := f.openSnapshotEnvelope(data)
	if err != nil {
		return err
	}

	trailers, err := peelMetaSnapshotTrailers(plain)
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

	// Phase 7 placement-generation registry. Missing slot (legacy snapshots) →
	// PlacementGenerationsLength()==0 → nil, preserving single-generation behavior.
	var newPlacementGenerations []placementGeneration
	if n := snap.PlacementGenerationsLength(); n > 0 {
		newPlacementGenerations = make([]placementGeneration, 0, n)
		var pgEntry clusterpb.PlacementGenerationEntry
		for i := 0; i < n; i++ {
			if !snap.PlacementGenerations(&pgEntry, i) {
				return fmt.Errorf("meta_fsm: Restore: placement generation %d decode failed", i)
			}
			ids := make([]string, pgEntry.GroupIdsLength())
			for j := 0; j < pgEntry.GroupIdsLength(); j++ {
				ids[j] = string(pgEntry.GroupIds(j))
			}
			newPlacementGenerations = append(newPlacementGenerations, placementGeneration{epoch: pgEntry.Epoch(), groupIDs: ids})
		}
	}

	newBucketRecords := make(map[string]BucketRecord, snap.BucketAssignmentsLength())
	var baEntry clusterpb.BucketAssignmentEntry
	for i := 0; i < snap.BucketAssignmentsLength(); i++ {
		if !snap.BucketAssignments(&baEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: bucket assignment %d decode failed", i)
		}
		bucket := string(baEntry.Bucket())
		var policy []byte
		if pb := baEntry.PolicyBytes(); len(pb) > 0 {
			policy = append([]byte(nil), pb...)
		}
		newBucketRecords[bucket] = BucketRecord{
			GroupID:    string(baEntry.GroupId()),
			Versioning: string(baEntry.Versioning()),
			Policy:     policy,
		}
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

	// Phase B Task 2: decode last_rotation_request_entries (slot 11).
	newLastRotationRequests := make([]rotationRequestRecord, 0, snap.LastRotationRequestEntriesLength())
	var lrrFB clusterpb.LastRotationRequestEntry
	for i := 0; i < snap.LastRotationRequestEntriesLength(); i++ {
		if !snap.LastRotationRequestEntries(&lrrFB, i) {
			return fmt.Errorf("meta_fsm: Restore: last_rotation_request_entries[%d] decode failed", i)
		}
		var rid [16]byte
		copy(rid[:], lrrFB.RequestIdBytes())
		newLastRotationRequests = append(newLastRotationRequests, rotationRequestRecord{
			requestID:  rid,
			status:     RotationStatus(lrrFB.Status()),
			applyIndex: lrrFB.ApplyIndex(),
		})
	}

	// Phase B Task 2: decode kek_status_entries (slot 12).
	newKEKStatuses := make([]kekStatusRecord, 0, snap.KekStatusEntriesLength())
	var kekFB clusterpb.KEKStatusEntry
	for i := 0; i < snap.KekStatusEntriesLength(); i++ {
		if !snap.KekStatusEntries(&kekFB, i) {
			return fmt.Errorf("meta_fsm: Restore: kek_status_entries[%d] decode failed", i)
		}
		newKEKStatuses = append(newKEKStatuses, kekStatusRecord{
			version:           kekFB.Version(),
			status:            KEKLifecycleStatus(kekFB.Status()),
			retireCommitIndex: kekFB.RetireCommitIndex(),
		})
	}

	// zero-CA peer registry (Task 5, slot 13): decode every PeerEntry. A missing
	// vector (legacy pre-Task-5 snapshot) yields an empty slice — the commit then
	// clears the registry, matching a fresh node with no membership yet.
	newPeers := make([]peerEntry, 0, snap.PeersLength())
	var peerFB clusterpb.PeerEntry
	for i := 0; i < snap.PeersLength(); i++ {
		if !snap.Peers(&peerFB, i) {
			return fmt.Errorf("meta_fsm: Restore: peers[%d] decode failed", i)
		}
		// SPKI MUST be exactly 32 bytes. copy() into [32]byte would silently
		// truncate/zero-pad a malformed length, so reject BEFORE the copy — a
		// corrupt meta snapshot is fatal, matching the decode-failure convention
		// above (deterministic across nodes: same bytes → same hard-error).
		if n := len(peerFB.SpkiBytes()); n != 32 {
			return fmt.Errorf("meta_fsm: Restore: peers[%d] SPKI length %d, want 32", i, n)
		}
		var spki [32]byte
		copy(spki[:], peerFB.SpkiBytes())
		newPeers = append(newPeers, peerEntry{
			NodeID:          string(peerFB.NodeId()),
			SPKI:            spki,
			Address:         string(peerFB.Address()),
			State:           peerState(peerFB.State()),
			PresentsPerNode: peerFB.PresentsPerNode(),
			NodeKeyKEKGen:   peerFB.NodeKeyKekGen(),
		})
	}
	// VALIDATE + BUILD the peer indexes HERE, in the decode phase, so a corrupt
	// peer vector (duplicate node ID / duplicate SPKI / bad state) fails BEFORE
	// any core FSM state is committed below. Previously peer import ran AFTER
	// the f.nodes/shardGroups commit, leaving a FAILED Restore with
	// partially-mutated core state (violating the meta-raft invariant that a
	// failed Restore leaves the FSM un-restored). The commit phase swaps these
	// pre-validated maps in via commitPeerIndexes, which cannot fail.
	newPeersByNodeID, newPeersBySPKI, err := validatePeerEntries(newPeers)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: peer registry validate: %w", err)
	}

	newDenyEntries := make([][32]byte, 0, snap.RevokedPeerSpkisLength())
	var denyFB clusterpb.SPKIBytes
	for i := 0; i < snap.RevokedPeerSpkisLength(); i++ {
		if !snap.RevokedPeerSpkis(&denyFB, i) {
			return fmt.Errorf("meta_fsm: Restore: revoked_peer_spkis[%d] decode failed", i)
		}
		raw := denyFB.ValueBytes()
		if len(raw) != 32 {
			return fmt.Errorf("meta_fsm: Restore: revoked_peer_spkis[%d] SPKI length %d, want 32", i, len(raw))
		}
		var spki [32]byte
		copy(spki[:], raw)
		newDenyEntries = append(newDenyEntries, spki)
	}
	newDeny, err := validateDenylistEntries(newDenyEntries)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: denylist validate: %w", err)
	}

	// zero-CA cutover drop bit (slot 15, H3): decode into a local here so the
	// commit + callback-fire below use the captured value, never a field read
	// outside the lock. Legacy snapshots default false (FlatBuffer default).
	droppedDecoded := snap.ClusterKeyDropped()

	// zero-CA revoked-node set (slot 17): decode into a local map. A missing
	// vector (legacy snapshot) yields an empty set, matching a fresh FSM. The
	// evacuator re-derives from the restored set on its next tick, so no
	// onNodeRevoked callback is fired from Restore (mirrors onRebalancePlan).
	newRevokedNodeIDs := make(map[string]struct{}, snap.RevokedNodeIdsLength())
	for i := 0; i < snap.RevokedNodeIdsLength(); i++ {
		id := string(snap.RevokedNodeIds(i))
		if id == "" {
			continue
		}
		newRevokedNodeIDs[id] = struct{}{}
	}

	// --- DECODE PHASE ---
	// Decode all trailers into local variables BEFORE touching any f.* field.
	// If any decode fails, Restore returns an error with f.* completely untouched.

	// DKVS: decode DEK version snapshot FIRST (R2 two-pass decode). The IAM
	// trailer below is sealed under the DEK post-R2, but the live DEKKeeper
	// is not wired until AFTER Restore returns (boot_phases_raft.go). We
	// build a transient read-only DataEncryptor from the DEK versions just
	// decoded, then use it to decrypt the IAM trailer. Codex P0: gate on
	// hasDEKData, NOT newDEKActive>0 — gen 0 is the legitimate
	// genesis-bootstrap active gen.
	var (
		newDEKVersions      map[uint32][]byte
		newDEKActive        uint32
		newDEKRefs          map[uint32]uint64
		newActiveKEKVersion uint32
		newDEKRewrapDone    map[uint32]map[string]uint32
		hasDEKData          bool
	)
	if len(trailers.dekData) > 0 {
		versions, active, refs, activeKEK, done, err := decodeMetaDEKVersionSnapshot(trailers.dekData)
		if err != nil {
			return fmt.Errorf("meta_fsm: Restore: decode DEK versions: %w", err)
		}
		newDEKVersions = versions
		newDEKActive = active
		newDEKRefs = refs
		newActiveKEKVersion = activeKEK
		newDEKRewrapDone = done
		hasDEKData = true
	}

	// IAM: validate by decoding into a temporary store; commit via RestoreFrom later.
	// (F17: iamEnc is no longer needed at commit time — RestoreFrom swaps the state
	// pointer atomically without re-parsing the snapshot bytes.)
	//
	// R2 two-pass decode: when there is IAM data to decrypt, build a transient
	// read-only DataEncryptor from the DEK versions just decoded above. The
	// transient is constructed lazily here (not eagerly above) so a snapshot
	// with DEK metadata but no IAM payload doesn't force the destination FSM
	// to know the source's activeKEK version (regression: that would break
	// snapshots taken on a cluster mid-KEK-rotation if the destination only
	// has the older KEK wired).
	var iamTempStore *iam.Store
	if len(trailers.iamData) > 0 {
		if f.iamStore == nil || f.iamApplier == nil {
			log.Warn().Int("iam_len", len(trailers.iamData)).Msg("meta_fsm: Restore: snapshot contains IAM section but IAM not wired; skipping IAM restore")
		} else if !hasDEKData {
			// IAM trailer present but no DEK trailer — corrupt snapshot.
			// Format guard refuses pre-R2 dirs at boot, so this combination
			// should be unreachable in honest operation. Hard-fail rather
			// than warn-skip (which would silently lose IAM state).
			return fmt.Errorf("meta_fsm: Restore: IAM trailer present but DEK trailer missing — corrupt snapshot")
		} else {
			transient, err := encrypt.NewTransientReadOnlyDEK(f.clusterID[:], newDEKVersions, newDEKActive, newActiveKEKVersion, f.KEKStore())
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: build transient read-only DEK: %w", err)
			}
			iamDecodeEnc := storage.NewTransientDataEncryptor(transient, f.clusterID[:])
			tmp := iam.NewStore()
			if err := iam.ReadSnapshot(bytes.NewReader(trailers.iamData), tmp, iamDecodeEnc); err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode IAM: %w", err)
			}
			iamTempStore = tmp
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

	// IPST: decode IAM policy stores snapshot (§2 stores).
	type ipstDecoded struct {
		polSnap    []policystore.PolicyEntry
		grpSnap    []group.GroupEntry
		attachSnap policyattach.AttachSnapshot
		bpSnap     []bucketpolicy.BucketPolicyEntry
	}
	var newIPST *ipstDecoded
	if len(trailers.ipstData) > 0 {
		if f.policyStore == nil && f.groupStore == nil && f.policyAttachStore == nil && f.bucketPolicyStore == nil {
			log.Warn().Int("ipst_len", len(trailers.ipstData)).Msg("meta_fsm: Restore: snapshot contains IPST section but no policy stores wired; skipping")
		} else {
			polSnap, grpSnap, attachSnap, bpSnap, err := decodeMetaIAMPolicyStoresSnapshot(trailers.ipstData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode IAM policy stores: %w", err)
			}
			newIPST = &ipstDecoded{polSnap, grpSnap, attachSnap, bpSnap}
		}
	}

	var newProtocolCredentials []protocred.Credential
	var newProtocolCredentialRequests []ProtocolCredentialRequestRecord
	restoreProtocolCredentials := f.protocolCredentialStore != nil
	if len(trailers.pcreData) > 0 {
		if !restoreProtocolCredentials {
			log.Warn().Int("pcre_len", len(trailers.pcreData)).Msg("meta_fsm: Restore: snapshot contains protocol credentials but store not wired; skipping")
		} else {
			rows, requests, err := decodeProtocolCredentialsSnapshotState(trailers.pcreData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode protocol credentials: %w", err)
			}
			newProtocolCredentials = rows
			newProtocolCredentialRequests = requests
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
	f.placementGenerations = newPlacementGenerations
	f.bucketRecords = newBucketRecords
	f.loadSnapshot = newLoadSnapshot
	f.activePlan = newActivePlan
	f.icebergNamespaces = newIcebergNamespaces
	f.icebergTables = newIcebergTables
	f.lastRotationRequests = newLastRotationRequests
	f.kekStatuses = newKEKStatuses
	f.clusterKeyDropped = droppedDecoded
	f.presentFlipBegun = snap.PresentFlipBegun()
	f.revokedNodeIDs = newRevokedNodeIDs
	if hasDEKData {
		f.pendingDEKVersions = newDEKVersions
		f.pendingDEKActive = newDEKActive
		f.activeKEKVersion = newActiveKEKVersion
		// Task 4c: remember which KEK version the snapshot wraps were sealed under.
		// This field is read by SnapshotCapturedKEKVersion() → rebuildDEKKeeperFromRestore.
		// It must NOT be overwritten by KEKRotateCmd log replay, hence the separate field.
		f.pendingActiveKEKVersion = newActiveKEKVersion
		if newDEKRefs != nil {
			f.dekRefCounts = newDEKRefs
		} else {
			// Pre-Task-12 snapshot: no ref_counts trailer field. Object index was
			// removed in Phase 4; start with empty ref counts.
			f.dekRefCounts = make(map[uint32]uint64)
		}
		// Restore rewrap done set. Pre-S6d snapshots decode nil → empty map.
		if newDEKRewrapDone != nil {
			f.dekRewrapDone = newDEKRewrapDone
		} else {
			f.dekRewrapDone = nil
		}
	} else {
		// No DKVS trailer (the leader's keeper was empty when it snapshotted).
		// Symmetrically RESET the pending DEK fields so the post-restore DEK
		// install (installSnapshotDEKs on the live path, rebuildDEKKeeperFromRestore
		// on boot) sees an empty version set and no-ops, rather than installing
		// from STALE versions left over from a prior with-trailer restore. Without
		// this reset the no-op contract would be violated on a repeated live restore.
		f.pendingDEKVersions = nil
		f.pendingDEKActive = 0
		f.pendingActiveKEKVersion = 0
		// Symmetric with the with-trailer branch: clear stale rewrap-done entries
		// so a no-trailer restore onto an FSM that previously held completion
		// records cannot leave phantom prune-safety input behind.
		f.dekRewrapDone = nil
	}
	if restoreProtocolCredentials {
		f.protocolCredentialStore.Restore(newProtocolCredentials)
		f.protocolCredentialRequests = make(map[string]ProtocolCredentialRequestRecord, len(newProtocolCredentialRequests))
		for _, row := range newProtocolCredentialRequests {
			f.protocolCredentialRequests[row.RequestID] = row
		}
	}
	cb := f.onBucketAssigned
	f.mu.Unlock()

	if newClusterCfgSnap != nil {
		f.clusterCfg.ReplaceSnap(newClusterCfgSnap)
	}
	if cb != nil {
		for bucket, rec := range newBucketRecords {
			cb(bucket, rec.GroupID)
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

	// zero-CA peer registry (Task 5): swap in the pre-validated registry indexes
	// (validated in the decode phase above), then fire onPeersChanged so the
	// transport composer rebuilds the accept-set union. Without this, the
	// per-node SPKIs vanish after snapshot install and the composer silently
	// drops them → partition. commitPeerIndexes cannot fail — all validation
	// (duplicate node ID / SPKI, bad state) ran before any core state committed,
	// so a corrupt peer vector never leaves partially-mutated FSM state. The
	// registry has its own mutex, so the swap runs outside f.mu. firePeersChanged
	// snapshots the callback under RLock and invokes it outside (existing pattern).
	//
	// This commit + callback are deliberately the LAST side effects of Restore:
	// the accept-set rebuild must fire ONLY after Restore is guaranteed to return
	// nil. All preceding store commits (IPST, JKEY) are documented atomic/no-error,
	// so a late failure must NOT have rebuilt the transport accept-set for a
	// Restore that ultimately fails. Nothing after this point can err.
	f.peers.commitDenylist(newDeny)
	f.peers.commitPeerIndexes(newPeersByNodeID, newPeersBySPKI)
	f.firePeersChanged()

	// zero-CA cutover drop bit (spec §8 H3): if the restored snapshot says the
	// cluster key was dropped, fire the boot callback OUTSIDE f.mu (the callback
	// mutates transport state). Gated on the decoded local, not a field read, to
	// avoid an outside-lock field access. Dormant in PR-1 (snapshot always false).
	if droppedDecoded {
		f.mu.RLock()
		cb := f.onClusterKeyDropped
		f.mu.RUnlock()
		if cb != nil {
			cb()
		}
	}
	// PR-2a §8c step 5 (F2 spec-gate fix): fire outside f.mu when bit restored.
	if snap.PresentFlipBegun() {
		f.mu.Lock()
		cb := f.onPresentFlip
		f.mu.Unlock()
		if cb != nil {
			cb()
		}
	}
	return nil
}
