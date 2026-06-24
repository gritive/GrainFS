package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/raft"
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
	clusterpb.MetaStateSnapshotAddObjectIndex(b, objectIndexVec)
	clusterpb.MetaStateSnapshotAddClusterConfig(b, clusterConfigVec)
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

	// --- PARSE ---
	snap, trailers, err := f.parseSnapshotRoot(data)
	if err != nil {
		return err
	}

	// --- DECODE PHASE ---
	// Decode every snapshot section into a carrier BEFORE touching any f.* field.
	// If any decode fails, Restore returns an error with f.* completely untouched
	// (meta-raft invariant: a failed Restore leaves the FSM un-restored).
	//
	// Calls are EXPLICIT and sequential (not a slice-loop) because the ordering is
	// load-bearing: decodeDEKTrailer MUST precede decodeIAMTrailer (the IAM payload
	// is decrypted with a transient encryptor built from the decoded DEK versions),
	// and decodePeerRegistry must validate before any core state commits.
	var st restoredMetaState
	if err := decodeCoreTopology(snap, &st); err != nil {
		return err
	}
	if err := decodeClusterConfig(snap, &st); err != nil {
		return err
	}
	if err := decodeKEKRotationState(snap, &st); err != nil {
		return err
	}
	if err := decodePeerRegistry(snap, &st); err != nil {
		return err
	}
	if err := f.decodeDEKTrailer(trailers, &st); err != nil {
		return err
	}
	if err := f.decodeIAMTrailer(trailers, &st); err != nil {
		return err
	}
	if err := f.decodeConfigAndPolicyTrailers(trailers, &st); err != nil {
		return err
	}
	if err := f.decodeProtocolCredentialTrailer(trailers, &st); err != nil {
		return err
	}
	if err := f.decodeJWTKeysTrailer(trailers, &st); err != nil {
		return err
	}

	// --- COMMIT PHASE ---
	// All decodes succeeded. Commit to f.* fields. No error returns below.
	// Core state under f.mu; satellite stores and the peer registry under their
	// own mutexes (outside f.mu). The peer-registry commit + side-effect callbacks
	// are LAST, fired only after Restore is guaranteed to return nil.
	onBucketAssigned := f.commitCoreState(&st)
	f.commitSatelliteStores(&st, onBucketAssigned)
	f.commitPeerRegistryAndFireCallbacks(&st)
	return nil
}
