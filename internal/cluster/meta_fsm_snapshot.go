package cluster

import (
	"fmt"

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
	buckets := make([]bucketSnapshotKV, 0, len(f.bucketRecords))
	for bucket, rec := range f.bucketRecords {
		buckets = append(buckets, bucketSnapshotKV{
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

	// All nested objects must be built BEFORE MetaStateSnapshotStart (A1). Each
	// per-section vector is built by a build*Vector helper that appends to the
	// SAME builder b; the call ORDER below is load-bearing (FlatBuffers wire
	// output depends on op order) and matches the pre-extraction sequence. The
	// byte-identity guard is TestByteIdentity_MetaFSMSnapshot.
	sgVec := buildShardGroupsVector(b, shardGroups)

	// Index groups removed in Phase 4; encode empty vector for wire compatibility.
	clusterpb.MetaStateSnapshotStartIndexGroupsVector(b, 0)
	igVec := b.EndVector(0)

	pgVec := buildPlacementGenerationsVector(b, placementGenerations)
	nodesVec := buildNodesVector(b, nodes)
	baVec := buildBucketAssignmentsVector(b, buckets)
	lsVec := buildLoadSnapshotVector(b, lsEntries)
	activePlanOff := buildActivePlan(b, activePlanCopy)

	// Object index removed in Phase 4; encode empty vector for wire compatibility.
	clusterpb.MetaStateSnapshotStartObjectIndexVector(b, 0)
	objectIndexVec := b.EndVector(0)

	// ClusterConfig: serialize the wrapper's current snap into a stand-alone
	// FBS buffer and embed it as a [ubyte] vector. Always emit — the inner
	// buffer is small (~tens of bytes) and a zero-rev empty config round-trips
	// to the same zero clusterConfigSnap on Restore.
	ccBytes := serializeClusterConfig(f.clusterCfg)
	clusterConfigVec := b.CreateByteVector(ccBytes)

	lrrVec := buildLastRotationRequestEntriesVector(b, lastRotationRequestsCopy)
	kekStatusVec := buildKEKStatusEntriesVector(b, kekStatusesCopy)
	peersVec := buildPeersVector(b, peersCopy)
	denyVec := buildRevokedPeerSpkisVector(b, denyCopy)
	revokedNodeIDsVec := buildRevokedNodeIdsVector(b, revokedIDs)

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
