package cluster

import (
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// This file holds the per-section FlatBuffers vector builders extracted from
// (*MetaFSM).Snapshot(). Each helper appends to the SAME builder b and returns
// the section's UOffsetT. The extraction is behavior-preserving: builder
// operation order (Create* / *StartVector / PrependUOffsetT / EndVector) and
// the call order in Snapshot() are byte-identical to the pre-extraction code —
// FlatBuffers wire output depends on op order, so this is extract-don't-reorder.
// The byte-identity guard is TestByteIdentity_MetaFSMSnapshot.

// bucketSnapshotKV is the encode-time capture of one bucketRecords entry,
// lifted from a function-local struct so buildBucketAssignmentsVector can take
// it as a typed slice.
type bucketSnapshotKV struct {
	bucket     string
	groupID    string
	versioning string
	policy     []byte
}

// prependOffsetsVector emits the common vector tail shared by every section:
// prepend the element offsets in REVERSE (FlatBuffers stores vectors back to
// front) and close the vector. The caller MUST have already opened the vector
// with the section-specific MetaStateSnapshotStart<X>Vector starter (the
// generated starters are thin b.StartVector wrappers, so the emitted bytes are
// independent of which starter opened it — the tail is identical). Byte
// output is unchanged from the pre-dedup inline loops; guarded by
// TestByteIdentity_MetaFSMSnapshot.
func prependOffsetsVector(b *flatbuffers.Builder, offs []flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func buildShardGroupsVector(b *flatbuffers.Builder, shardGroups []ShardGroupEntry) flatbuffers.UOffsetT {
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
	return prependOffsetsVector(b, sgOffs)
}

// buildPlacementGenerationsVector encodes the Phase 7 topology-generation
// registry. Empty for single-generation legacy clusters → zero-length vector
// (restore reads it back as nil).
func buildPlacementGenerationsVector(b *flatbuffers.Builder, placementGenerations []placementGeneration) flatbuffers.UOffsetT {
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
	return prependOffsetsVector(b, pgOffs)
}

func buildNodesVector(b *flatbuffers.Builder, nodes []MetaNodeEntry) flatbuffers.UOffsetT {
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
	return prependOffsetsVector(b, nodeOffs)
}

// buildBucketAssignmentsVector encodes per-bucket records. versioning and
// policy are additive fields (Task 1); older readers that don't know them will
// simply ignore them. All nested objects (strings, byte vectors) must be
// created before BucketAssignmentEntryStart (FlatBuffers A1 rule).
func buildBucketAssignmentsVector(b *flatbuffers.Builder, buckets []bucketSnapshotKV) flatbuffers.UOffsetT {
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
	return prependOffsetsVector(b, baOffs)
}

func buildLoadSnapshotVector(b *flatbuffers.Builder, lsEntries []LoadStatEntry) flatbuffers.UOffsetT {
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
	return prependOffsetsVector(b, lsOffs)
}

// buildLastRotationRequestEntriesVector encodes the KEK rotation request ring
// (Phase B Task 2).
func buildLastRotationRequestEntriesVector(b *flatbuffers.Builder, lastRotationRequests []rotationRequestRecord) flatbuffers.UOffsetT {
	lrrOffs := make([]flatbuffers.UOffsetT, len(lastRotationRequests))
	for i := len(lastRotationRequests) - 1; i >= 0; i-- {
		r := lastRotationRequests[i]
		ridVec := b.CreateByteVector(r.requestID[:])
		clusterpb.LastRotationRequestEntryStart(b)
		clusterpb.LastRotationRequestEntryAddRequestId(b, ridVec)
		clusterpb.LastRotationRequestEntryAddStatus(b, byte(r.status))
		clusterpb.LastRotationRequestEntryAddApplyIndex(b, r.applyIndex)
		lrrOffs[i] = clusterpb.LastRotationRequestEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartLastRotationRequestEntriesVector(b, len(lrrOffs))
	return prependOffsetsVector(b, lrrOffs)
}

// buildKEKStatusEntriesVector encodes the per-version KEK lifecycle status
// slice (Phase B Task 2).
func buildKEKStatusEntriesVector(b *flatbuffers.Builder, kekStatuses []kekStatusRecord) flatbuffers.UOffsetT {
	kekOffs := make([]flatbuffers.UOffsetT, len(kekStatuses))
	for i := len(kekStatuses) - 1; i >= 0; i-- {
		e := kekStatuses[i]
		clusterpb.KEKStatusEntryStart(b)
		clusterpb.KEKStatusEntryAddVersion(b, e.version)
		clusterpb.KEKStatusEntryAddStatus(b, byte(e.status))
		clusterpb.KEKStatusEntryAddRetireCommitIndex(b, e.retireCommitIndex)
		kekOffs[i] = clusterpb.KEKStatusEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartKekStatusEntriesVector(b, len(kekOffs))
	return prependOffsetsVector(b, kekOffs)
}

// buildPeersVector encodes the zero-CA peer registry (Task 5). Nested
// strings/vectors must be created before PeerEntryStart (A1).
func buildPeersVector(b *flatbuffers.Builder, peersCopy []peerEntry) flatbuffers.UOffsetT {
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
	return prependOffsetsVector(b, peerOffs)
}

func buildRevokedPeerSpkisVector(b *flatbuffers.Builder, spkis [][32]byte) flatbuffers.UOffsetT {
	denyOffs := make([]flatbuffers.UOffsetT, len(spkis))
	for i := len(spkis) - 1; i >= 0; i-- {
		vec := b.CreateByteVector(spkis[i][:])
		clusterpb.SPKIBytesStart(b)
		clusterpb.SPKIBytesAddValue(b, vec)
		denyOffs[i] = clusterpb.SPKIBytesEnd(b)
	}
	clusterpb.MetaStateSnapshotStartRevokedPeerSpkisVector(b, len(denyOffs))
	return prependOffsetsVector(b, denyOffs)
}

// buildRevokedNodeIdsVector encodes the zero-CA revoked-node set (slot 17),
// sorted for determinism. Nested strings must be created before the vector
// start. NOTE: sorts revokedIDs in-place. revoked_peer_spkis (the deny set) is
// NOT sorted — exportDenylist returns unsorted map iteration — so this is
// sorted for its own determinism, not to mirror that section.
func buildRevokedNodeIdsVector(b *flatbuffers.Builder, revokedIDs []string) flatbuffers.UOffsetT {
	sort.Strings(revokedIDs)
	revokedOffs := make([]flatbuffers.UOffsetT, len(revokedIDs))
	for i, id := range revokedIDs {
		revokedOffs[i] = b.CreateString(id)
	}
	clusterpb.MetaStateSnapshotStartRevokedNodeIdsVector(b, len(revokedOffs))
	return prependOffsetsVector(b, revokedOffs)
}
