package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeMetaDEKVersionPruneCmd serializes a DEKVersionPrune payload.
func encodeMetaDEKVersionPruneCmd(gen uint32) ([]byte, error) {
	b := clusterBuilderPool.Get()
	clusterpb.MetaDEKVersionPruneCmdStart(b)
	clusterpb.MetaDEKVersionPruneCmdAddGen(b, gen)
	return fbFinish(b, clusterpb.MetaDEKVersionPruneCmdEnd(b)), nil
}

// decodeMetaDEKVersionPruneCmd parses the inner DEKVersionPrune payload bytes.
func decodeMetaDEKVersionPruneCmd(data []byte) (gen uint32, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaDEKVersionPruneCmd {
		return clusterpb.GetRootAsMetaDEKVersionPruneCmd(d, 0)
	})
	if err != nil {
		return 0, fmt.Errorf("dek_codec: MetaDEKVersionPruneCmd: %w", err)
	}
	return t.Gen(), nil
}

// encodeMetaDEKVersionSnapshot serializes the DEK versions map + active gen +
// per-generation ref counts + active KEK version + rewrap done set into a
// MetaDEKVersionSnapshot FlatBuffers buffer used as the DKVS trailer payload.
// Entries are emitted in ascending gen order for byte-determinism across
// replicas. refCounts may be nil (emits empty list). activeKEKVersion defaults
// to 0 in Phase A (no rotation yet). rewrapDone may be nil (emits empty list).
func encodeMetaDEKVersionSnapshot(versions map[uint32][]byte, active uint32, refCounts map[uint32]uint64, activeKEKVersion uint32, rewrapDone map[uint32]map[string]struct{}) ([]byte, error) {
	b := clusterBuilderPool.Get()

	// Sort gens for deterministic output.
	gens := make([]uint32, 0, len(versions))
	for g := range versions {
		gens = append(gens, g)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })

	// Build DEKVersionEntry offsets in reverse order (FlatBuffers convention).
	entryOffs := make([]flatbuffers.UOffsetT, len(gens))
	for i := len(gens) - 1; i >= 0; i-- {
		g := gens[i]
		wrappedOff := b.CreateByteVector(versions[g])
		clusterpb.DEKVersionEntryStart(b)
		clusterpb.DEKVersionEntryAddGen(b, g)
		clusterpb.DEKVersionEntryAddWrapped(b, wrappedOff)
		entryOffs[i] = clusterpb.DEKVersionEntryEnd(b)
	}

	clusterpb.MetaDEKVersionSnapshotStartVersionsVector(b, len(entryOffs))
	for i := len(entryOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(entryOffs[i])
	}
	versionsVec := b.EndVector(len(entryOffs))

	// Build DEKRefEntry offsets for per-gen ref counts (ascending gen order).
	refGens := make([]uint32, 0, len(refCounts))
	for g := range refCounts {
		if refCounts[g] > 0 {
			refGens = append(refGens, g)
		}
	}
	sort.Slice(refGens, func(i, j int) bool { return refGens[i] < refGens[j] })
	refOffs := make([]flatbuffers.UOffsetT, len(refGens))
	for i := len(refGens) - 1; i >= 0; i-- {
		g := refGens[i]
		clusterpb.DEKRefEntryStart(b)
		clusterpb.DEKRefEntryAddGen(b, g)
		clusterpb.DEKRefEntryAddCount(b, int64(refCounts[g]))
		refOffs[i] = clusterpb.DEKRefEntryEnd(b)
	}
	clusterpb.MetaDEKVersionSnapshotStartRefCountsVector(b, len(refOffs))
	for i := len(refOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(refOffs[i])
	}
	refVec := b.EndVector(len(refOffs))

	// Build DEKRewrapDoneEntry offsets (ascending gen order; node_ids sorted
	// within each entry for byte-determinism — mirrors refGens sort above).
	doneGens := make([]uint32, 0, len(rewrapDone))
	for g := range rewrapDone {
		if len(rewrapDone[g]) > 0 {
			doneGens = append(doneGens, g)
		}
	}
	sort.Slice(doneGens, func(i, j int) bool { return doneGens[i] < doneGens[j] })
	doneOffs := make([]flatbuffers.UOffsetT, len(doneGens))
	for i := len(doneGens) - 1; i >= 0; i-- {
		g := doneGens[i]
		nodeSet := rewrapDone[g]
		nodeIDs := make([]string, 0, len(nodeSet))
		for n := range nodeSet {
			nodeIDs = append(nodeIDs, n)
		}
		sort.Strings(nodeIDs)
		// Build string offsets for node_ids (must precede DEKRewrapDoneEntryStart).
		nodeOffs := make([]flatbuffers.UOffsetT, len(nodeIDs))
		for j, id := range nodeIDs {
			nodeOffs[j] = b.CreateString(id)
		}
		clusterpb.DEKRewrapDoneEntryStartNodeIdsVector(b, len(nodeOffs))
		for j := len(nodeOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(nodeOffs[j])
		}
		nodeVec := b.EndVector(len(nodeOffs))
		clusterpb.DEKRewrapDoneEntryStart(b)
		clusterpb.DEKRewrapDoneEntryAddGen(b, g)
		clusterpb.DEKRewrapDoneEntryAddNodeIds(b, nodeVec)
		doneOffs[i] = clusterpb.DEKRewrapDoneEntryEnd(b)
	}
	clusterpb.MetaDEKVersionSnapshotStartRewrapDoneVector(b, len(doneOffs))
	for i := len(doneOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(doneOffs[i])
	}
	rewrapDoneVec := b.EndVector(len(doneOffs))

	clusterpb.MetaDEKVersionSnapshotStart(b)
	clusterpb.MetaDEKVersionSnapshotAddVersions(b, versionsVec)
	clusterpb.MetaDEKVersionSnapshotAddActive(b, active)
	clusterpb.MetaDEKVersionSnapshotAddRefCounts(b, refVec)
	clusterpb.MetaDEKVersionSnapshotAddActiveKekVersion(b, activeKEKVersion)
	clusterpb.MetaDEKVersionSnapshotAddRewrapDone(b, rewrapDoneVec)
	return fbFinish(b, clusterpb.MetaDEKVersionSnapshotEnd(b)), nil
}

// encodeMetaDEKRewrapProgressCmd builds the inner payload for a per-node
// rewrap-completion report: "nodeID finished rewrapping all its lanes for gen".
func encodeMetaDEKRewrapProgressCmd(nodeID string, gen uint32) ([]byte, error) {
	b := clusterBuilderPool.Get()
	nid := b.CreateString(nodeID)
	clusterpb.MetaDEKRewrapProgressCmdStart(b)
	clusterpb.MetaDEKRewrapProgressCmdAddNodeId(b, nid)
	clusterpb.MetaDEKRewrapProgressCmdAddGen(b, gen)
	return fbFinish(b, clusterpb.MetaDEKRewrapProgressCmdEnd(b)), nil
}

// decodeMetaDEKRewrapProgressCmd parses the inner DEKRewrapProgress payload bytes.
func decodeMetaDEKRewrapProgressCmd(data []byte) (nodeID string, gen uint32, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaDEKRewrapProgressCmd {
		return clusterpb.GetRootAsMetaDEKRewrapProgressCmd(d, 0)
	})
	if err != nil {
		return "", 0, fmt.Errorf("dek_codec: MetaDEKRewrapProgressCmd: %w", err)
	}
	return string(t.NodeId()), t.Gen(), nil
}

// decodeMetaDEKVersionSnapshot parses a MetaDEKVersionSnapshot FlatBuffers buffer
// and returns the versions map, active generation, per-generation ref counts,
// active KEK version, and per-generation rewrap done sets. refCounts is nil when
// the field was absent (pre-Task-12 snapshot backward compat). activeKEKVersion
// is 0 when the field was absent (pre-Phase-A snapshot backward compat).
// rewrapDone is nil when the field was absent (pre-S6d snapshot backward compat).
func decodeMetaDEKVersionSnapshot(data []byte) (versions map[uint32][]byte, active uint32, refCounts map[uint32]uint64, activeKEKVersion uint32, rewrapDone map[uint32]map[string]struct{}, err error) {
	snap, err := fbSafe(data, func(d []byte) *clusterpb.MetaDEKVersionSnapshot {
		return clusterpb.GetRootAsMetaDEKVersionSnapshot(d, 0)
	})
	if err != nil {
		return nil, 0, nil, 0, nil, fmt.Errorf("dek_codec: MetaDEKVersionSnapshot: %w", err)
	}

	out := make(map[uint32][]byte, snap.VersionsLength())
	var entry clusterpb.DEKVersionEntry
	for i := 0; i < snap.VersionsLength(); i++ {
		if snap.Versions(&entry, i) {
			wrapped := append([]byte(nil), entry.WrappedBytes()...)
			out[entry.Gen()] = wrapped
		}
	}

	var refs map[uint32]uint64
	if n := snap.RefCountsLength(); n > 0 {
		refs = make(map[uint32]uint64, n)
		var refEntry clusterpb.DEKRefEntry
		for i := 0; i < n; i++ {
			if !snap.RefCounts(&refEntry, i) {
				continue
			}
			// Reject negative counts (tampered or bit-flipped snapshot). int64→uint64
			// cast on a negative value would store 0xFFFFFFFFFFFFFFFF, permanently
			// pinning the DEK ref count above zero and blocking prune of that
			// generation forever (DEK leak). Drop the entry; rebuild from
			// objectIndex will fix it on the next restore path.
			if c := refEntry.Count(); c >= 0 {
				refs[refEntry.Gen()] = uint64(c)
			}
		}
	}

	var done map[uint32]map[string]struct{}
	if n := snap.RewrapDoneLength(); n > 0 {
		done = make(map[uint32]map[string]struct{}, n)
		var doneEntry clusterpb.DEKRewrapDoneEntry
		for i := 0; i < n; i++ {
			if !snap.RewrapDone(&doneEntry, i) {
				continue
			}
			g := doneEntry.Gen()
			nodeSet := make(map[string]struct{}, doneEntry.NodeIdsLength())
			for j := 0; j < doneEntry.NodeIdsLength(); j++ {
				id := string(doneEntry.NodeIds(j))
				if id != "" {
					nodeSet[id] = struct{}{}
				}
			}
			if len(nodeSet) > 0 {
				done[g] = nodeSet
			}
		}
	}
	return out, snap.Active(), refs, snap.ActiveKekVersion(), done, nil
}
