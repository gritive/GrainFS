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

// encodeMetaDEKVersionSnapshot serializes the DEK versions map + active gen into
// a MetaDEKVersionSnapshot FlatBuffers buffer used as the DKVS trailer payload.
// Entries are emitted in ascending gen order for byte-determinism across replicas.
func encodeMetaDEKVersionSnapshot(versions map[uint32][]byte, active uint32) ([]byte, error) {
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

	clusterpb.MetaDEKVersionSnapshotStart(b)
	clusterpb.MetaDEKVersionSnapshotAddVersions(b, versionsVec)
	clusterpb.MetaDEKVersionSnapshotAddActive(b, active)
	return fbFinish(b, clusterpb.MetaDEKVersionSnapshotEnd(b)), nil
}

// decodeMetaDEKVersionSnapshot parses a MetaDEKVersionSnapshot FlatBuffers buffer
// and returns the versions map and active generation.
func decodeMetaDEKVersionSnapshot(data []byte) (versions map[uint32][]byte, active uint32, err error) {
	snap, err := fbSafe(data, func(d []byte) *clusterpb.MetaDEKVersionSnapshot {
		return clusterpb.GetRootAsMetaDEKVersionSnapshot(d, 0)
	})
	if err != nil {
		return nil, 0, fmt.Errorf("dek_codec: MetaDEKVersionSnapshot: %w", err)
	}

	out := make(map[uint32][]byte, snap.VersionsLength())
	var entry clusterpb.DEKVersionEntry
	for i := 0; i < snap.VersionsLength(); i++ {
		if snap.Versions(&entry, i) {
			wrapped := append([]byte(nil), entry.WrappedBytes()...)
			out[entry.Gen()] = wrapped
		}
	}
	return out, snap.Active(), nil
}
