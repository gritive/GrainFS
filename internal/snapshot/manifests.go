package snapshot

import (
	"github.com/gritive/GrainFS/internal/chunkref"
)

// ManifestsFromSnapshots converts live snapshot descriptors into chunkref.Manifest
// inputs (ManifestID = SnapshotID(seq)). Concatenated with the live-version
// manifests, the rebuilt RefTable keeps chunks frozen by a snapshot referenced
// even after the live object is deleted — preventing retention-window GC from
// reclaiming chunks a snapshot still needs.
func ManifestsFromSnapshots(snaps []*Snapshot) []chunkref.Manifest {
	manifests := make([]chunkref.Manifest, 0, len(snaps))
	for _, s := range snaps {
		mid := chunkref.SnapshotID(s.Seq)
		for i := range s.Objects {
			locs := s.Objects[i].ChunkLocators()
			chunks := make([]chunkref.ChunkID, 0, len(locs))
			for _, l := range locs {
				chunks = append(chunks, chunkref.ChunkID(l))
			}
			manifests = append(manifests, chunkref.Manifest{ID: mid, Chunks: chunks})
		}
	}
	return manifests
}
