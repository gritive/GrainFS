package cluster

import (
	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/storage"
)

// ManifestsFromSnapshotObjects converts the authoritative object-version manifest
// set (as returned by ListAllObjects) into chunkref.Manifest inputs for
// chunkref.Rebuild. Cluster keeps no incremental refcount in the meta-Raft FSM
// (ObjectIndexEntry carries no Segments); the RefTable is rebuilt on demand from
// this manifest scan (spec: manifest = truth, refcount = rebuildable cache).
// Delete markers carry no chunks, so they contribute an empty manifest.
func ManifestsFromSnapshotObjects(objs []storage.SnapshotObject) []chunkref.Manifest {
	manifests := make([]chunkref.Manifest, 0, len(objs))
	for i := range objs {
		o := &objs[i]
		locs := o.ChunkLocators()
		chunks := make([]chunkref.ChunkID, 0, len(locs))
		for _, l := range locs {
			chunks = append(chunks, chunkref.ChunkID(l))
		}
		manifests = append(manifests, chunkref.Manifest{
			ID:     chunkref.ObjectVersionID(o.Bucket, o.Key, o.VersionID),
			Chunks: chunks,
		})
	}
	return manifests
}
