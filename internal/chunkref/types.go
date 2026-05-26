// Package chunkref provides the chunk-layer reference primitive shared by
// snapshot, PITR, and dedup: an idempotent (manifestID, chunkID) reference
// table plus a t_zero GC tombstone registry.
//
// refcount is a DERIVED value (the number of distinct ManifestIDs referencing a
// chunk). The authoritative source of truth is the manifest set; RefTable is a
// rebuildable cache (see Rebuild). This is the cluster-consistency-model
// contract from the design spec: manifest = strongly consistent truth,
// refcount/index = eventually-consistent derived cache.
package chunkref

// ManifestDomain classifies which kind of manifest holds a chunk reference.
type ManifestDomain uint8

const (
	// DomainObjectVersion is an object version's manifest (Object.Segments).
	DomainObjectVersion ManifestDomain = iota
	// DomainSnapshot is a snapshot descriptor's frozen manifest.
	DomainSnapshot
	// DomainVolumeMap is a volume block-range → chunk map (future).
	DomainVolumeMap
)

// ManifestID uniquely identifies a manifest cluster-wide.
//
// VersionID is an OPAQUE cluster-global identifier; chunkref never parses it.
// Callers MUST guarantee global uniqueness *within a domain*:
//   - DomainObjectVersion: a globally-unique key such as "<bucket>/<key>/<versionID>"
//     (the raw S3 VersionID alone is NOT unique across different (bucket,key)).
//   - DomainSnapshot: the snapshot descriptor ID.
//   - DomainVolumeMap: the volume-map ID.
//
// ManifestID is comparable and used directly as a map key.
type ManifestID struct {
	Domain    ManifestDomain
	VersionID string
}

// ChunkID is the canonical string form of a storage.Locator: a bare legacy
// BlobID (UUIDv7) or "cas://<hash>". Callers MUST pass storage.Locator.String();
// chunkref treats it as an opaque key and does not validate or parse it.
// (chunkref does not import storage, to keep the primitive decoupled.)
type ChunkID string
