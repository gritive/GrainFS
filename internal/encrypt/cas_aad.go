package encrypt

// CASChunkAAD builds the object-independent AAD for a content-addressed
// canonical chunk. It is keyed only by (cluster_id, canonicalLocator): no
// bucket, key, or shard index participates, so every object that references
// the same canonical chunk derives identical AAD and can decrypt the single
// stored copy.
//
// canonicalLocator is the raw BLAKE3 digest of the chunk plaintext. Deriving
// it is the dedup convergence pass's responsibility (Plan 4c); this helper is
// agnostic to how the bytes were produced.
//
// clusterID MUST be 16 bytes (BuildAAD panics otherwise — programmer error).
func CASChunkAAD(clusterID, canonicalLocator []byte) []byte {
	return BuildAAD(DomainCASChunk, clusterID, FieldBytes(canonicalLocator))
}
