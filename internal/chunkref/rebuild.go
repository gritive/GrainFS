package chunkref

// Manifest is the authoritative input for rebuilding a RefTable: a manifest's
// identity plus the chunks it references. The manifest set is the source of
// truth; RefTable is a derived cache.
type Manifest struct {
	ID     ManifestID
	Chunks []ChunkID
}

// Rebuild constructs a RefTable from the authoritative manifest set. The result
// is equivalent to applying AddRef for every (manifest, chunk) pair.
//
// A rebuilt table reflects only live manifests, so it contains NO tombstones: a
// chunk absent from every manifest simply does not appear (refcount 0, no
// t_zero). Tombstones are a runtime artifact of RemoveRef transitions and are
// intentionally not reconstructed — GC after a rebuild relies on the
// authoritative manifest-absence re-check, not a reconstructed t_zero.
func Rebuild(manifests []Manifest) *RefTable {
	t := NewRefTable()
	for _, mf := range manifests {
		for _, c := range mf.Chunks {
			t.AddRef(mf.ID, c)
		}
	}
	return t
}
