package storage

// ChunkLocators returns the canonical chunk-locator strings for every chunk the
// object's manifest references — both Segments and Coalesced blobs. Each locator
// is normalized through ParseLocator/String so callers get the same opaque
// chunkref.ChunkID key regardless of legacy/cas:// scheme form. Returns nil for
// a manifest with no chunks.
func (o *Object) ChunkLocators() []string {
	return chunkLocators(o.Segments, o.Coalesced)
}

// ChunkLocators mirrors (*Object).ChunkLocators for the point-in-time snapshot
// record, covering both Segments and Coalesced so snapshot freeze pins every
// chunk the object references.
func (o *SnapshotObject) ChunkLocators() []string {
	return chunkLocators(o.Segments, o.Coalesced)
}

func chunkLocators(segments []SegmentRef, coalesced []CoalescedRef) []string {
	n := len(segments) + len(coalesced)
	if n == 0 {
		return nil
	}
	out := make([]string, 0, n)
	for _, s := range segments {
		out = append(out, ParseLocator(s.BlobID).String())
	}
	for _, c := range coalesced {
		out = append(out, ParseLocator(c.CoalescedID).String())
	}
	return out
}
