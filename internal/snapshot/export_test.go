package snapshot

// RunPruneOld exposes pruneOld for white-box testing.
func RunPruneOld(a *AutoSnapshotter, maxRetain int) {
	a.pruneOld(maxRetain)
}

// WriteSnapshotForTest exposes writeSnapshot for tests that need to craft
// custom on-disk snapshot payloads (e.g. simulating older formats).
func WriteSnapshotForTest(m *Manager, snap *Snapshot) error {
	return writeSnapshot(m.path(snap.Seq), snap)
}
