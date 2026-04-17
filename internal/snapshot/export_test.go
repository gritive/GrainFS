package snapshot

// RunPruneOld exposes pruneOld for white-box testing.
func RunPruneOld(a *AutoSnapshotter) {
	a.pruneOld()
}
