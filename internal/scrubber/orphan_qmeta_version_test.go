package scrubber

import "testing"

type fakeQMetaVersionWalker struct {
	candidates [][3]string // {bucket,key,vid}
	deleted    [][3]string
}

func (f *fakeQMetaVersionWalker) WalkOrphanQuorumMetaVersions(fn func(b, k, v, p string) error) error {
	for _, c := range f.candidates {
		if err := fn(c[0], c[1], c[2], "/p/"+c[2]); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeQMetaVersionWalker) DeleteOrphanQuorumMetaVersion(b, k, v string) error {
	f.deleted = append(f.deleted, [3]string{b, k, v})
	return nil
}

func newSweepScrubber() *BackgroundScrubber {
	return &BackgroundScrubber{orphanVersionTombstone: map[string]struct{}{}}
}

func TestOrphanVersionSweep_TwoCycleDelay(t *testing.T) {
	w := &fakeQMetaVersionWalker{candidates: [][3]string{{"bkt", "k", "vA"}}}
	s := newSweepScrubber()

	s.orphanVersionSweep(w) // cycle 1: tombstone only, no delete
	if len(w.deleted) != 0 {
		t.Fatalf("cycle 1 must not delete, deleted=%v", w.deleted)
	}
	s.orphanVersionSweep(w) // cycle 2: still orphan → delete
	if len(w.deleted) != 1 || w.deleted[0] != [3]string{"bkt", "k", "vA"} {
		t.Fatalf("cycle 2 must delete vA, got %v", w.deleted)
	}
}

func TestOrphanVersionSweep_RecoveredBeforeSecondCycle(t *testing.T) {
	w := &fakeQMetaVersionWalker{candidates: [][3]string{{"bkt", "k", "vR"}}}
	s := newSweepScrubber()

	s.orphanVersionSweep(w) // tombstone vR
	w.candidates = nil      // vR recovered (record re-created / blob already gone)
	s.orphanVersionSweep(w) // must clear tombstone, not delete
	if len(w.deleted) != 0 {
		t.Fatalf("recovered candidate must not be deleted, got %v", w.deleted)
	}
	if _, still := s.orphanVersionTombstone[versionTombstoneKey("bkt", "k", "vR")]; still {
		t.Fatalf("recovered candidate's tombstone must be cleared")
	}
}

func TestVersionTombstoneKeyRoundTrip(t *testing.T) {
	// keys containing '/' must round-trip (the \x00 separator is unambiguous).
	b, k, v := splitVersionTombstoneKey(versionTombstoneKey("bkt", "a/b/c.txt", "vid-1"))
	if b != "bkt" || k != "a/b/c.txt" || v != "vid-1" {
		t.Fatalf("round-trip mismatch: %q %q %q", b, k, v)
	}
}
