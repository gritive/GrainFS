package chunkref

import "testing"

func TestObjectVersionIDComposesClusterGlobalKey(t *testing.T) {
	m := ObjectVersionID("bkt", "path/to/obj", "v7-abc")
	if m.Domain != DomainObjectVersion {
		t.Fatalf("Domain = %d, want DomainObjectVersion", m.Domain)
	}
	other := ObjectVersionID("bkt2", "path/to/obj", "v7-abc")
	if m == other {
		t.Fatalf("distinct (bucket,key) produced equal ManifestID: %+v", m)
	}
}

func TestObjectVersionIDEmptyVersionIsStable(t *testing.T) {
	a := ObjectVersionID("bkt", "k", "")
	b := ObjectVersionID("bkt", "k", "")
	if a != b {
		t.Fatalf("empty-version composition not deterministic: %+v vs %+v", a, b)
	}
}

func TestSnapshotID(t *testing.T) {
	m := SnapshotID(42)
	if m.Domain != DomainSnapshot {
		t.Fatalf("Domain = %d, want DomainSnapshot", m.Domain)
	}
	if m != SnapshotID(42) {
		t.Fatalf("SnapshotID not deterministic")
	}
	if m == SnapshotID(43) {
		t.Fatalf("distinct seq produced equal ManifestID")
	}
}
