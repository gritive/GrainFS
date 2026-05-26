package storage

import (
	"context"
	"errors"
	"testing"
)

func TestParseLocator(t *testing.T) {
	tests := []struct {
		name       string
		blobID     string
		wantScheme LocatorScheme
		wantRef    string
	}{
		{"implicit legacy uuid", "0192f3c0-aaaa-7bbb-8ccc-000000000001", LocatorLegacy, "0192f3c0-aaaa-7bbb-8ccc-000000000001"},
		{"explicit legacy prefix", "legacy://bucket/key/0192f3c0-aaaa-7bbb-8ccc-000000000001", LocatorLegacy, "0192f3c0-aaaa-7bbb-8ccc-000000000001"},
		{"cas locator", "cas://b3-0011223344556677", LocatorCAS, "b3-0011223344556677"},
		{"empty is legacy", "", LocatorLegacy, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc := ParseLocator(tt.blobID)
			if loc.Scheme != tt.wantScheme {
				t.Fatalf("scheme = %v, want %v", loc.Scheme, tt.wantScheme)
			}
			if loc.Ref != tt.wantRef {
				t.Fatalf("ref = %q, want %q", loc.Ref, tt.wantRef)
			}
		})
	}
}

func TestLocalOpenSegmentRejectsCAS(t *testing.T) {
	b := newTestLocalBackend(t)
	store := localSegmentStore{b: b, bucket: "bkt", key: "obj"}
	_, err := store.OpenSegment(context.Background(), SegmentRef{BlobID: "cas://b3-deadbeef", Size: 16})
	if !errors.Is(err, ErrCASNotImplemented) {
		t.Fatalf("err = %v, want ErrCASNotImplemented", err)
	}
}

func TestLocatorStringRoundTrip(t *testing.T) {
	loc := Locator{Scheme: LocatorCAS, Ref: "b3-0011223344556677"}
	if got := loc.String(); got != "cas://b3-0011223344556677" {
		t.Fatalf("CAS String() = %q", got)
	}
	if back := ParseLocator(loc.String()); back.Scheme != LocatorCAS || back.Ref != loc.Ref {
		t.Fatalf("CAS round-trip failed: %+v", back)
	}
	bare := "0192f3c0-aaaa-7bbb-8ccc-000000000001"
	if got := ParseLocator(bare).String(); got != bare {
		t.Fatalf("legacy String() = %q, want bare %q", got, bare)
	}
}
