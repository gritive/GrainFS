package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
			require.Equal(t, tt.wantScheme, loc.Scheme, "scheme")
			require.Equal(t, tt.wantRef, loc.Ref, "ref")
		})
	}
}

func TestLocalOpenSegmentRejectsCAS(t *testing.T) {
	b := newTestLocalBackend(t)
	store := localSegmentStore{b: b, bucket: "bkt", key: "obj"}
	_, err := store.OpenSegment(context.Background(), SegmentRef{BlobID: "cas://b3-deadbeef", Size: 16})
	require.ErrorIs(t, err, ErrCASNotImplemented)
}

func TestParseLocatorEdgeCases(t *testing.T) {
	tests := []struct {
		blobID     string
		wantScheme LocatorScheme
		wantRef    string
	}{
		{"cas://", LocatorCAS, ""},
		{"legacy://", LocatorLegacy, ""},
		{"legacy://bucket/key/", LocatorLegacy, ""},
	}
	for _, tt := range tests {
		t.Run(tt.blobID, func(t *testing.T) {
			loc := ParseLocator(tt.blobID)
			require.Equal(t, tt.wantScheme, loc.Scheme, "scheme")
			require.Equal(t, tt.wantRef, loc.Ref, "ref")
		})
	}
}

func TestSegmentKnownPath(t *testing.T) {
	got := SegmentKnownPath("b1", "dir/obj", "01HXYZ-raw")
	want := "b1/dir/obj_segments/" + ParseLocator("01HXYZ-raw").Ref
	require.Equal(t, want, got)
}

func TestLocatorStringRoundTrip(t *testing.T) {
	loc := Locator{Scheme: LocatorCAS, Ref: "b3-0011223344556677"}
	require.Equal(t, "cas://b3-0011223344556677", loc.String())
	back := ParseLocator(loc.String())
	require.Equal(t, LocatorCAS, back.Scheme)
	require.Equal(t, loc.Ref, back.Ref)
	bare := "0192f3c0-aaaa-7bbb-8ccc-000000000001"
	require.Equal(t, bare, ParseLocator(bare).String())
}
