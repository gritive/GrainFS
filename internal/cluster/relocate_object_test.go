package cluster

import (
	"errors"
	"testing"
)

func TestRelocationStillEligible(t *testing.T) {
	const (
		etag = "etag-abc"
		ver  = "ver-123"
	)
	base := relocateInput{Bucket: "b", Key: "k", VersionID: ver, ExpectedETag: etag}

	tests := []struct {
		name             string
		cur              PutObjectMetaCmd
		in               relocateInput
		clusterRedundant bool
		wantErr          bool
	}{
		{
			name:             "eligible non-redundant matching etag/version redundant cluster",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          false,
		},
		{
			name:             "already redundant parity>0",
			cur:              PutObjectMetaCmd{ECData: 4, ECParity: 2, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "etag mismatch",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: "stale", VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "version mismatch",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: "other"},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "delete marker",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver, IsDeleteMarker: true},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
		{
			name:             "cluster not redundant",
			cur:              PutObjectMetaCmd{ECData: 1, ECParity: 0, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: false,
			wantErr:          true,
		},
		{
			name:             "data shards zero",
			cur:              PutObjectMetaCmd{ECData: 0, ECParity: 0, ETag: etag, VersionID: ver},
			in:               base,
			clusterRedundant: true,
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := relocationStillEligible(tt.cur, tt.in, tt.clusterRedundant)
			if tt.wantErr {
				if !errors.Is(err, ErrRelocateSkipped) {
					t.Fatalf("want ErrRelocateSkipped, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}
