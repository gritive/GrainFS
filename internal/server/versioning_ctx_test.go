package server

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// listVersionsSpy embeds a real backend and records the bucket-versioning
// decision present in the ctx handed to ListObjectVersions.
type listVersionsSpy struct {
	storage.Backend
	stateByBucket map[string]string
	seen, stamped bool
}

func (s *listVersionsSpy) GetBucketVersioning(bucket string) (string, error) {
	if st, ok := s.stateByBucket[bucket]; ok {
		return st, nil
	}
	return "Suspended", nil
}

func (s *listVersionsSpy) SetBucketVersioning(bucket, state string) error {
	s.stateByBucket[bucket] = state
	return nil
}

func (s *listVersionsSpy) ListObjectVersions(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	s.seen = true
	s.stamped = ctxVersioningEnabled(ctx)
	return nil, nil
}

// TestListObjectVersionsEdge_StampsVersionHistory proves GET /<bucket>?versions
// stamps "version history can exist" (Enabled OR Suspended) at the edge — NOT
// the Enabled-only ListObjects semantic — so the PR-B derive serves Suspended
// buckets' history. Never-versioned stamps not-history-bearing.
func TestListObjectVersionsEdge_StampsVersionHistory(t *testing.T) {
	cases := []struct {
		state string
		want  bool
	}{
		{"Enabled", true},
		{"Suspended", true}, // THE KEY CASE: Suspended has history → per-version source applies
		{"Unversioned", false},
	}
	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			real, err := storage.NewLocalBackend(t.TempDir())
			require.NoError(t, err)
			spy := &listVersionsSpy{Backend: real, stateByBucket: map[string]string{"b": tc.state}}
			srv := New("127.0.0.1:0", spy)

			if _, err := srv.loadObjectVersions(context.Background(), "b", "", 1000); err != nil {
				t.Fatalf("loadObjectVersions: %v", err)
			}
			require.True(t, spy.seen, "backend ListObjectVersions must be reached")
			require.Equal(t, tc.want, spy.stamped,
				"edge must stamp history-bearing=%v for state %q", tc.want, tc.state)
		})
	}
}
