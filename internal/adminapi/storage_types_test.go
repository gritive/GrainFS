package adminapi_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// TestStorageBucketSummaryJSONShape asserts the post-NFS JSON shape of
// StorageBucketSummary: only "name" and "has_upstream" fields (no nfs_export).
func TestStorageBucketSummaryJSONShape(t *testing.T) {
	s := adminapi.StorageBucketSummary{
		Name:        "my-bucket",
		HasUpstream: true,
	}
	b, err := json.Marshal(s)
	require.NoError(t, err)

	got := string(b)
	require.Contains(t, got, `"name":"my-bucket"`)
	require.Contains(t, got, `"has_upstream":true`)
	require.False(t, strings.Contains(got, "nfs_export"),
		"nfs_export field must not appear in StorageBucketSummary JSON: %s", got)
}

// TestListStorageBucketsRespJSONShape asserts the wrapping type marshals cleanly.
func TestListStorageBucketsRespJSONShape(t *testing.T) {
	r := adminapi.ListStorageBucketsResp{
		Buckets: []adminapi.StorageBucketSummary{
			{Name: "a", HasUpstream: false},
			{Name: "b", HasUpstream: true},
		},
	}
	b, err := json.Marshal(r)
	require.NoError(t, err)

	got := string(b)
	require.Contains(t, got, `"name":"a"`)
	require.Contains(t, got, `"name":"b"`)
	require.False(t, strings.Contains(got, "nfs_export"),
		"nfs_export field must not appear in ListStorageBucketsResp JSON: %s", got)
}
