package audit_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestBuildSearchSQL(t *testing.T) {
	q, args := audit.BuildSearchSQL(audit.SearchFilter{
		Since:       time.Unix(100, 0),
		Until:       time.Unix(200, 0),
		Bucket:      "photos",
		KeyPrefix:   "2026/",
		SAID:        "sa-1",
		Operation:   "GetObject",
		StatusClass: 400,
		RequestID:   "req-1",
		Limit:       25,
	})

	require.Contains(t, q, "FROM grainfs_iceberg.audit.s3")
	require.Contains(t, q, "bucket = ?")
	require.Contains(t, q, "starts_with(COALESCE(key, ''), ?)")
	require.Contains(t, q, "COALESCE(user_agent, '')")
	require.Contains(t, q, "COALESCE(latency_ms, 0)")
	require.Contains(t, q, "http_status >= ? AND http_status < ?")
	require.Contains(t, q, "LIMIT 25")
	require.Equal(t, []any{
		time.Unix(100, 0), time.Unix(200, 0), "photos", "2026/", "sa-1", "GetObject", 400, 500, "req-1",
	}, args)
}

func TestBuildSearchSQLTreatsKeyPrefixAsLiteral(t *testing.T) {
	q, args := audit.BuildSearchSQL(audit.SearchFilter{KeyPrefix: "foo_%", Limit: 1})

	require.Contains(t, q, "starts_with(COALESCE(key, ''), ?)")
	require.NotContains(t, q, "LIKE")
	require.Equal(t, []any{"foo_%"}, args)
}

func TestBuildSearchSQLClampsLimit(t *testing.T) {
	q, _ := audit.BuildSearchSQL(audit.SearchFilter{Limit: 10000})
	require.Contains(t, q, "LIMIT 500")
}
