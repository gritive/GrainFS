package audit_test

import (
	"context"
	"strings"
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

// TestQueryRejectsComments — F37 defense-in-depth: -- and /* comment syntax
// must be rejected before any SQL is sent to DuckDB.
func TestQueryRejectsComments(t *testing.T) {
	// Use a nil searcher to exercise the guard path without a live DuckDB.
	// Query returns "not configured" before it reaches the comment check.
	// We need a configured-but-offline searcher — use the query guard directly
	// via a table-driven check on the BuildSearchSQL helper, or verify the
	// guard order by checking the exported Query on a non-nil searcher.
	// Since the searcher requires a running S3/Iceberg, we test the guard
	// at the string level using the same logic mirror the package exposes.
	//
	// The canonical test: use the internal package test to call the unexported
	// guard, or use the exported Query and assert the error message.
	// We use a searcher with a non-empty Endpoint so it reaches the guard.
	s := audit.NewDuckDBSearcher(audit.DuckDBSearchConfig{
		Endpoint:  "http://127.0.0.1:1",
		AccessKey: "k",
		SecretKey: "s",
	})
	ctx := context.Background()

	cases := []struct {
		name    string
		sql     string
		wantErr string
	}{
		// F37: comment guards must reject before reaching the database.
		// Note: use queries without semicolons so the comment check fires
		// rather than the earlier semicolon check.
		{"line comment no semicolon", "SELECT 1 -- EVIL", "SQL comments (--)"},
		{"block comment open", "SELECT /* hidden */ 1", "SQL comments (/*)"},
		// Valid queries should not be rejected by any guard.
		{"select ok", "SELECT 1", ""},
		{"with ok", "WITH cte AS (SELECT 1) SELECT * FROM cte", ""},
		{"single minus ok", "SELECT a - b FROM t", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := s.Query(ctx, tc.sql, 1)
			if tc.wantErr == "" {
				// For valid queries we expect a dial/network error (not a guard error).
				if err != nil && (containsAny(err.Error(), "SQL comments", "semicolons", "only SELECT")) {
					t.Fatalf("valid query should not be rejected by guard: %v", err)
				}
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr, "expected guard rejection message")
			}
		})
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
