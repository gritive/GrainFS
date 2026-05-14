package audit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

const MaxSearchLimit = 500

type SearchFilter struct {
	Since       time.Time
	Until       time.Time
	Bucket      string
	KeyPrefix   string
	SAID        string
	Operation   string
	Status      int
	StatusClass int
	ErrClass    string
	RequestID   string
	Limit       int
}

type SearchRow struct {
	Ts        time.Time `json:"ts"`
	RequestID string    `json:"request_id"`
	SAID      string    `json:"sa_id"`
	SourceIP  string    `json:"source_ip"`
	UserAgent string    `json:"user_agent"`
	Operation string    `json:"operation"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	Status    int       `json:"http_status"`
	ErrClass  string    `json:"err_class"`
	ErrReason string    `json:"err_reason"`
	LatencyMs int       `json:"latency_ms"`
}

type DuckDBSearchConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

type DuckDBSearcher struct {
	cfg DuckDBSearchConfig
}

func NewDuckDBSearcher(cfg DuckDBSearchConfig) *DuckDBSearcher {
	return &DuckDBSearcher{cfg: cfg}
}

func ClampSearchLimit(limit int) int {
	if limit <= 0 || limit > MaxSearchLimit {
		return MaxSearchLimit
	}
	return limit
}

func BuildSearchSQL(f SearchFilter) (string, []any) {
	limit := ClampSearchLimit(f.Limit)
	clauses := []string{"1=1"}
	args := []any{}
	if !f.Since.IsZero() {
		clauses = append(clauses, "ts >= ?")
		args = append(args, f.Since)
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "ts < ?")
		args = append(args, f.Until)
	}
	if f.Bucket != "" {
		clauses = append(clauses, "bucket = ?")
		args = append(args, f.Bucket)
	}
	if f.KeyPrefix != "" {
		clauses = append(clauses, "key LIKE ?")
		args = append(args, f.KeyPrefix+"%")
	}
	if f.SAID != "" {
		clauses = append(clauses, "sa_id = ?")
		args = append(args, f.SAID)
	}
	if f.Operation != "" {
		clauses = append(clauses, "operation = ?")
		args = append(args, f.Operation)
	}
	if f.Status != 0 {
		clauses = append(clauses, "http_status = ?")
		args = append(args, f.Status)
	}
	if f.StatusClass != 0 {
		clauses = append(clauses, "http_status >= ? AND http_status < ?")
		args = append(args, f.StatusClass, f.StatusClass+100)
	}
	if f.ErrClass != "" {
		clauses = append(clauses, "err_class = ?")
		args = append(args, f.ErrClass)
	}
	if f.RequestID != "" {
		clauses = append(clauses, "request_id = ?")
		args = append(args, f.RequestID)
	}
	q := fmt.Sprintf(`SELECT ts, request_id, sa_id, source_ip, user_agent, operation, bucket, key, http_status, err_class, err_reason, latency_ms
FROM grainfs_iceberg.audit.s3
WHERE %s
ORDER BY ts DESC
LIMIT %d`, strings.Join(clauses, " AND "), limit)
	return q, args
}

func (s *DuckDBSearcher) SearchS3(ctx context.Context, f SearchFilter) ([]SearchRow, error) {
	if s == nil || s.cfg.Endpoint == "" {
		return nil, fmt.Errorf("audit searcher is not configured")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, s.setupSQL()); err != nil {
		return nil, err
	}

	q, args := BuildSearchSQL(f)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []SearchRow{}
	for rows.Next() {
		var row SearchRow
		if err := rows.Scan(
			&row.Ts,
			&row.RequestID,
			&row.SAID,
			&row.SourceIP,
			&row.UserAgent,
			&row.Operation,
			&row.Bucket,
			&row.Key,
			&row.Status,
			&row.ErrClass,
			&row.ErrReason,
			&row.LatencyMs,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *DuckDBSearcher) setupSQL() string {
	endpointHost := strings.TrimPrefix(strings.TrimPrefix(s.cfg.Endpoint, "http://"), "https://")
	useSSL := strings.HasPrefix(s.cfg.Endpoint, "https://")
	return fmt.Sprintf(`
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;
CREATE OR REPLACE SECRET grainfs_s3 (
	TYPE s3,
	KEY_ID '%s',
	SECRET '%s',
	REGION 'us-east-1',
	ENDPOINT '%s',
	URL_STYLE 'path',
	USE_SSL %t
);
ATTACH 'grainfs' AS grainfs_iceberg (
	TYPE iceberg,
	ENDPOINT '%s/iceberg',
	AUTHORIZATION_TYPE 'none',
	ACCESS_DELEGATION_MODE 'none'
);
`, sqlString(s.cfg.AccessKey), sqlString(s.cfg.SecretKey), sqlString(endpointHost), useSSL, sqlString(s.cfg.Endpoint))
}

func sqlString(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}
