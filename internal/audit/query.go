package audit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
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
	mu  sync.Mutex
	db  *sql.DB
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
		clauses = append(clauses, "starts_with(COALESCE(key, ''), ?)")
		args = append(args, f.KeyPrefix)
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
	q := fmt.Sprintf(`SELECT ts, COALESCE(request_id, ''), COALESCE(sa_id, ''), COALESCE(source_ip, ''), COALESCE(user_agent, ''), COALESCE(operation, ''), COALESCE(bucket, ''), COALESCE(key, ''), http_status, COALESCE(err_class, ''), COALESCE(err_reason, ''), COALESCE(latency_ms, 0)
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
	db, err := s.open(ctx)
	if err != nil {
		return nil, err
	}

	q, args := BuildSearchSQL(f)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		if !isDuckDBCatalogMiss(err) {
			return nil, err
		}
		if closeErr := s.Close(); closeErr != nil {
			return nil, closeErr
		}
		db, err = s.open(ctx)
		if err != nil {
			return nil, err
		}
		rows, err = db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
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

// Query executes a read-only SQL statement against the DuckDB/Iceberg catalog
// and returns the column names and rows as string slices. Only SELECT and WITH
// statements are accepted; multi-statement input (containing a semicolon after
// the first token) is rejected as a safety measure.
//
// The caller is responsible for imposing a LIMIT in the SQL itself; this method
// stops reading after MaxSearchLimit rows as a server-side safety net.
func (s *DuckDBSearcher) Query(ctx context.Context, rawSQL string) (columns []string, rows [][]string, err error) {
	if s == nil || s.cfg.Endpoint == "" {
		return nil, nil, fmt.Errorf("audit searcher is not configured")
	}
	stmt := strings.TrimSpace(rawSQL)
	lower := strings.ToLower(stmt)
	if !strings.HasPrefix(lower, "select") && !strings.HasPrefix(lower, "with") {
		return nil, nil, fmt.Errorf("only SELECT or WITH statements are allowed")
	}
	// Reject compound statements (simple heuristic: semicolon present outside
	// string literals). DuckDB itself accepts multi-statement strings, so we
	// guard here rather than relying on the driver.
	if strings.Contains(stmt, ";") {
		return nil, nil, fmt.Errorf("compound statements (semicolons) are not allowed")
	}

	db, err := s.open(ctx)
	if err != nil {
		return nil, nil, err
	}

	dbRows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, nil, err
	}
	defer dbRows.Close()

	columns, err = dbRows.Columns()
	if err != nil {
		return nil, nil, err
	}

	rows = [][]string{}
	for dbRows.Next() {
		if len(rows) >= MaxSearchLimit {
			break
		}
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := dbRows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make([]string, len(columns))
		for i, v := range vals {
			if v == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		rows = append(rows, row)
	}
	return columns, rows, dbRows.Err()
}

func (s *DuckDBSearcher) Warmup(ctx context.Context) error {
	_, err := s.open(ctx)
	return err
}

func (s *DuckDBSearcher) open(ctx context.Context) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return s.db, nil
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	configureDuckDBConnectionPool(db)
	if _, err := db.ExecContext(ctx, s.setupSQL()); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.db = db
	return db, nil
}

func (s *DuckDBSearcher) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func configureDuckDBConnectionPool(db *sql.DB) {
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
}

func (s *DuckDBSearcher) setupSQL() string {
	endpointHost := strings.TrimPrefix(strings.TrimPrefix(s.cfg.Endpoint, "http://"), "https://")
	useSSL := strings.HasPrefix(s.cfg.Endpoint, "https://")
	return fmt.Sprintf(`
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;
SET s3_access_key_id='%s';
SET s3_secret_access_key='%s';
SET s3_region='us-east-1';
SET s3_endpoint='%s';
SET s3_url_style='path';
SET s3_use_ssl=%t;
SET iceberg_via_aws_sdk_for_catalog_interactions=true;
CREATE OR REPLACE SECRET grainfs_s3 (
	TYPE s3,
	KEY_ID '%s',
	SECRET '%s',
	REGION 'us-east-1',
	ENDPOINT '%s',
	URL_STYLE 'path',
	USE_SSL %t
);
CREATE OR REPLACE SECRET grainfs_iceberg_oauth (
	TYPE iceberg,
	CLIENT_ID '%s',
	CLIENT_SECRET '%s',
	OAUTH2_SERVER_URI '%s/iceberg/v1/oauth/tokens',
	OAUTH2_SCOPE 'PRINCIPAL_ROLE:%s'
);
ATTACH '%s' AS grainfs_iceberg (
	TYPE iceberg,
	SECRET grainfs_iceberg_oauth,
	ENDPOINT '%s/iceberg',
	AUTHORIZATION_TYPE 'oauth2'
);
`,
		sqlString(s.cfg.AccessKey), sqlString(s.cfg.SecretKey), sqlString(endpointHost), useSSL,
		sqlString(s.cfg.AccessKey), sqlString(s.cfg.SecretKey), sqlString(endpointHost), useSSL,
		sqlString(s.cfg.AccessKey), sqlString(s.cfg.SecretKey), sqlString(s.cfg.Endpoint), sqlString(Warehouse),
		sqlString(Warehouse), sqlString(s.cfg.Endpoint),
	)
}

func sqlString(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}

func isDuckDBCatalogMiss(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "does not exist because schema") ||
		strings.Contains(msg, "Table with name") && strings.Contains(msg, "does not exist")
}
