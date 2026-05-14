// internal/audit/parquet_internal_test.go
package audit

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func TestEncodeParquetReadback(t *testing.T) {
	events := []S3Event{
		{Ts: 1716000000000000, NodeID: "n1", RequestID: "r1", SAID: "sa:admin",
			SourceIP: "127.0.0.1", Method: "PUT", Bucket: "bkt", Key: "obj/key",
			Status: 200, BytesIn: 1024, BytesOut: 0, LatencyMs: 5, ErrClass: ""},
		{Ts: 1716000000000001, NodeID: "n2", Method: "GET", Bucket: "bkt", Key: "obj/key",
			Status: 200, BytesOut: 1024, LatencyMs: 2},
	}

	data, err := encodeParquet(events)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	pr, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer pr.Close()

	reader, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	require.NoError(t, err)

	tbl, err := reader.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	require.Equal(t, int64(len(events)), tbl.NumRows(), "row count must match")
	require.Equal(t, 13, int(tbl.NumCols()), "must have 13 columns")

	// Verify column names match schema
	schema := tbl.Schema()
	wantCols := []string{
		"ts", "node_id", "request_id", "sa_id", "source_ip",
		"method", "bucket", "key", "http_status",
		"bytes_in", "bytes_out", "latency_ms", "err_class",
	}
	for i, name := range wantCols {
		require.Equal(t, name, schema.Field(i).Name, "column %d name", i)
	}
}

// TestEncodeParquetDuckDB verifies the Parquet output is readable by DuckDB.
// Skipped when duckdb binary is not in PATH.
func TestEncodeParquetDuckDB(t *testing.T) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb not in PATH")
	}

	events := []S3Event{
		{Ts: 1716000000000000, NodeID: "n1", Method: "PUT", Bucket: "bkt", Key: "obj", Status: 200, BytesIn: 512},
		{Ts: 1716000000000001, NodeID: "n2", Method: "GET", Bucket: "bkt", Key: "obj", Status: 404, ErrClass: "NoSuchKey"},
	}

	data, err := encodeParquet(events)
	require.NoError(t, err)

	f, err := os.CreateTemp("", "audit-spike-*.parquet")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	require.NoError(t, err)
	f.Close()

	out, err := exec.Command("duckdb", "-c",
		"SELECT node_id, method, http_status FROM read_parquet('"+f.Name()+"') ORDER BY node_id;",
	).CombinedOutput()
	require.NoError(t, err, "duckdb failed: %s", out)

	outStr := string(out)
	require.True(t, strings.Contains(outStr, "n1"), "n1 row expected in output:\n%s", outStr)
	require.True(t, strings.Contains(outStr, "n2"), "n2 row expected in output:\n%s", outStr)
	require.True(t, strings.Contains(outStr, "PUT"), "PUT method expected:\n%s", outStr)
	require.True(t, strings.Contains(outStr, "GET"), "GET method expected:\n%s", outStr)
}
