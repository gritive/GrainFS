// internal/audit/parquet_internal_test.go
package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func TestAuditIcebergArtifactContract(t *testing.T) {
	metaJSON := fmt.Sprintf(S3InitialMetadata, "test-uuid", "s3://grainfs-audit", int64(1770000000000))
	var meta map[string]any
	require.NoError(t, json.Unmarshal([]byte(metaJSON), &meta))
	require.Equal(t, float64(2), meta["format-version"])
	require.Equal(t, float64(1000), meta["last-partition-id"])
	specs := meta["partition-specs"].([]any)
	require.Len(t, specs, 1)
	spec := specs[0].(map[string]any)
	specFields := spec["fields"].([]any)
	require.Len(t, specFields, 1)
	require.Equal(t, "ts_day", specFields[0].(map[string]any)["name"])
	require.Equal(t, "day", specFields[0].(map[string]any)["transform"])

	manifestList, err := encodeManifestList(101, 1, "s3://grainfs-audit/metadata/s3/101-manifest.avro", 456, 123, "2026-05-14", 0)
	require.NoError(t, err)
	listSchema, listMetadata, _ := parseAvroContainerForTest(t, manifestList)
	require.Equal(t, auditIcebergSchemaJSON, listMetadata["schema"])
	require.Equal(t, auditPartitionSpecJSON, listMetadata["partition-spec"])
	partitions := avroFieldForTest(t, listSchema, "partitions")
	partitionArray := partitions["type"].(map[string]any)
	require.Equal(t, float64(508), partitionArray["element-id"])

	manifest, err := encodeManifest(101, 1, "s3://grainfs-audit/data/2026-05-14/file.parquet", 456, 123, "2026-05-14", 0)
	require.NoError(t, err)
	manifestSchema, manifestMetadata, datum := parseAvroContainerForTest(t, manifest)
	require.Equal(t, auditIcebergSchemaJSON, manifestMetadata["schema"])
	require.Equal(t, auditPartitionSpecJSON, manifestMetadata["partition-spec"])

	dataFile := avroFieldForTest(t, manifestSchema, "data_file")
	dataFileFields := dataFile["type"].(map[string]any)["fields"].([]any)
	require.Len(t, dataFileFields, 16)
	partition := avroFieldFromFieldsForTest(t, dataFileFields, "partition")
	partitionFields := partition["type"].(map[string]any)["fields"].([]any)
	require.Len(t, partitionFields, 1)
	require.Equal(t, "ts_day", partitionFields[0].(map[string]any)["name"])
	for _, name := range []string{"column_sizes", "value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds"} {
		field := avroFieldFromFieldsForTest(t, dataFileFields, name)
		union := field["type"].([]any)
		require.Equal(t, "map", union[1].(map[string]any)["logicalType"], name)
	}
	require.Equal(t, float64(133), avroFieldFromFieldsForTest(t, dataFileFields, "split_offsets")["type"].([]any)[1].(map[string]any)["element-id"])
	require.Equal(t, float64(136), avroFieldFromFieldsForTest(t, dataFileFields, "equality_ids")["type"].([]any)[1].(map[string]any)["element-id"])

	var expected bytes.Buffer
	appendAvroInt(&expected, 1)
	appendAvroInt(&expected, 1)
	appendAvroLong(&expected, 101)
	appendAvroInt(&expected, 1)
	appendAvroLong(&expected, 1)
	appendAvroInt(&expected, 0)
	appendAvroInt(&expected, 0)
	appendAvroString(&expected, "s3://grainfs-audit/data/2026-05-14/file.parquet")
	appendAvroString(&expected, "PARQUET")
	day, err := partitionDay("2026-05-14")
	require.NoError(t, err)
	appendAvroInt(&expected, 1)
	appendAvroInt(&expected, day)
	appendAvroLong(&expected, 123)
	appendAvroLong(&expected, 456)
	for i := 0; i < 10; i++ {
		appendAvroInt(&expected, 0)
	}
	require.Equal(t, expected.Bytes(), datum, "manifest datum must encode all 10 optional data_file tail fields")
}

func TestAuditSchemaV2HasSearchFieldsAndDayPartition(t *testing.T) {
	metaJSON := fmt.Sprintf(S3InitialMetadata, "test-uuid", "s3://grainfs-audit", int64(1770000000000))
	var meta map[string]any
	require.NoError(t, json.Unmarshal([]byte(metaJSON), &meta))

	require.Equal(t, float64(28), meta["last-column-id"])
	specs := meta["partition-specs"].([]any)
	require.Len(t, specs, 1)
	fields := specs[0].(map[string]any)["fields"].([]any)
	require.Len(t, fields, 1)
	require.Equal(t, "day", fields[0].(map[string]any)["transform"])
	require.Equal(t, float64(1), fields[0].(map[string]any)["source-id"])

	rawSchemas := meta["schemas"].([]any)
	schema := rawSchemas[0].(map[string]any)
	rawFields := schema["fields"].([]any)
	names := map[string]bool{}
	for _, raw := range rawFields {
		names[raw.(map[string]any)["name"].(string)] = true
	}
	for _, name := range []string{"event_id", "user_agent", "operation", "auth_status", "err_reason", "version_id", "upload_id", "copy_source_bucket", "copy_source_key"} {
		require.True(t, names[name], "schema must include %s", name)
	}
}

func TestManifestArtifactsUseDefaultPartitionSpecID(t *testing.T) {
	manifestList, err := encodeManifestList(101, 1, "s3://grainfs-audit/metadata/s3/101-manifest.avro", 456, 123, "2026-05-14", 1)
	require.NoError(t, err)
	_, listMetadata, listDatum := parseAvroContainerForTest(t, manifestList)
	require.Equal(t, "1", listMetadata["partition-spec-id"])

	r := bytes.NewReader(listDatum)
	require.Equal(t, "s3://grainfs-audit/metadata/s3/101-manifest.avro", readAvroStringForTest(t, r))
	require.Equal(t, int64(456), readAvroLongForTest(t, r))
	require.Equal(t, int64(1), readAvroLongForTest(t, r), "manifest list partition_spec_id must match table default")

	manifest, err := encodeManifest(101, 1, "s3://grainfs-audit/data/2026-05-14/file.parquet", 456, 123, "2026-05-14", 1)
	require.NoError(t, err)
	_, manifestMetadata, _ := parseAvroContainerForTest(t, manifest)
	require.Equal(t, "1", manifestMetadata["partition-spec-id"])
}

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
	require.Equal(t, 28, int(tbl.NumCols()), "must have 28 columns")

	// Verify column names match schema
	schema := tbl.Schema()
	wantCols := []string{
		"ts", "node_id", "request_id", "sa_id", "source_ip",
		"method", "bucket", "key", "http_status",
		"bytes_in", "bytes_out", "latency_ms", "err_class",
		"event_id", "user_agent", "operation", "subresource", "auth_status",
		"err_reason", "version_id", "upload_id", "copy_source_bucket", "copy_source_key",
		"matched_policy_id", "matched_sid", "authz_latency_us", "condition_context_json",
		"source",
	}
	for i, name := range wantCols {
		require.Equal(t, name, schema.Field(i).Name, "column %d name", i)
	}

	root := pr.MetaData().Schema.Root()
	require.Equal(t, len(wantCols), root.NumFields())
	for i, name := range wantCols {
		field := root.Field(i)
		require.Equal(t, name, field.Name())
		require.Equal(t, int32(i+1), field.FieldID(), "column %s field id", name)
	}
}

func parseAvroContainerForTest(t *testing.T, data []byte) (map[string]any, map[string]string, []byte) {
	t.Helper()
	require.True(t, bytes.HasPrefix(data, []byte{'O', 'b', 'j', 0x01}))
	r := bytes.NewReader(data[4:])
	metadata := make(map[string]string)
	for {
		count := readAvroLongForTest(t, r)
		if count == 0 {
			break
		}
		require.Positive(t, count)
		for i := int64(0); i < count; i++ {
			key := readAvroStringForTest(t, r)
			value := readAvroBytesForTest(t, r)
			metadata[key] = string(value)
		}
	}
	sync := make([]byte, 16)
	_, err := r.Read(sync)
	require.NoError(t, err)
	require.Equal(t, int64(1), readAvroLongForTest(t, r))
	blockSize := readAvroLongForTest(t, r)
	require.Positive(t, blockSize)
	datum := make([]byte, blockSize)
	_, err = r.Read(datum)
	require.NoError(t, err)

	var schema map[string]any
	require.NoError(t, json.Unmarshal([]byte(metadata["avro.schema"]), &schema))
	return schema, metadata, datum
}

func readAvroLongForTest(t *testing.T, r *bytes.Reader) int64 {
	t.Helper()
	var n uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		require.NoError(t, err)
		n |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return int64((n >> 1) ^ uint64(-int64(n&1)))
}

func readAvroBytesForTest(t *testing.T, r *bytes.Reader) []byte {
	t.Helper()
	n := readAvroLongForTest(t, r)
	require.GreaterOrEqual(t, n, int64(0))
	out := make([]byte, n)
	_, err := r.Read(out)
	require.NoError(t, err)
	return out
}

func readAvroStringForTest(t *testing.T, r *bytes.Reader) string {
	t.Helper()
	return string(readAvroBytesForTest(t, r))
}

func avroFieldForTest(t *testing.T, schema map[string]any, name string) map[string]any {
	t.Helper()
	return avroFieldFromFieldsForTest(t, schema["fields"].([]any), name)
}

func avroFieldFromFieldsForTest(t *testing.T, fields []any, name string) map[string]any {
	t.Helper()
	for _, raw := range fields {
		field := raw.(map[string]any)
		if field["name"] == name {
			return field
		}
	}
	t.Fatalf("field %q not found", name)
	return nil
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

func BenchmarkEncodeParquet(b *testing.B) {
	for _, rows := range []int{100, 1000, 10000} {
		events := makeBenchmarkEvents(rows)
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var data []byte
			for i := 0; i < b.N; i++ {
				var err error
				data, err = encodeParquet(events)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(int64(len(data)))
			b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}

func BenchmarkArrowReadParquet(b *testing.B) {
	for _, rows := range []int{1000, 10000} {
		data, err := encodeParquet(makeBenchmarkEvents(rows))
		require.NoError(b, err)
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pr, err := file.NewParquetReader(bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}
				reader, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
				if err != nil {
					_ = pr.Close()
					b.Fatal(err)
				}
				tbl, err := reader.ReadTable(context.Background())
				if err != nil {
					_ = pr.Close()
					b.Fatal(err)
				}
				tbl.Release()
				_ = pr.Close()
			}
			b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}

func BenchmarkDuckDBReadParquetCLI(b *testing.B) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		b.Skip("duckdb not in PATH")
	}

	for _, rows := range []int{1000, 10000} {
		data, err := encodeParquet(makeBenchmarkEvents(rows))
		require.NoError(b, err)
		f, err := os.CreateTemp("", fmt.Sprintf("audit-duckdb-bench-%d-*.parquet", rows))
		require.NoError(b, err)
		_, err = f.Write(data)
		require.NoError(b, err)
		require.NoError(b, f.Close())
		b.Cleanup(func() { _ = os.Remove(f.Name()) })

		path := strings.ReplaceAll(f.Name(), "'", "''")
		query := "SELECT COUNT(*) FROM read_parquet('" + path + "') WHERE method = 'PUT';"
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := exec.Command("duckdb", "-c", query).CombinedOutput()
				if err != nil {
					b.Fatalf("duckdb failed: %v\n%s", err, out)
				}
			}
			b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}

func makeBenchmarkEvents(n int) []S3Event {
	events := make([]S3Event, n)
	for i := range events {
		method := "GET"
		if i%4 == 0 {
			method = "PUT"
		}
		events[i] = S3Event{
			Ts:        1716000000000000 + int64(i),
			NodeID:    fmt.Sprintf("node-%d", i%3),
			RequestID: fmt.Sprintf("req-%d", i),
			SAID:      "sa:admin",
			SourceIP:  "127.0.0.1",
			Method:    method,
			Bucket:    "bench-bucket",
			Key:       fmt.Sprintf("objects/%06d", i),
			Status:    200,
			BytesIn:   int64(512 + i%4096),
			BytesOut:  int64(1024 + i%8192),
			LatencyMs: int32(i % 100),
		}
	}
	return events
}
