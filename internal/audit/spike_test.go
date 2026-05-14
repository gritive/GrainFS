//go:build spike

package audit_spike_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
	badger "github.com/dgraph-io/badger/v4"
	avro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestSpike_LocalFile_DuckDBReadsBack — single-node round-trip:
//
//	storage.NewLocalBackend (CreateBucket → PutObject parquet/manifest/manifest-list/metadata.json)
//	icebergcatalog.NewStore   (CreateNamespace → CreateTable → CommitTable)
//	duckdb iceberg_scan(<local metadata.json>) == 1 row
//
// 모든 Iceberg URI는 local FS 절대 path. s3:// URI 절대 안 씀 — DuckDB httpfs 의존성 회피.
func TestSpike_LocalFile_DuckDBReadsBack(t *testing.T) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb binary not in PATH")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	// 1. on-disk backend — NewLocalBackend returns (*LocalBackend, error)
	warehouseRoot := filepath.Join(tmpDir, "warehouse")
	require.NoError(t, os.MkdirAll(warehouseRoot, 0o755))
	backend, err := storage.NewLocalBackend(warehouseRoot)
	require.NoError(t, err)

	const bucket = "grainfs-audit"
	const namespace = "audit"
	const tableName = "audit_s3"

	// CreateBucket: (ctx, bucket) — no CreateBucketRequest arg
	require.NoError(t, backend.CreateBucket(ctx, bucket))

	// 2. single-node catalog (Badger)
	db, err := badger.Open(badger.DefaultOptions(filepath.Join(tmpDir, "badger")).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()
	store := icebergcatalog.NewStore(db, "file://"+warehouseRoot)

	require.NoError(t, store.CreateNamespace(ctx, []string{namespace}, nil))

	// 3. table layout dirs — LocalBackend stores at root/data/<bucket>/<key>
	dataDir := filepath.Join(warehouseRoot, "data")
	tableBase := filepath.Join(dataDir, bucket, namespace, tableName)
	require.NoError(t, os.MkdirAll(filepath.Join(tableBase, "metadata"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tableBase, "data", "dt=2026-05-14"), 0o755))

	schema := buildIcebergSchema(t)
	metaV1Bytes, metaV1Loc := buildInitialMetadataJSON(t, tableBase, schema)
	require.NoError(t, backendPut(t, ctx, backend, bucket,
		namespace+"/"+tableName+"/metadata/v1.metadata.json", metaV1Bytes))

	_, err = store.CreateTable(ctx,
		icebergcatalog.Identifier{Namespace: []string{namespace}, Name: tableName},
		icebergcatalog.CreateTableInput{
			MetadataLocation: metaV1Loc,
			Metadata:         metaV1Bytes,
		})
	require.NoError(t, err)

	// 4. 1-row Parquet — content paths use dataDir (where LocalBackend actually stores files)
	parquetRel := namespace + "/" + tableName + "/data/dt=2026-05-14/spike-1.parquet"
	parquetAbs := filepath.Join(dataDir, bucket, parquetRel)
	parquetBytes := buildOneRowParquet(t, schema)
	require.NoError(t, backendPut(t, ctx, backend, bucket, parquetRel, parquetBytes))

	// 5. manifest + manifest-list
	manifestRel := namespace + "/" + tableName + "/metadata/snap-1-manifest.avro"
	manifestAbs := filepath.Join(dataDir, bucket, manifestRel)
	manifestBytes := buildManifest(t, schema, parquetAbs, len(parquetBytes), 1)
	require.NoError(t, backendPut(t, ctx, backend, bucket, manifestRel, manifestBytes))

	manifestListRel := namespace + "/" + tableName + "/metadata/snap-1-manifest-list.avro"
	manifestListAbs := filepath.Join(dataDir, bucket, manifestListRel)
	manifestListBytes := buildManifestList(t, manifestAbs, 1)
	require.NoError(t, backendPut(t, ctx, backend, bucket, manifestListRel, manifestListBytes))

	// 6. metadata.json v2 (snapshot 추가)
	metaV2Bytes, metaV2Loc := buildAppendedMetadataJSON(t, tableBase, schema, manifestListAbs, 1)
	require.NoError(t, backendPut(t, ctx, backend, bucket,
		namespace+"/"+tableName+"/metadata/v2.metadata.json", metaV2Bytes))

	_, err = store.CommitTable(ctx,
		icebergcatalog.Identifier{Namespace: []string{namespace}, Name: tableName},
		icebergcatalog.CommitTableInput{
			ExpectedMetadataLocation: metaV1Loc,
			NewMetadataLocation:      metaV2Loc,
			Metadata:                 metaV2Bytes,
		})
	require.NoError(t, err)

	// 7. DuckDB: 로컬 file path로 iceberg_scan
	metaV2AbsForDuckDB := filepath.Join(tableBase, "metadata/v2.metadata.json")
	out, err := exec.Command("duckdb", "-noheader", "-csv", "-c",
		"INSTALL iceberg; LOAD iceberg; "+
			"SELECT count(*) FROM iceberg_scan('"+metaV2AbsForDuckDB+"');").
		CombinedOutput()
	t.Logf("duckdb out: %s", out)
	require.NoError(t, err, "DuckDB iceberg_scan failed: %s", out)
	require.Equal(t, "1", strings.TrimSpace(string(out)))
}

func backendPut(t *testing.T, ctx context.Context, b storage.Backend, bucket, key string, body []byte) error {
	t.Helper()
	_, err := b.PutObject(ctx, bucket, key, bytes.NewReader(body), "application/octet-stream")
	return err
}

// 헬퍼들 — verbatim capture 기반 구현.
func buildIcebergSchema(t *testing.T) *iceberg.Schema {
	t.Helper()
	// 2열: ts(timestamptz, id=1) + sa_id(string, id=2)
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
		iceberg.NestedField{ID: 2, Name: "sa_id", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}
func buildInitialMetadataJSON(t *testing.T, tableBase string, schema *iceberg.Schema) ([]byte, string) {
	t.Helper()
	loc := filepath.Join(tableBase, "metadata/v1.metadata.json")
	schemaJSON, err := schema.MarshalJSON()
	require.NoError(t, err)
	meta := map[string]any{
		"format-version":       2,
		"table-uuid":           "00000000-0000-0000-0000-000000000001",
		"location":             tableBase,
		"last-sequence-number": 0,
		"last-updated-ms":      0,
		"last-column-id":       2,
		"current-schema-id":    0,
		"schemas":              []json.RawMessage{schemaJSON},
		"default-spec-id":      0,
		"partition-specs": []map[string]any{
			{"spec-id": 0, "fields": []any{}},
		},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []map[string]any{{"order-id": 0, "fields": []any{}}},
		"properties":            map[string]string{},
		"current-snapshot-id":   -1,
		"snapshots":             []any{},
		"snapshot-log":          []any{},
		"metadata-log":          []any{},
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	return data, loc
}
func buildOneRowParquet(t *testing.T, _ *iceberg.Schema) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType, Nullable: true},
		{Name: "sa_id", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	var buf bytes.Buffer
	pw, err := pqarrow.NewFileWriter(arrowSchema, &buf, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()

	bldr.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1747238400000000)) // 2026-05-14T16:00:00Z in microseconds
	bldr.Field(1).(*array.StringBuilder).Append("audit-sa-0001")

	rec := bldr.NewRecord()
	defer rec.Release()

	pw.NewRowGroup()
	require.NoError(t, pw.Write(rec))
	require.NoError(t, pw.Close())
	return buf.Bytes()
}
func buildManifest(t *testing.T, schema *iceberg.Schema, parquetAbs string, parquetSize, rows int) []byte {
	t.Helper()
	spec := iceberg.NewPartitionSpec() // unpartitioned
	snapshotID := int64(1)

	df, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		parquetAbs,
		iceberg.ParquetFile,
		nil, // no partition data
		nil, // no logical types
		nil, // no fixed sizes
		int64(rows),
		int64(parquetSize),
	)
	require.NoError(t, err)

	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		&snapshotID,
		nil, // seqNum inherited
		nil, // fileSeqNum inherited
		df.Build(),
	)

	var buf bytes.Buffer
	// WriteManifest: filename is embedded in Avro metadata (not written to disk by this call)
	_, err = iceberg.WriteManifest(parquetAbs+".manifest", &buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	return buf.Bytes()
}
func buildManifestList(t *testing.T, manifestAbs string, addedFiles int) []byte {
	t.Helper()
	snapshotID := int64(1)
	seqNumber := int64(1)

	mf := iceberg.NewManifestFile(2, manifestAbs, 0, 0, snapshotID).
		AddedFiles(int32(addedFiles)).
		AddedRows(int64(addedFiles)).
		SequenceNum(seqNumber, seqNumber).
		Build()

	var buf bytes.Buffer
	w, err := iceberg.NewManifestListWriterV2(&buf, snapshotID, seqNumber, nil)
	require.NoError(t, err)
	require.NoError(t, w.AddManifests([]iceberg.ManifestFile{mf}))
	require.NoError(t, w.Close())
	return buf.Bytes()
}
func buildAppendedMetadataJSON(t *testing.T, tableBase string, schema *iceberg.Schema, manifestListAbs string, addedRecords int) ([]byte, string) {
	t.Helper()
	loc := filepath.Join(tableBase, "metadata/v2.metadata.json")
	schemaJSON, err := schema.MarshalJSON()
	require.NoError(t, err)

	nowMs := int64(1747238400000) // 2026-05-14T16:00:00Z in milliseconds
	snapshot := map[string]any{
		"snapshot-id":     1,
		"sequence-number": 1,
		"timestamp-ms":    nowMs,
		"manifest-list":   manifestListAbs,
		"summary": map[string]string{
			"operation":        "append",
			"added-data-files": "1",
			"added-records":    "1",
		},
		"schema-id": 0,
	}
	meta := map[string]any{
		"format-version":       2,
		"table-uuid":           "00000000-0000-0000-0000-000000000001",
		"location":             tableBase,
		"last-sequence-number": 1,
		"last-updated-ms":      nowMs,
		"last-column-id":       2,
		"current-schema-id":    0,
		"schemas":              []json.RawMessage{schemaJSON},
		"default-spec-id":      0,
		"partition-specs": []map[string]any{
			{"spec-id": 0, "fields": []any{}},
		},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []map[string]any{{"order-id": 0, "fields": []any{}}},
		"properties":            map[string]string{},
		"current-snapshot-id":   1,
		"snapshots":             []any{snapshot},
		"snapshot-log":          []map[string]any{{"timestamp-ms": nowMs, "snapshot-id": 1}},
		"metadata-log":          []any{},
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	return data, loc
}

// avro.LogicalType 참조 (NewDataFileBuilder 인자 타입 — Step 4.4에서 직접 사용)
var _ avro.LogicalType
