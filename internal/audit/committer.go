// internal/audit/committer.go
package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

// auditBackend is the storage subset required by the committer.
type auditBackend interface {
	PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error)
	CreateBucket(ctx context.Context, bucket string) error
}

// CommitterConfig holds the dependencies for a Committer.
type CommitterConfig struct {
	Emitter  *Emitter
	Catalog  icebergcatalog.Catalog
	Backend  auditBackend
	IsLeader func() bool
	// ShipToLeader sends audit events to the current leader via QUIC.
	// Set on all nodes; the committer ignores it when IsLeader() returns true.
	ShipToLeader func(ctx context.Context, events []S3Event) error
	NodeID       string
	Interval     time.Duration
}

// Committer periodically drains the ring buffer and commits to the Iceberg table.
// On leader nodes it writes Parquet + Iceberg metadata. On follower nodes it ships
// events to the leader via ShipToLeader.
type Committer struct {
	cfg         CommitterConfig
	batch       []S3Event
	leaderExtra []S3Event
}

// NewCommitter creates a Committer.
func NewCommitter(cfg CommitterConfig) *Committer {
	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	return &Committer{
		cfg:         cfg,
		batch:       make([]S3Event, 0, ringCap),
		leaderExtra: make([]S3Event, 0, ringCap),
	}
}

// AppendFromFollower adds events shipped by a follower to the leader queue.
// Called from the QUIC stream handler.
func (c *Committer) AppendFromFollower(events []S3Event) {
	c.leaderExtra = append(c.leaderExtra, events...)
}

// Run runs the committer loop until ctx is cancelled.
func (c *Committer) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.cfg.IsLeader() {
				c.batch = c.cfg.Emitter.Ring().DrainInto(c.batch)
				c.batch = append(c.batch, c.leaderExtra...)
				c.leaderExtra = c.leaderExtra[:0]
				if len(c.batch) == 0 {
					continue
				}
				if err := c.commit(ctx, c.batch); err != nil {
					log.Error().Err(err).Msg("audit committer: commit failed; events retained in zerolog")
				}
				c.batch = c.batch[:0]
			} else if c.cfg.ShipToLeader != nil {
				c.batch = c.cfg.Emitter.Ring().DrainInto(c.batch)
				if len(c.batch) == 0 {
					continue
				}
				if err := c.cfg.ShipToLeader(ctx, c.batch); err != nil {
					log.Warn().Err(err).Int("events", len(c.batch)).Msg("audit committer: ship to leader failed; events in zerolog")
				}
				c.batch = c.batch[:0]
			}
		}
	}
}

func (c *Committer) commit(ctx context.Context, events []S3Event) error {
	ident := icebergcatalog.Identifier{Namespace: []string{Namespace}, Name: TableS3}

	tbl, err := c.cfg.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("load audit.s3 table: %w", err)
	}

	parquetBytes, err := encodeParquet(events)
	if err != nil {
		return fmt.Errorf("encode parquet: %w", err)
	}

	dt := time.Now().UTC().Format("2006-01-02")
	snapshotID := time.Now().UnixNano()
	fileUUID := uuid.New().String()
	parquetKey := fmt.Sprintf("data/dt=%s/%s.parquet", dt, fileUUID)

	obj, err := c.cfg.Backend.PutObject(ctx, BucketName, parquetKey,
		bytes.NewReader(parquetBytes), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("put parquet: %w", err)
	}

	newMetaJSON, newMetaPath, err := c.buildNewMetadata(ctx, tbl, fmt.Sprintf("s3://%s/%s", BucketName, parquetKey), obj.Size, int64(len(events)), snapshotID, dt)
	if err != nil {
		return fmt.Errorf("build iceberg metadata: %w", err)
	}

	_, err = c.cfg.Catalog.CommitTable(ctx, ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: tbl.MetadataLocation,
		NewMetadataLocation:      newMetaPath,
		Metadata:                 newMetaJSON,
	})
	if err != nil {
		return fmt.Errorf("commit table: %w", err)
	}

	log.Info().
		Int("events", len(events)).
		Str("parquet_key", parquetKey).
		Int64("snapshot_id", snapshotID).
		Msg("audit committer: committed snapshot")
	return nil
}

func (c *Committer) buildNewMetadata(
	ctx context.Context,
	tbl *icebergcatalog.Table,
	parquetPath string, parquetSize, rowCount, snapshotID int64,
	dt string,
) (json.RawMessage, string, error) {
	var meta map[string]any
	if err := json.Unmarshal(tbl.Metadata, &meta); err != nil {
		return nil, "", fmt.Errorf("parse current metadata: %w", err)
	}

	nowMs := time.Now().UnixMilli()

	manifestListPath, _, err := c.writeManifestList(ctx, snapshotID, parquetPath, parquetSize, rowCount, dt)
	if err != nil {
		return nil, "", fmt.Errorf("write manifest list: %w", err)
	}

	seqNum := getInt64(meta, "last-sequence-number") + 1
	snapshot := map[string]any{
		"snapshot-id":     snapshotID,
		"timestamp-ms":    nowMs,
		"sequence-number": seqNum,
		"summary": map[string]string{
			"operation":        "append",
			"added-data-files": "1",
			"added-records":    fmt.Sprintf("%d", rowCount),
			"added-files-size": fmt.Sprintf("%d", parquetSize),
		},
		"manifest-list": manifestListPath,
		"schema-id":     0,
	}

	snapshots, _ := meta["snapshots"].([]any)
	snapshots = append(snapshots, snapshot)
	snapshotLog, _ := meta["snapshot-log"].([]any)
	snapshotLog = append(snapshotLog, map[string]any{"timestamp-ms": nowMs, "snapshot-id": snapshotID})
	metaLog, _ := meta["metadata-log"].([]any)
	metaLog = append(metaLog, map[string]any{"timestamp-ms": nowMs, "metadata-file": tbl.MetadataLocation})

	meta["last-sequence-number"] = seqNum
	meta["last-updated-ms"] = nowMs
	meta["current-snapshot-id"] = snapshotID
	meta["snapshots"] = snapshots
	meta["snapshot-log"] = snapshotLog
	meta["metadata-log"] = metaLog
	meta["refs"] = map[string]any{
		"main": map[string]any{"snapshot-id": snapshotID, "type": "branch"},
	}

	newMetaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, "", err
	}

	newMetaKey := fmt.Sprintf("metadata/s3/%05d-%s.metadata.json", seqNum, uuid.New().String())
	if _, err := c.cfg.Backend.PutObject(ctx, BucketName, newMetaKey,
		bytes.NewReader(newMetaBytes), "application/json"); err != nil {
		return nil, "", fmt.Errorf("put new metadata.json: %w", err)
	}

	return json.RawMessage(newMetaBytes), fmt.Sprintf("s3://%s/%s", BucketName, newMetaKey), nil
}

// writeManifestList writes the Iceberg manifest and manifest-list Avro files.
// Uses minimal Avro Object Container File format directly (avro container format spec §5).
func (c *Committer) writeManifestList(
	ctx context.Context,
	snapshotID int64,
	parquetPath string, parquetSize, rowCount int64,
	dt string,
) (manifestListPath string, manifestListSize int64, _ error) {
	manifestKey := fmt.Sprintf("metadata/s3/%d-%s-manifest.avro", snapshotID, uuid.New().String())
	manifestPath := fmt.Sprintf("s3://%s/%s", BucketName, manifestKey)

	manifestBytes, err := encodeManifest(snapshotID, parquetPath, parquetSize, rowCount, dt)
	if err != nil {
		return "", 0, fmt.Errorf("encode manifest: %w", err)
	}
	if _, err := c.cfg.Backend.PutObject(ctx, BucketName, manifestKey,
		bytes.NewReader(manifestBytes), "application/octet-stream"); err != nil {
		return "", 0, fmt.Errorf("put manifest: %w", err)
	}

	listKey := fmt.Sprintf("metadata/s3/snap-%d-%s.avro", snapshotID, uuid.New().String())
	listPath := fmt.Sprintf("s3://%s/%s", BucketName, listKey)

	listBytes, err := encodeManifestList(snapshotID, manifestPath, int64(len(manifestBytes)), rowCount)
	if err != nil {
		return "", 0, fmt.Errorf("encode manifest list: %w", err)
	}
	if _, err := c.cfg.Backend.PutObject(ctx, BucketName, listKey,
		bytes.NewReader(listBytes), "application/octet-stream"); err != nil {
		return "", 0, fmt.Errorf("put manifest list: %w", err)
	}

	return listPath, int64(len(listBytes)), nil
}

func getInt64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	}
	return 0
}

// encodeParquet encodes S3Events to Parquet with Iceberg field IDs.
func encodeParquet(events []S3Event) ([]byte, error) {
	pool := memory.NewGoAllocator()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})},
		{Name: "node_id", Type: arrow.BinaryTypes.LargeString, Nullable: false,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"})},
		{Name: "request_id", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"3"})},
		{Name: "sa_id", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"4"})},
		{Name: "source_ip", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"5"})},
		{Name: "method", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"6"})},
		{Name: "bucket", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"7"})},
		{Name: "key", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"8"})},
		{Name: "http_status", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"9"})},
		{Name: "bytes_in", Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"10"})},
		{Name: "bytes_out", Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"11"})},
		{Name: "latency_ms", Type: arrow.PrimitiveTypes.Int32,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"12"})},
		{Name: "err_class", Type: arrow.BinaryTypes.LargeString,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"13"})},
	}, nil)

	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	tsB := builder.Field(0).(*array.TimestampBuilder)
	nodeB := builder.Field(1).(*array.LargeStringBuilder)
	reqB := builder.Field(2).(*array.LargeStringBuilder)
	saB := builder.Field(3).(*array.LargeStringBuilder)
	ipB := builder.Field(4).(*array.LargeStringBuilder)
	methB := builder.Field(5).(*array.LargeStringBuilder)
	buckB := builder.Field(6).(*array.LargeStringBuilder)
	keyB := builder.Field(7).(*array.LargeStringBuilder)
	statB := builder.Field(8).(*array.Int32Builder)
	binB := builder.Field(9).(*array.Int64Builder)
	boutB := builder.Field(10).(*array.Int64Builder)
	latB := builder.Field(11).(*array.Int32Builder)
	errB := builder.Field(12).(*array.LargeStringBuilder)

	for _, e := range events {
		tsB.Append(arrow.Timestamp(e.Ts))
		nodeB.Append(e.NodeID)
		reqB.Append(e.RequestID)
		saB.Append(e.SAID)
		ipB.Append(e.SourceIP)
		methB.Append(e.Method)
		buckB.Append(e.Bucket)
		keyB.Append(e.Key)
		statB.Append(e.Status)
		binB.Append(e.BytesIn)
		boutB.Append(e.BytesOut)
		latB.Append(e.LatencyMs)
		errB.Append(e.ErrClass)
	}

	rec := builder.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	if err := w.Write(rec); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeManifestList creates an Iceberg manifest list Avro file with one manifest entry.
func encodeManifestList(snapshotID int64, manifestPath string, manifestLen, rowCount int64) ([]byte, error) {
	schema := `{"type":"record","name":"manifest_file","fields":[` +
		`{"name":"manifest_path","type":"string","field-id":500},` +
		`{"name":"manifest_length","type":"long","field-id":501},` +
		`{"name":"partition_spec_id","type":"int","field-id":502},` +
		`{"name":"content","type":"int","field-id":517},` +
		`{"name":"sequence_number","type":"long","field-id":515},` +
		`{"name":"min_sequence_number","type":"long","field-id":516},` +
		`{"name":"added_snapshot_id","type":"long","field-id":503},` +
		`{"name":"added_files_count","type":"int","field-id":504},` +
		`{"name":"existing_files_count","type":"int","field-id":505},` +
		`{"name":"deleted_files_count","type":"int","field-id":506},` +
		`{"name":"added_rows_count","type":"long","field-id":512},` +
		`{"name":"existing_rows_count","type":"long","field-id":513},` +
		`{"name":"deleted_rows_count","type":"long","field-id":514},` +
		`{"name":"partitions","type":{"type":"array","items":{"type":"record","name":"r508","fields":[` +
		`{"name":"contains_null","type":"boolean","field-id":509},` +
		`{"name":"contains_nan","type":["null","boolean"],"default":null,"field-id":518},` +
		`{"name":"lower_bound","type":["null","bytes"],"default":null,"field-id":510},` +
		`{"name":"upper_bound","type":["null","bytes"],"default":null,"field-id":511}` +
		`]},"default":[]},"field-id":507}` +
		`]}`

	var rec bytes.Buffer
	appendAvroString(&rec, manifestPath)
	appendAvroLong(&rec, manifestLen)
	appendAvroInt(&rec, 0)           // partition_spec_id
	appendAvroInt(&rec, 0)           // content=DATA
	appendAvroLong(&rec, 1)          // sequence_number
	appendAvroLong(&rec, 1)          // min_sequence_number
	appendAvroLong(&rec, snapshotID) // added_snapshot_id
	appendAvroInt(&rec, 1)           // added_files_count
	appendAvroInt(&rec, 0)           // existing_files_count
	appendAvroInt(&rec, 0)           // deleted_files_count
	appendAvroLong(&rec, rowCount)   // added_rows_count
	appendAvroLong(&rec, 0)          // existing_rows_count
	appendAvroLong(&rec, 0)          // deleted_rows_count
	appendAvroLong(&rec, 0)          // partitions: empty array (block count = 0)

	return buildAvroContainer(schema, rec.Bytes()), nil
}

// encodeManifest creates an Iceberg manifest Avro file with one data file entry.
func encodeManifest(snapshotID int64, parquetPath string, parquetSize, rowCount int64, dt string) ([]byte, error) {
	dtTime, _ := time.Parse("2006-01-02", dt)
	julianDay := int32(dtTime.Unix() / 86400)

	schema := `{"type":"record","name":"manifest_entry","fields":[` +
		`{"name":"status","type":"int","field-id":0},` +
		`{"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},` +
		`{"name":"sequence_number","type":["null","long"],"default":null,"field-id":3},` +
		`{"name":"file_sequence_number","type":["null","long"],"default":null,"field-id":4},` +
		`{"name":"data_file","type":{"type":"record","name":"r2","fields":[` +
		`{"name":"content","type":"int","field-id":134},` +
		`{"name":"file_path","type":"string","field-id":100},` +
		`{"name":"file_format","type":"string","field-id":101},` +
		`{"name":"partition","type":{"type":"record","name":"r102","fields":[` +
		`{"name":"dt","type":["null","int"],"default":null,"field-id":1000}` +
		`]},"field-id":102},` +
		`{"name":"record_count","type":"long","field-id":103},` +
		`{"name":"file_size_in_bytes","type":"long","field-id":104},` +
		`{"name":"column_sizes","type":["null",{"type":"array","items":{"type":"record","name":"k81v81","fields":[{"name":"key","type":"int","field-id":117},{"name":"value","type":"long","field-id":118}]}}],"default":null,"field-id":108},` +
		`{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k81v81_2","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]}}],"default":null,"field-id":109},` +
		`{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k81v81_3","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]}}],"default":null,"field-id":110},` +
		`{"name":"nan_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k81v81_4","fields":[{"name":"key","type":"int","field-id":138},{"name":"value","type":"long","field-id":139}]}}],"default":null,"field-id":137},` +
		`{"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k81v81_5","fields":[{"name":"key","type":"int","field-id":126},{"name":"value","type":"bytes","field-id":127}]}}],"default":null,"field-id":125},` +
		`{"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k81v81_6","fields":[{"name":"key","type":"int","field-id":129},{"name":"value","type":"bytes","field-id":130}]}}],"default":null,"field-id":128},` +
		`{"name":"key_metadata","type":["null","bytes"],"default":null,"field-id":131},` +
		`{"name":"split_offsets","type":["null",{"type":"array","items":"long"}],"default":null,"field-id":132},` +
		`{"name":"equality_ids","type":["null",{"type":"array","items":"int"}],"default":null,"field-id":135},` +
		`{"name":"sort_order_id","type":["null","int"],"default":null,"field-id":140}` +
		`]},"field-id":2}` +
		`]}`

	var rec bytes.Buffer
	appendAvroInt(&rec, 1) // status=ADDED
	// snapshot_id: union[null(0), long(1)] → index 1 + value
	appendAvroInt(&rec, 1)
	appendAvroLong(&rec, snapshotID)
	// sequence_number: union → 1 + value
	appendAvroInt(&rec, 1)
	appendAvroLong(&rec, 1)
	// file_sequence_number: null (index 0)
	appendAvroInt(&rec, 0)
	// data_file record
	appendAvroInt(&rec, 0) // content=DATA
	appendAvroString(&rec, parquetPath)
	appendAvroString(&rec, "PARQUET")
	// partition: dt field = union[null(0), int(1)] → 1 + julianDay
	appendAvroInt(&rec, 1)
	appendAvroInt(&rec, julianDay)
	appendAvroLong(&rec, rowCount)
	appendAvroLong(&rec, parquetSize)
	// optional map fields: all null (union index 0), 8 fields
	for i := 0; i < 8; i++ {
		appendAvroInt(&rec, 0)
	}

	return buildAvroContainer(schema, rec.Bytes()), nil
}

// buildAvroContainer creates a single-record Avro Object Container File.
func buildAvroContainer(schema string, datum []byte) []byte {
	sync := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	var hdr bytes.Buffer
	hdr.Write([]byte{'O', 'b', 'j', 0x01})

	appendAvroLong(&hdr, 2) // 2 map entries
	appendAvroString(&hdr, "avro.schema")
	appendAvroBytes(&hdr, []byte(schema))
	appendAvroString(&hdr, "avro.codec")
	appendAvroBytes(&hdr, []byte("null"))
	appendAvroLong(&hdr, 0) // end of map
	hdr.Write(sync[:])

	var block bytes.Buffer
	appendAvroLong(&block, 1)                 // object count
	appendAvroLong(&block, int64(len(datum))) // byte count
	block.Write(datum)
	block.Write(sync[:])

	var out bytes.Buffer
	out.Write(hdr.Bytes())
	out.Write(block.Bytes())
	return out.Bytes()
}

func appendAvroLong(buf *bytes.Buffer, v int64) {
	n := uint64((v << 1) ^ (v >> 63))
	for n > 0x7F {
		buf.WriteByte(byte(n&0x7F) | 0x80)
		n >>= 7
	}
	buf.WriteByte(byte(n))
}

func appendAvroInt(buf *bytes.Buffer, v int32) { appendAvroLong(buf, int64(v)) }

func appendAvroString(buf *bytes.Buffer, s string) {
	appendAvroLong(buf, int64(len(s)))
	buf.WriteString(s)
}

func appendAvroBytes(buf *bytes.Buffer, b []byte) {
	appendAvroLong(buf, int64(len(b)))
	buf.Write(b)
}
