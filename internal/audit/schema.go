// internal/audit/schema.go
package audit

const (
	Namespace  = "audit"
	TableS3    = "s3"
	BucketName = "grainfs-audit"
	SystemSA   = "system:audit"
)

// S3Event is one row of the audit.s3 Iceberg table.
// Ts is Unix microseconds (Iceberg TIMESTAMPTZ → int64 μs since epoch).
type S3Event struct {
	Ts               int64
	EventID          string
	NodeID           string
	RequestID        string
	SAID             string
	SourceIP         string
	UserAgent        string
	Method           string
	Operation        string
	Bucket           string
	Key              string
	Subresource      string
	Status           int32
	AuthStatus       string
	BytesIn          int64
	BytesOut         int64
	LatencyMs        int32
	ErrClass         string
	ErrReason        string
	VersionID         string
	UploadID         string
	CopySourceBucket string
	CopySourceKey    string
}

// S3InitialMetadata is the Iceberg v2 metadata.json for an empty audit.s3 table.
// Use fmt.Sprintf(S3InitialMetadata, tableUUID, s3Location, nowMs) to create.
const auditIcebergSchemaJSON = `{"type":"struct","schema-id":0,"fields":[` +
	`{"id":1,"name":"ts","required":true,"type":"timestamptz"},` +
	`{"id":2,"name":"node_id","required":true,"type":"string"},` +
	`{"id":3,"name":"request_id","required":false,"type":"string"},` +
	`{"id":4,"name":"sa_id","required":false,"type":"string"},` +
	`{"id":5,"name":"source_ip","required":false,"type":"string"},` +
	`{"id":6,"name":"method","required":false,"type":"string"},` +
	`{"id":7,"name":"bucket","required":false,"type":"string"},` +
	`{"id":8,"name":"key","required":false,"type":"string"},` +
	`{"id":9,"name":"http_status","required":true,"type":"int"},` +
	`{"id":10,"name":"bytes_in","required":false,"type":"long"},` +
	`{"id":11,"name":"bytes_out","required":false,"type":"long"},` +
	`{"id":12,"name":"latency_ms","required":false,"type":"int"},` +
	`{"id":13,"name":"err_class","required":false,"type":"string"}` +
	`]}`

const S3InitialMetadata = `{"format-version":2,"table-uuid":%q,"location":%q,` +
	`"last-sequence-number":0,"last-updated-ms":%d,"last-column-id":13,` +
	`"current-schema-id":0,"schemas":[` + auditIcebergSchemaJSON + `],` +
	`"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":999,` +
	`"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],` +
	`"properties":{},"current-snapshot-id":-1,"snapshots":[],` +
	`"snapshot-log":[],"metadata-log":[]}`
