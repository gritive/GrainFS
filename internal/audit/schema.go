// internal/audit/schema.go
package audit

const (
	Namespace  = "audit"
	TableS3    = "s3"
	BucketName = "grainfs-audit"
	SystemSA   = "system:audit"
	Warehouse  = "default"
	// AnonSAID is the canonical sentinel written into the sa_id column of
	// audit.s3 rows for anonymous (unauthenticated) requests. Using a
	// non-empty sentinel lets analytics queries distinguish anonymous traffic
	// from genuine empty-attribution write bugs via WHERE sa_id = '(anonymous)'.
	AnonSAID = "(anonymous)"
)

// S3Event is one row of the audit.s3 Iceberg table.
// Ts is Unix microseconds (Iceberg TIMESTAMPTZ → int64 μs since epoch).
type S3Event struct {
	Ts        int64
	EventID   string
	Finalized bool
	NodeID    string
	RequestID string
	SAID      string
	SourceIP  string
	UserAgent string
	Method    string
	Operation string
	Bucket    string
	Key       string
	// Subresource captures the S3 sub-resource query (e.g. "acl", "tagging").
	Subresource string
	Status      int32
	// AuthStatus is the outcome of authz. Valid values:
	//   "allow"      — explicit allow by a policy statement
	//   "deny"       — explicit deny (or default deny)
	//   "anon_allow" — anonymous request allowed by a bucket policy (no SA)
	//   "incomplete" — request did not finalize (server-side reaper)
	AuthStatus       string
	BytesIn          int64
	BytesOut         int64
	LatencyMs        int32
	ErrClass         string
	ErrReason        string
	VersionID        string
	UploadID         string
	CopySourceBucket string
	CopySourceKey    string

	// Policy decision metadata (T48' §6). All fields optional; zero values mean
	// "no policy matched" / "not measured" / "no condition context".

	// MatchedPolicyID is the ID of the policy whose statement authorized (or
	// denied) the request. Empty if the decision was a default deny or no
	// policy was evaluated.
	MatchedPolicyID string
	// MatchedSID is the statement ID (Sid) within MatchedPolicyID. Empty if
	// the matched statement has no Sid or none was matched.
	MatchedSID string
	// AuthzLatencyUS is the policy-evaluation latency in microseconds. Zero
	// if not measured.
	AuthzLatencyUS int32
	// ConditionContext is the IAM condition context observed at evaluation
	// time (e.g. aws:SourceIp, s3:prefix). On the wire this is serialized as
	// a JSON-encoded string into the Iceberg `condition_context_json`
	// column; an empty/nil map writes an empty string. (We use a JSON string
	// rather than Iceberg `map<string,string>` to keep the row-serializer
	// path simple; a future migration can promote it to a real map type.)
	ConditionContext map[string]string
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
	`{"id":13,"name":"err_class","required":false,"type":"string"},` +
	`{"id":14,"name":"event_id","required":false,"type":"string"},` +
	`{"id":15,"name":"user_agent","required":false,"type":"string"},` +
	`{"id":16,"name":"operation","required":false,"type":"string"},` +
	`{"id":17,"name":"subresource","required":false,"type":"string"},` +
	`{"id":18,"name":"auth_status","required":false,"type":"string"},` +
	`{"id":19,"name":"err_reason","required":false,"type":"string"},` +
	`{"id":20,"name":"version_id","required":false,"type":"string"},` +
	`{"id":21,"name":"upload_id","required":false,"type":"string"},` +
	`{"id":22,"name":"copy_source_bucket","required":false,"type":"string"},` +
	`{"id":23,"name":"copy_source_key","required":false,"type":"string"},` +
	`{"id":24,"name":"matched_policy_id","required":false,"type":"string"},` +
	`{"id":25,"name":"matched_sid","required":false,"type":"string"},` +
	`{"id":26,"name":"authz_latency_us","required":false,"type":"int"},` +
	`{"id":27,"name":"condition_context_json","required":false,"type":"string"}` +
	`]}`

const auditPartitionSpecJSON = `[{"name":"ts_day","transform":"day","source-id":1,"field-id":1000}]`

// currentSchemaLastColumnID is the highest column id in auditIcebergSchemaJSON.
// Keep this in sync with the `"last-column-id":N` literal in S3InitialMetadata
// below; TestS3InitialMetadata_LastColumnID_Updated catches drift. The
// migration trigger reads this to decide whether an existing table needs its
// schema rewritten.
const currentSchemaLastColumnID = 27

const S3InitialMetadata = `{"format-version":2,"table-uuid":%q,"location":%q,` +
	`"last-sequence-number":0,"last-updated-ms":%d,"last-column-id":27,` +
	`"current-schema-id":0,"schemas":[` + auditIcebergSchemaJSON + `],` +
	`"partition-specs":[{"spec-id":0,"fields":` + auditPartitionSpecJSON + `}],` +
	`"default-spec-id":0,"last-partition-id":1000,` +
	`"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],` +
	`"properties":{},"current-snapshot-id":-1,"snapshots":[],` +
	`"snapshot-log":[],"metadata-log":[]}`

const S3InitialMetadataV1ForTest = `{"format-version":2,"table-uuid":%q,"location":%q,` +
	`"last-sequence-number":0,"last-updated-ms":%d,"last-column-id":13,` +
	`"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[` +
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
	`]}],` +
	`"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":999,` +
	`"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],` +
	`"properties":{},"current-snapshot-id":-1,"snapshots":[],` +
	`"snapshot-log":[],"metadata-log":[]}`
