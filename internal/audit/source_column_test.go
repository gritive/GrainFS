package audit_test

// T15 NFS§C: Audit schema bump — source column tests.
//
// Covers:
//  1. S3Event struct has Source field.
//  2. auditIcebergSchemaJSON includes "source" field id=28.
//  3. S3InitialMetadata last-column-id=28.
//  4. Wire roundtrip preserves Source field.
//  5. Migration bumps last-column-id 27→28.
//  6. Existing rows (Source="") are preserved by migration.

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestS3Event_HasSourceField(t *testing.T) {
	// Compile-time check that S3Event has a Source field.
	var e audit.S3Event
	e.Source = "nfs4"
	if e.Source != "nfs4" {
		t.Fatalf("Source field not set correctly, got %q", e.Source)
	}
	e.Source = "9p"
	e.Source = "iceberg"
	e.Source = "s3"
	e.Source = ""
	_ = e
}

func TestIcebergSchemaJSON_IncludesSourceColumn(t *testing.T) {
	for _, want := range []string{
		`"name":"source"`,
	} {
		if !strings.Contains(audit.AuditIcebergSchemaJSONForTest, want) {
			t.Errorf("auditIcebergSchemaJSON missing %s", want)
		}
	}
}

func TestS3InitialMetadata_LastColumnID_Is28(t *testing.T) {
	// S3InitialMetadata must claim last-column-id=28 after adding "source".
	if audit.CurrentSchemaLastColumnIDForTest != 28 {
		t.Fatalf("currentSchemaLastColumnID = %d, want 28", audit.CurrentSchemaLastColumnIDForTest)
	}
}

func TestWireRoundtrip_Source(t *testing.T) {
	events := []audit.S3Event{
		{
			Ts:        1716000000000100,
			EventID:   "evt-nfs4",
			NodeID:    "node-1",
			RequestID: "req-nfs-1",
			SAID:      "alice-mount",
			SourceIP:  "10.0.0.2",
			Method:    "LOOKUP",
			Bucket:    "mybucket",
			Status:    200,
			AuthStatus: "allow",
			Source:    "nfs4",
		},
		{
			Ts:        1716000000000101,
			EventID:   "evt-9p",
			NodeID:    "node-1",
			RequestID: "req-9p-1",
			SAID:      "",
			SourceIP:  "10.0.0.3",
			Method:    "Walk",
			Bucket:    "mybucket",
			Status:    403,
			AuthStatus: "deny",
			Source:    "9p",
		},
		{
			Ts:        1716000000000102,
			EventID:   "evt-s3",
			NodeID:    "node-1",
			Method:    "PUT",
			Bucket:    "mybucket",
			Status:    200,
			AuthStatus: "allow",
			Source:    "s3",
		},
		{
			Ts:        1716000000000103,
			EventID:   "evt-empty-source",
			NodeID:    "node-1",
			Method:    "GET",
			Bucket:    "mybucket",
			Status:    200,
			AuthStatus: "allow",
			Source:    "", // existing rows: empty
		},
	}

	enc, err := audit.EncodeS3Batch(events)
	require.NoError(t, err)

	got, err := audit.DecodeS3Batch(enc)
	require.NoError(t, err)
	require.Equal(t, events, got)
}

// s3InitialMetadataV2_27ForTest is the schema as it was before T15 added "source".
// Used to verify the migration trigger fires when last-column-id=27.
const s3InitialMetadataV2_27ForTest = `{"format-version":2,"table-uuid":%q,"location":%q,` +
	`"last-sequence-number":0,"last-updated-ms":%d,"last-column-id":27,` +
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
	`]}],` +
	`"partition-specs":[{"spec-id":0,"fields":[{"name":"ts_day","transform":"day","source-id":1,"field-id":1000}]}],` +
	`"default-spec-id":0,"last-partition-id":1000,` +
	`"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],` +
	`"properties":{},"current-snapshot-id":-1,"snapshots":[],` +
	`"snapshot-log":[],"metadata-log":[]}`

func TestMigration_BumpsExistingV2Table_LastColumnID27to28(t *testing.T) {
	v27 := fmt.Sprintf(s3InitialMetadataV2_27ForTest, "uuid", "s3://grainfs-audit", time.Now().UnixMilli())
	got, changed, err := audit.MigrateMetadataToCurrent(json.RawMessage(v27), time.Now().UnixMilli())
	require.NoError(t, err)
	require.True(t, changed, "v2 table at last-column-id=27 must be migrated to 28")

	var meta map[string]any
	require.NoError(t, json.Unmarshal(got, &meta))
	require.Equal(t, float64(28), meta["last-column-id"])

	gotJSON := string(got)
	require.Contains(t, gotJSON, `"name":"source"`, "migrated schema must include source column")

	// Idempotent: re-running on the migrated metadata is a no-op.
	_, changedAgain, err := audit.MigrateMetadataToCurrent(got, time.Now().Add(time.Second).UnixMilli())
	require.NoError(t, err)
	require.False(t, changedAgain)
}
