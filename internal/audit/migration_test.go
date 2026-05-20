package audit_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestMigrateMetadataV1ToCurrent(t *testing.T) {
	v1 := fmt.Sprintf(audit.S3InitialMetadataV1ForTest, "uuid", "s3://grainfs-audit", time.Now().UnixMilli())
	got, changed, err := audit.MigrateMetadataToCurrent(json.RawMessage(v1), time.Now().UnixMilli())
	require.NoError(t, err)
	require.True(t, changed)

	var meta map[string]any
	require.NoError(t, json.Unmarshal(got, &meta))
	require.Equal(t, float64(27), meta["last-column-id"])
	require.Equal(t, float64(1000), meta["last-partition-id"])
	specs := meta["partition-specs"].([]any)
	require.Len(t, specs, 2)
	require.Equal(t, float64(0), specs[0].(map[string]any)["spec-id"])
	require.Empty(t, specs[0].(map[string]any)["fields"], "legacy unpartitioned spec must remain readable")
	require.Equal(t, float64(1), specs[1].(map[string]any)["spec-id"])
	require.Equal(t, float64(1), meta["default-spec-id"])

	gotAgain, changedAgain, err := audit.MigrateMetadataToCurrent(got, time.Now().Add(time.Second).UnixMilli())
	require.NoError(t, err)
	require.False(t, changedAgain)
	require.JSONEq(t, string(got), string(gotAgain))
}

func TestMigrateMetadataCurrentNoop(t *testing.T) {
	current := fmt.Sprintf(audit.S3InitialMetadata, "uuid", "s3://grainfs-audit", time.Now().UnixMilli())
	got, changed, err := audit.MigrateMetadataToCurrent(json.RawMessage(current), time.Now().UnixMilli())
	require.NoError(t, err)
	require.False(t, changed)
	require.JSONEq(t, current, string(got))
}

// s3InitialMetadataV2_23ForTest is the audit.s3 metadata.json as it shipped
// before T48' added ids 24-27. Used to verify the migration trigger advances
// when the canonical schema grows.
const s3InitialMetadataV2_23ForTest = `{"format-version":2,"table-uuid":%q,"location":%q,` +
	`"last-sequence-number":0,"last-updated-ms":%d,"last-column-id":23,` +
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
	`{"id":23,"name":"copy_source_key","required":false,"type":"string"}` +
	`]}],` +
	`"partition-specs":[{"spec-id":0,"fields":[{"name":"ts_day","transform":"day","source-id":1,"field-id":1000}]}],` +
	`"default-spec-id":0,"last-partition-id":1000,` +
	`"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],` +
	`"properties":{},"current-snapshot-id":-1,"snapshots":[],` +
	`"snapshot-log":[],"metadata-log":[]}`

func TestMigration_BumpsExistingV2Table_LastColumnID23to27(t *testing.T) {
	v2 := fmt.Sprintf(s3InitialMetadataV2_23ForTest, "uuid", "s3://grainfs-audit", time.Now().UnixMilli())
	got, changed, err := audit.MigrateMetadataToCurrent(json.RawMessage(v2), time.Now().UnixMilli())
	require.NoError(t, err)
	require.True(t, changed, "v2 table at last-column-id=23 must be migrated")

	var meta map[string]any
	require.NoError(t, json.Unmarshal(got, &meta))
	require.Equal(t, float64(27), meta["last-column-id"])

	gotJSON := string(got)
	for _, want := range []string{
		`"name":"matched_policy_id"`,
		`"name":"matched_sid"`,
		`"name":"authz_latency_us"`,
		`"name":"condition_context_json"`,
	} {
		require.Contains(t, gotJSON, want)
	}

	// Idempotent: re-running on the migrated metadata is a no-op.
	_, changedAgain, err := audit.MigrateMetadataToCurrent(got, time.Now().Add(time.Second).UnixMilli())
	require.NoError(t, err)
	require.False(t, changedAgain)
}
