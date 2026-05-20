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
