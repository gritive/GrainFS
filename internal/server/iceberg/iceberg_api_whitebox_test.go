package iceberg

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIcebergSnapshotRequirementAllowsDuckDBRoundedLargeIDs(t *testing.T) {
	metadata := json.RawMessage(`{"refs":{"main":{"type":"branch","snapshot-id":493852316999895052}}}`)
	require.NoError(t, validateIcebergRequirements(metadata, []json.RawMessage{
		json.RawMessage(`{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":493852316999895040}`),
	}))
}

// TestIcebergCreateTable_TwoWarehousesDistinctPaths verifies that F21 is fixed:
// two warehouses sharing the same (ns, table) name must produce distinct S3
// metadata paths, and the "default" warehouse must use the backward-compatible
// path (no warehouse segment).
func TestIcebergCreateTable_TwoWarehousesDistinctPaths(t *testing.T) {
	const s3Prefix = "s3://grainfs-tables/warehouse"
	const ns = "analytics"
	const table = "events"

	pathDefault := icebergTableBasePath(s3Prefix, "default", ns, table)
	pathEmpty := icebergTableBasePath(s3Prefix, "", ns, table)
	pathA := icebergTableBasePath(s3Prefix, "warehouse-a", ns, table)
	pathB := icebergTableBasePath(s3Prefix, "warehouse-b", ns, table)

	// Backward compat: "default" and "" both omit the warehouse segment.
	require.Equal(t, s3Prefix+"/"+ns+"/"+table, pathDefault,
		"default warehouse must not include warehouse segment")
	require.Equal(t, pathDefault, pathEmpty,
		"empty warehouse must behave identically to default")

	// Non-default warehouses must include the warehouse segment.
	require.Contains(t, pathA, "/warehouse-a/",
		"non-default warehouse-a must include warehouse segment")
	require.Contains(t, pathB, "/warehouse-b/",
		"non-default warehouse-b must include warehouse segment")

	// Distinct warehouses must produce distinct paths for the same (ns, table).
	require.NotEqual(t, pathA, pathDefault,
		"warehouse-a path must differ from default path")
	require.NotEqual(t, pathA, pathB,
		"warehouse-a path must differ from warehouse-b path")
}

// TestIcebergTableBasePath_S3LogicalWarehouseIncludesSegment verifies that a
// URI-shaped logical warehouse name (e.g. a crafted bearer claim "s3://attacker/x")
// that differs from s3Prefix is NOT treated as its own prefix and DOES include
// the warehouse segment in the path (F24 defense in depth).
func TestIcebergTableBasePath_S3LogicalWarehouseIncludesSegment(t *testing.T) {
	const s3Prefix = "s3://grainfs-tables/warehouse"
	const ns = "ns"
	const table = "tbl"

	// Legacy mode: Warehouse() returns the full S3 URI, equal to s3Prefix.
	// The segment must be omitted (backward-compatible).
	pathLegacy := icebergTableBasePath(s3Prefix, s3Prefix, ns, table)
	require.Equal(t, s3Prefix+"/"+ns+"/"+table, pathLegacy,
		"when warehouse==s3Prefix (legacy Store mode), warehouse segment must be omitted")

	// MetaCatalog mode: logical warehouse is a URI-shaped name but != s3Prefix.
	// The warehouse segment MUST be included so paths remain isolated.
	uriWarehouse := "s3://attacker.com/x"
	pathAttacker := icebergTableBasePath(s3Prefix, uriWarehouse, ns, table)
	require.Contains(t, pathAttacker, uriWarehouse,
		"URI-shaped logical warehouse must include the warehouse segment in the path (F24)")
	require.NotEqual(t, s3Prefix+"/"+ns+"/"+table, pathAttacker,
		"URI-shaped logical warehouse path must differ from the default path")
}
