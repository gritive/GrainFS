package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

type noDBProviderBackend struct {
	storage.Backend
}

type fakeIcebergCatalog struct {
	warehouse string
}

func (f fakeIcebergCatalog) Warehouse() string { return f.warehouse }
func (f fakeIcebergCatalog) CreateNamespace(context.Context, string, []string, map[string]string) error {
	return icebergcatalog.ErrNamespaceExists
}
func (f fakeIcebergCatalog) LoadNamespace(context.Context, string, []string) (map[string]string, error) {
	return nil, icebergcatalog.ErrNamespaceNotFound
}
func (f fakeIcebergCatalog) ListNamespaces(context.Context, string) ([][]string, error) {
	return nil, nil
}
func (f fakeIcebergCatalog) DeleteNamespace(context.Context, string, []string) error {
	return icebergcatalog.ErrNamespaceNotFound
}
func (f fakeIcebergCatalog) CreateTable(context.Context, string, icebergcatalog.Identifier, icebergcatalog.CreateTableInput) (*icebergcatalog.Table, error) {
	return nil, icebergcatalog.ErrTableExists
}
func (f fakeIcebergCatalog) LoadTable(context.Context, string, icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	return nil, icebergcatalog.ErrTableNotFound
}
func (f fakeIcebergCatalog) ListTables(context.Context, string, []string) ([]icebergcatalog.Identifier, error) {
	return nil, nil
}
func (f fakeIcebergCatalog) DeleteTable(context.Context, string, icebergcatalog.Identifier) error {
	return icebergcatalog.ErrTableNotFound
}
func (f fakeIcebergCatalog) CommitTable(context.Context, string, icebergcatalog.Identifier, icebergcatalog.CommitTableInput) (*icebergcatalog.Table, error) {
	return nil, icebergcatalog.ErrCommitFailed
}

type staleLoadCommitCatalog struct {
	warehouse string
	mu        sync.Mutex
	commits   int
}

func (s *staleLoadCommitCatalog) Warehouse() string { return s.warehouse }
func (s *staleLoadCommitCatalog) CreateNamespace(context.Context, string, []string, map[string]string) error {
	return nil
}
func (s *staleLoadCommitCatalog) LoadNamespace(context.Context, string, []string) (map[string]string, error) {
	return nil, nil
}
func (s *staleLoadCommitCatalog) ListNamespaces(context.Context, string) ([][]string, error) {
	return nil, nil
}
func (s *staleLoadCommitCatalog) DeleteNamespace(context.Context, string, []string) error {
	return nil
}
func (s *staleLoadCommitCatalog) CreateTable(context.Context, string, icebergcatalog.Identifier, icebergcatalog.CreateTableInput) (*icebergcatalog.Table, error) {
	return nil, nil
}
func (s *staleLoadCommitCatalog) LoadTable(context.Context, string, icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	return &icebergcatalog.Table{
		Identifier:       icebergcatalog.Identifier{Namespace: []string{"ns2"}, Name: "t"},
		MetadataLocation: "s3://grainfs-tables/warehouse/ns2/t/metadata/00000.json",
		Metadata:         buildInitialIcebergMetadata("s3://grainfs-tables/warehouse/ns2/t", json.RawMessage(`{"type":"struct","fields":[],"schema-id":0}`), nil),
	}, nil
}
func (s *staleLoadCommitCatalog) ListTables(context.Context, string, []string) ([]icebergcatalog.Identifier, error) {
	return nil, nil
}
func (s *staleLoadCommitCatalog) DeleteTable(context.Context, string, icebergcatalog.Identifier) error {
	return nil
}
func (s *staleLoadCommitCatalog) CommitTable(_ context.Context, _ string, ident icebergcatalog.Identifier, in icebergcatalog.CommitTableInput) (*icebergcatalog.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commits++
	wantExpected := "s3://grainfs-tables/warehouse/ns2/t/metadata/00000.json"
	wantNext := "s3://grainfs-tables/warehouse/ns2/t/metadata/00001.json"
	if s.commits == 2 {
		wantExpected = "s3://grainfs-tables/warehouse/ns2/t/metadata/00001.json"
		wantNext = "s3://grainfs-tables/warehouse/ns2/t/metadata/00002.json"
	}
	if in.ExpectedMetadataLocation != wantExpected || in.NewMetadataLocation != wantNext {
		return nil, icebergcatalog.ErrCommitFailed
	}
	return &icebergcatalog.Table{
		Identifier:       ident,
		MetadataLocation: in.NewMetadataLocation,
		Metadata:         append(json.RawMessage(nil), in.Metadata...),
	}, nil
}

func (s *staleLoadCommitCatalog) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

func TestIcebergConfigUsesJSONAndBypassesS3Routes(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=warehouse")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	var got struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "s3://grainfs-tables/warehouse", got.Defaults["warehouse"])
}

func TestIcebergAIStorAliasSupportsWarpWarehouseAndConfig(t *testing.T) {
	base := setupTestServer(t)

	postIcebergJSON(t, base+"/_iceberg/v1/warehouses", `{"name":"warehouse"}`, http.StatusOK)

	resp, err := http.Get(base + "/_iceberg/v1/config?warehouse=warehouse")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	var got struct {
		Defaults map[string]string `json:"defaults"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "s3://grainfs-tables/warehouse", got.Defaults["warehouse"])
}

func TestIcebergConfigUsesInjectedCatalogInterface(t *testing.T) {
	base := setupTestServerWithOptions(t, WithIcebergCatalog(fakeIcebergCatalog{warehouse: "s3://custom/warehouse"}))

	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=warehouse")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		Defaults map[string]string `json:"defaults"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "s3://custom/warehouse", got.Defaults["warehouse"])
}

func TestIcebergCreateNamespaceAndTableTransactionCommit(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)

	createIcebergWarehouseBucket(t, backend)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces", `{"namespace":["ns2"],"properties":{}}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces/ns2/tables", `{
		"stage-create": false,
		"name": "t",
		"schema": {"type":"struct","fields":[{"name":"a","id":1,"type":"int","required":false}],"schema-id":0},
		"partition-spec": {"spec-id":0,"type":"struct","fields":[]},
		"write-order": {"order-id":0,"fields":[]},
		"properties": {"format-version":"2"}
	}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/transactions/commit", `{
		"table-changes":[{
			"requirements":[],
			"updates":[
				{"action":"assign-uuid","uuid":"ns2-t-uuid"},
				{"action":"set-location","location":"s3://grainfs-tables/warehouse/ns2/t"},
				{"action":"add-snapshot","snapshot":{"snapshot-id":7,"sequence-number":1,"timestamp-ms":1777711820223,"manifest-list":"s3://grainfs-tables/warehouse/ns2/t/metadata/snap-7.avro","summary":{"operation":"overwrite"},"schema-id":0}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":7}
			],
			"identifier":{"name":"t","namespace":["ns2"]}
		}]
	}`, http.StatusOK)

	resp, err := http.Get(base + "/iceberg/v1/namespaces/ns2/tables/t")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		MetadataLocation string `json:"metadata-location"`
		Metadata         struct {
			CurrentSnapshotID int64 `json:"current-snapshot-id"`
		} `json:"metadata"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "s3://grainfs-tables/warehouse/ns2/t/metadata/00001.json", got.MetadataLocation)
	require.EqualValues(t, 7, got.Metadata.CurrentSnapshotID)

	resp, err = http.Get(base + "/grainfs-tables/warehouse/ns2/t/metadata/00001.json")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var metadata struct {
		CurrentSnapshotID int64 `json:"current-snapshot-id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&metadata))
	require.EqualValues(t, 7, metadata.CurrentSnapshotID)
}

func TestIcebergTableScopedCommitAndDelete(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)

	createIcebergWarehouseBucket(t, backend)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces", `{"namespace":["ns2"],"properties":{}}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces/ns2/tables", `{
		"name": "t",
		"schema": {"type":"struct","fields":[{"name":"a","id":1,"type":"int","required":false}],"schema-id":0},
		"properties": {"format-version":"2"}
	}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces/ns2/tables/t", `{
		"requirements":[],
		"updates":[
			{"action":"add-snapshot","snapshot":{"snapshot-id":9,"sequence-number":1,"timestamp-ms":1777711820223,"manifest-list":"s3://grainfs-tables/warehouse/ns2/t/metadata/snap-9.avro","summary":{"operation":"overwrite"},"schema-id":0}},
			{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":9}
		]
	}`, http.StatusOK)

	req, err := http.NewRequest(http.MethodDelete, base+"/iceberg/v1/namespaces/ns2", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "NamespaceNotEmptyException", decodeIcebergErrorType(t, resp))

	req, err = http.NewRequest(http.MethodDelete, base+"/iceberg/v1/namespaces/ns2/tables/t", nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	req, err = http.NewRequest(http.MethodDelete, base+"/iceberg/v1/namespaces/ns2", nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// TestMetaCatalog_CreateTable_MetadataPathIsS3URL is an F16 regression test.
// Before the fix, icebergCreateTable used the FSM warehouse key ("default" or
// a bearer claims.Warehouse) as the S3 path prefix, producing non-s3:// locations
// that parseS3Location rejects. After the fix it must use S3URLPrefix().
func TestMetaCatalog_CreateTable_MetadataPathIsS3URL(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	createIcebergWarehouseBucket(t, backend)

	postIcebergJSON(t, base+"/iceberg/v1/namespaces",
		`{"namespace":["regression16"],"properties":{}}`, http.StatusOK)

	resp, err := http.Post(base+"/iceberg/v1/namespaces/regression16/tables",
		"application/json", strings.NewReader(`{
			"name": "tbl",
			"schema": {"type":"struct","fields":[],"schema-id":0},
			"properties": {}
		}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	// Must succeed — before the fix this returned 500 (invalid metadata location).
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateTable must succeed with valid S3URLPrefix")

	var got struct {
		MetadataLocation string `json:"metadata-location"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.True(t, strings.HasPrefix(got.MetadataLocation, "s3://"),
		"MetadataLocation must start with s3://, got: %s", got.MetadataLocation)
	require.False(t, strings.HasPrefix(got.MetadataLocation, "default/"),
		"MetadataLocation must NOT start with 'default/', got: %s", got.MetadataLocation)
}

func TestIcebergTransactionCommitRejectsStaleSnapshotRequirement(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)

	createIcebergWarehouseBucket(t, backend)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces", `{"namespace":["ns2"],"properties":{}}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/namespaces/ns2/tables", `{
		"name": "t",
		"schema": {"type":"struct","fields":[{"name":"a","id":1,"type":"int","required":false}],"schema-id":0},
		"properties": {"format-version":"2"}
	}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/transactions/commit", `{
		"table-changes":[{
			"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
			"updates":[
				{"action":"add-snapshot","snapshot":{"snapshot-id":7,"sequence-number":1,"timestamp-ms":1777711820223,"manifest-list":"s3://grainfs-tables/warehouse/ns2/t/metadata/snap-7.avro","summary":{"operation":"overwrite"},"schema-id":0}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":7}
			],
			"identifier":{"name":"t","namespace":["ns2"]}
		}]
	}`, http.StatusOK)
	postIcebergJSON(t, base+"/iceberg/v1/transactions/commit", `{
		"table-changes":[{
			"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":6}],
			"updates":[
				{"action":"add-snapshot","snapshot":{"snapshot-id":8,"sequence-number":2,"timestamp-ms":1777711821223,"manifest-list":"s3://grainfs-tables/warehouse/ns2/t/metadata/snap-8.avro","summary":{"operation":"overwrite"},"schema-id":0}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":8}
			],
			"identifier":{"name":"t","namespace":["ns2"]}
		}]
	}`, http.StatusConflict)

	resp, err := http.Get(base + "/iceberg/v1/namespaces/ns2/tables/t")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		Metadata struct {
			CurrentSnapshotID int64 `json:"current-snapshot-id"`
		} `json:"metadata"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.EqualValues(t, 7, got.Metadata.CurrentSnapshotID)
}

func TestIcebergTransactionCommitReusesCommittedTableWithinRequest(t *testing.T) {
	catalog := &staleLoadCommitCatalog{warehouse: "s3://grainfs-tables/warehouse"}
	base, backend := setupTestServerWithBackend(t, WithIcebergCatalog(catalog))

	createIcebergWarehouseBucket(t, backend)
	postIcebergJSON(t, base+"/iceberg/v1/transactions/commit", `{
		"table-changes":[{
			"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],
			"updates":[
				{"action":"add-snapshot","snapshot":{"snapshot-id":7,"sequence-number":1,"timestamp-ms":1777711820223,"manifest-list":"s3://grainfs-tables/warehouse/ns2/t/metadata/snap-7.avro","summary":{"operation":"overwrite"},"schema-id":0}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":7}
			],
			"identifier":{"name":"t","namespace":["ns2"]}
		},{
			"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":7}],
			"updates":[
				{"action":"add-snapshot","snapshot":{"snapshot-id":8,"sequence-number":2,"timestamp-ms":1777711821223,"manifest-list":"s3://grainfs-tables/warehouse/ns2/t/metadata/snap-8.avro","summary":{"operation":"append"},"schema-id":0}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":8}
			],
			"identifier":{"name":"t","namespace":["ns2"]}
		}]
	}`, http.StatusOK)
	require.Equal(t, 2, catalog.commitCount())
}

func TestIcebergSnapshotRequirementAllowsDuckDBRoundedLargeIDs(t *testing.T) {
	metadata := json.RawMessage(`{"refs":{"main":{"type":"branch","snapshot-id":493852316999895052}}}`)
	require.NoError(t, validateIcebergRequirements(metadata, []json.RawMessage{
		json.RawMessage(`{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":493852316999895040}`),
	}))
}

func TestIcebergMissingResourcesReturnJSONErrors(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.Get(base + "/iceberg/v1/namespaces/missing")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, "NoSuchNamespaceException", decodeIcebergErrorType(t, resp))

	postIcebergJSON(t, base+"/iceberg/v1/namespaces", `{"namespace":["ns2"],"properties":{}}`, http.StatusOK)
	resp, err = http.Get(base + "/iceberg/v1/namespaces/ns2/tables/missing")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, "NoSuchTableException", decodeIcebergErrorType(t, resp))
}

func TestIcebergUnsupportedBackendReturnsJSON501(t *testing.T) {
	base := setupNoIcebergStoreServer(t)

	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=warehouse")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.Equal(t, "NotImplementedException", decodeIcebergErrorType(t, resp))
}

func TestIcebergUnsupportedOperationReturnsJSON(t *testing.T) {
	base := setupTestServer(t)

	req, err := http.NewRequest(http.MethodPatch, base+"/iceberg/v1/namespaces/ns2", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.Equal(t, "NotImplementedException", decodeIcebergErrorType(t, resp))
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

func postIcebergJSON(t *testing.T, url, body string, wantStatus int) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, wantStatus, resp.StatusCode)
}

func createIcebergWarehouseBucket(t *testing.T, backend *storage.LocalBackend) {
	t.Helper()
	require.NoError(t, backend.CreateBucket(t.Context(), "grainfs-tables"))
}

func decodeIcebergErrorType(t *testing.T, resp *http.Response) string {
	t.Helper()
	var got struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	return got.Error.Type
}

func setupNoIcebergStoreServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, noDBProviderBackend{Backend: backend})
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr
}
