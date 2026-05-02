package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

type noDBProviderBackend struct {
	storage.Backend
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

func TestIcebergConfigFollowsAuthMiddleware(t *testing.T) {
	base := setupAuthServer(t)

	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=warehouse")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")
}

func TestIcebergCreateNamespaceAndTableTransactionCommit(t *testing.T) {
	base := setupTestServer(t)

	createIcebergWarehouseBucket(t, base)
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

func TestIcebergTransactionCommitRejectsStaleSnapshotRequirement(t *testing.T) {
	base := setupTestServer(t)

	createIcebergWarehouseBucket(t, base)
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

	req, err := http.NewRequest(http.MethodDelete, base+"/iceberg/v1/namespaces/ns2", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.Equal(t, "NotImplementedException", decodeIcebergErrorType(t, resp))
}

func postIcebergJSON(t *testing.T, url, body string, wantStatus int) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, wantStatus, resp.StatusCode)
}

func createIcebergWarehouseBucket(t *testing.T, base string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+"/grainfs-tables", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
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
