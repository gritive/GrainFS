package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) registerIcebergAPI(h *server.Hertz) {
	h.GET("/iceberg/v1/config", s.icebergConfig)
	h.GET("/iceberg/v1/namespaces", s.icebergListNamespaces)
	h.POST("/iceberg/v1/namespaces", s.icebergCreateNamespace)
	h.GET("/iceberg/v1/namespaces/:namespace", s.icebergLoadNamespace)
	h.HEAD("/iceberg/v1/namespaces/:namespace", s.icebergHeadNamespace)
	h.DELETE("/iceberg/v1/namespaces/:namespace", s.icebergDeleteNamespace)
	h.GET("/iceberg/v1/namespaces/:namespace/tables", s.icebergListTables)
	h.POST("/iceberg/v1/namespaces/:namespace/tables", s.icebergCreateTable)
	h.GET("/iceberg/v1/namespaces/:namespace/tables/:table", s.icebergLoadTable)
	h.HEAD("/iceberg/v1/namespaces/:namespace/tables/:table", s.icebergHeadTable)
	h.POST("/iceberg/v1/namespaces/:namespace/tables/:table", s.icebergCommitTable)
	h.DELETE("/iceberg/v1/namespaces/:namespace/tables/:table", s.icebergDeleteTable)
	h.POST("/iceberg/v1/transactions/commit", s.icebergCommitTransaction)
	h.Any("/iceberg/*path", s.icebergUnsupported)
}

func (s *Server) requireIceberg(c *app.RequestContext) (icebergcatalog.Catalog, bool) {
	if s.icebergCatalog == nil {
		writeIcebergError(c, consts.StatusNotImplemented, "NotImplementedException", "Iceberg REST Catalog is only supported for local Badger-backed servers in this release")
		return nil, false
	}
	return s.icebergCatalog, true
}

func (s *Server) icebergConfig(_ context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	c.JSON(consts.StatusOK, map[string]map[string]string{
		"defaults":  {"warehouse": store.Warehouse()},
		"overrides": {},
	})
}

func (s *Server) icebergListNamespaces(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	namespaces, err := store.ListNamespaces(ctx)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespaces": namespaces})
}

func (s *Server) icebergCreateNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	var req struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid namespace request")
		return
	}
	if err := store.CreateNamespace(ctx, req.Namespace, req.Properties); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespace": req.Namespace, "properties": nonNilMap(req.Properties)})
}

func (s *Server) icebergLoadNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ns := []string{c.Param("namespace")}
	props, err := store.LoadNamespace(ctx, ns)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespace": ns, "properties": props})
}

func (s *Server) icebergHeadNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if _, err := store.LoadNamespace(ctx, []string{c.Param("namespace")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergDeleteNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if err := store.DeleteNamespace(ctx, []string{c.Param("namespace")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergListTables(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	tables, err := store.ListTables(ctx, []string{c.Param("namespace")})
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"identifiers": tables})
}

func (s *Server) icebergCreateTable(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ns := []string{c.Param("namespace")}
	var req struct {
		Name       string            `json:"name"`
		Schema     json.RawMessage   `json:"schema"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid table request")
		return
	}
	ident := icebergcatalog.Identifier{Namespace: ns, Name: req.Name}
	location := fmt.Sprintf("%s/%s/%s", store.Warehouse(), ns[0], req.Name)
	metadataLocation := location + "/metadata/00000.json"
	metadata := buildInitialIcebergMetadata(location, req.Schema, req.Properties)
	if _, err := store.LoadTable(ctx, ident); err == nil {
		writeIcebergMappedError(c, icebergcatalog.ErrTableExists)
		return
	} else if err != nil && !errors.Is(err, icebergcatalog.ErrTableNotFound) {
		writeIcebergMappedError(c, err)
		return
	}
	if err := s.writeIcebergMetadataObject(metadataLocation, metadata); err != nil {
		writeIcebergStorageError(c, err)
		return
	}
	tbl, err := store.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: metadataLocation,
		Metadata:         metadata,
		Properties:       req.Properties,
	})
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	writeIcebergTable(c, tbl)
}

func (s *Server) icebergLoadTable(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	tbl, err := store.LoadTable(ctx, icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")})
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	writeIcebergTable(c, tbl)
}

func (s *Server) icebergHeadTable(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if _, err := store.LoadTable(ctx, icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergDeleteTable(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ident := icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}
	if err := store.DeleteTable(ctx, ident); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergCommitTable(ctx context.Context, c *app.RequestContext) {
	var req struct {
		Requirements []json.RawMessage `json:"requirements"`
		Updates      []json.RawMessage `json:"updates"`
	}
	if err := decodeIcebergBody(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid table commit request")
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ident := icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}
	tbl, err := store.LoadTable(ctx, ident)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	tbl, ok = s.commitIcebergTableFrom(ctx, c, store, tbl, req.Requirements, req.Updates)
	if !ok {
		return
	}
	writeIcebergTable(c, tbl)
}

func (s *Server) icebergCommitTransaction(ctx context.Context, c *app.RequestContext) {
	var req struct {
		TableChanges []struct {
			Identifier   icebergcatalog.Identifier `json:"identifier"`
			Requirements []json.RawMessage         `json:"requirements"`
			Updates      []json.RawMessage         `json:"updates"`
		} `json:"table-changes"`
	}
	if err := decodeIcebergBody(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid transaction commit request")
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	committed := make(map[string]*icebergcatalog.Table)
	for _, change := range req.TableChanges {
		key := icebergIdentifierKey(change.Identifier)
		tbl := committed[key]
		if tbl == nil {
			var err error
			tbl, err = store.LoadTable(ctx, change.Identifier)
			if err != nil {
				writeIcebergMappedError(c, err)
				return
			}
		}
		next, ok := s.commitIcebergTableFrom(ctx, c, store, tbl, change.Requirements, change.Updates)
		if !ok {
			return
		}
		committed[key] = next
	}
	c.JSON(consts.StatusOK, map[string]any{})
}

func (s *Server) commitIcebergTableFrom(ctx context.Context, c *app.RequestContext, store icebergcatalog.Catalog, tbl *icebergcatalog.Table, requirements, updates []json.RawMessage) (*icebergcatalog.Table, bool) {
	ident := tbl.Identifier
	if err := validateIcebergRequirements(tbl.Metadata, requirements); err != nil {
		writeIcebergMappedError(c, err)
		return nil, false
	}
	metadata, err := applyIcebergUpdates(tbl.Metadata, updates)
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid transaction updates")
		return nil, false
	}
	nextMetadataLocation := nextIcebergMetadataLocation(tbl.MetadataLocation)
	if err := s.writeIcebergMetadataObject(nextMetadataLocation, metadata); err != nil {
		writeIcebergStorageError(c, err)
		return nil, false
	}
	committed, err := store.CommitTable(ctx, ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: tbl.MetadataLocation,
		NewMetadataLocation:      nextMetadataLocation,
		Metadata:                 metadata,
	})
	if err != nil {
		writeIcebergMappedError(c, err)
		return nil, false
	}
	return committed, true
}

func icebergIdentifierKey(ident icebergcatalog.Identifier) string {
	return strings.Join(ident.Namespace, "\x00") + "\x00" + ident.Name
}

func (s *Server) icebergUnsupported(_ context.Context, c *app.RequestContext) {
	writeIcebergError(c, consts.StatusNotImplemented, "NotImplementedException", "unsupported Iceberg REST Catalog operation")
}

func writeIcebergTable(c *app.RequestContext, tbl *icebergcatalog.Table) {
	var metadata any
	if len(tbl.Metadata) > 0 {
		_ = json.Unmarshal(tbl.Metadata, &metadata)
	}
	c.JSON(consts.StatusOK, map[string]any{
		"metadata-location": tbl.MetadataLocation,
		"metadata":          metadata,
		"config":            map[string]string{},
	})
}

func writeIcebergMappedError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, icebergcatalog.ErrNamespaceNotFound):
		writeIcebergError(c, consts.StatusNotFound, "NoSuchNamespaceException", "namespace not found")
	case errors.Is(err, icebergcatalog.ErrNamespaceExists):
		writeIcebergError(c, consts.StatusConflict, "AlreadyExistsException", "namespace already exists")
	case errors.Is(err, icebergcatalog.ErrNamespaceNotEmpty):
		writeIcebergError(c, consts.StatusConflict, "NamespaceNotEmptyException", "namespace is not empty")
	case errors.Is(err, icebergcatalog.ErrTableNotFound):
		writeIcebergError(c, consts.StatusNotFound, "NoSuchTableException", "table not found")
	case errors.Is(err, icebergcatalog.ErrTableExists):
		writeIcebergError(c, consts.StatusConflict, "AlreadyExistsException", "table already exists")
	case errors.Is(err, icebergcatalog.ErrCommitFailed):
		writeIcebergError(c, consts.StatusConflict, "CommitFailedException", "table metadata pointer changed")
	case errors.Is(err, icebergcatalog.ErrServiceUnavailable):
		writeIcebergError(c, consts.StatusServiceUnavailable, "ServiceUnavailableException", "Iceberg catalog service unavailable")
	default:
		writeIcebergError(c, consts.StatusInternalServerError, "InternalServerError", err.Error())
	}
}

func writeIcebergStorageError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrNoSuchBucket):
		writeIcebergError(c, consts.StatusNotFound, "NoSuchBucketException", "warehouse bucket not found")
	default:
		writeIcebergError(c, consts.StatusInternalServerError, "InternalServerError", err.Error())
	}
}

func writeIcebergError(c *app.RequestContext, status int, typ, message string) {
	c.JSON(status, map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    typ,
			"code":    status,
		},
	})
}

func buildInitialIcebergMetadata(location string, schema json.RawMessage, props map[string]string) json.RawMessage {
	if len(schema) == 0 {
		schema = json.RawMessage(`{"type":"struct","schema-id":0,"fields":[]}`)
	}
	data, _ := json.Marshal(map[string]any{
		"format-version":        2,
		"table-uuid":            "grainfs-local",
		"location":              location,
		"last-sequence-number":  0,
		"last-updated-ms":       0,
		"last-column-id":        0,
		"schemas":               []json.RawMessage{schema},
		"current-schema-id":     0,
		"partition-specs":       []map[string]any{{"spec-id": 0, "fields": []any{}}},
		"default-spec-id":       0,
		"sort-orders":           []map[string]any{{"order-id": 0, "fields": []any{}}},
		"default-sort-order-id": 0,
		"properties":            nonNilMap(props),
		"snapshots":             []any{},
		"snapshot-log":          []any{},
		"metadata-log":          []any{},
	})
	return data
}

func applyIcebergUpdates(metadata json.RawMessage, updates []json.RawMessage) (json.RawMessage, error) {
	var doc map[string]any
	if len(metadata) > 0 {
		if err := decodeIcebergBody(metadata, &doc); err != nil {
			return nil, err
		}
	}
	if doc == nil {
		doc = map[string]any{}
	}
	for _, raw := range updates {
		var upd map[string]any
		if err := decodeIcebergBody(raw, &upd); err != nil {
			return nil, err
		}
		action, _ := upd["action"].(string)
		switch action {
		case "assign-uuid":
			if uuid, ok := upd["uuid"].(string); ok {
				doc["table-uuid"] = uuid
			}
		case "set-location":
			if location, ok := upd["location"].(string); ok {
				doc["location"] = location
			}
		case "add-snapshot":
			snapshot, ok := upd["snapshot"].(map[string]any)
			if !ok {
				continue
			}
			doc["snapshots"] = appendAnySlice(doc["snapshots"], snapshot)
			if id, ok := snapshot["snapshot-id"]; ok {
				doc["current-snapshot-id"] = id
			}
			if seq, ok := snapshot["sequence-number"]; ok {
				doc["last-sequence-number"] = seq
			}
			if ts, ok := snapshot["timestamp-ms"]; ok {
				doc["last-updated-ms"] = ts
			}
		case "set-snapshot-ref":
			refName, _ := upd["ref-name"].(string)
			if refName == "" {
				refName = "main"
			}
			refs := anyMap(doc["refs"])
			ref := map[string]any{
				"type":        upd["type"],
				"snapshot-id": upd["snapshot-id"],
			}
			refs[refName] = ref
			doc["refs"] = refs
			if refName == "main" {
				doc["current-snapshot-id"] = upd["snapshot-id"]
			}
		}
	}
	return json.Marshal(doc)
}

func validateIcebergRequirements(metadata json.RawMessage, requirements []json.RawMessage) error {
	for _, raw := range requirements {
		var req map[string]any
		if err := decodeIcebergBody(raw, &req); err != nil {
			return err
		}
		switch req["type"] {
		case "assert-ref-snapshot-id":
			ref, _ := req["ref"].(string)
			if ref == "" {
				ref = "main"
			}
			current, currentOK, err := icebergSnapshotRef(metadata, ref)
			if err != nil {
				return err
			}
			expected, expectedOK := req["snapshot-id"]
			if !expectedOK || expected == nil {
				if currentOK {
					return icebergcatalog.ErrCommitFailed
				}
				continue
			}
			if !currentOK || !sameIcebergValue(expected, current) {
				return icebergcatalog.ErrCommitFailed
			}
		}
	}
	return nil
}

func icebergSnapshotRef(metadata json.RawMessage, ref string) (any, bool, error) {
	var doc map[string]any
	if len(metadata) == 0 {
		return nil, false, nil
	}
	if err := decodeIcebergBody(metadata, &doc); err != nil {
		return nil, false, err
	}
	refs, ok := doc["refs"].(map[string]any)
	if !ok {
		return nil, false, nil
	}
	refDoc, ok := refs[ref].(map[string]any)
	if !ok {
		return nil, false, nil
	}
	value, ok := refDoc["snapshot-id"]
	if !ok || value == nil {
		return nil, false, nil
	}
	return value, true, nil
}

func sameIcebergValue(a, b any) bool {
	if af, aok := icebergNumberFloat(a); aok {
		if bf, bok := icebergNumberFloat(b); bok {
			return af == bf
		}
	}
	return icebergScalarString(a) == icebergScalarString(b)
}

func icebergNumberFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func icebergScalarString(v any) string {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return x
	default:
		data, _ := json.Marshal(x)
		return string(data)
	}
}

func decodeIcebergBody(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(out)
}

func appendAnySlice(existing any, value any) []any {
	if values, ok := existing.([]any); ok {
		return append(values, value)
	}
	return []any{value}
}

func anyMap(existing any) map[string]any {
	if values, ok := existing.(map[string]any); ok {
		return values
	}
	return map[string]any{}
}

func nextIcebergMetadataLocation(current string) string {
	const suffix = ".json"
	if !strings.HasSuffix(current, suffix) {
		return current
	}
	slash := strings.LastIndex(current, "/")
	if slash < 0 {
		return current
	}
	prefix := current[:slash+1]
	name := strings.TrimSuffix(current[slash+1:], suffix)
	n, err := strconv.Atoi(name)
	if err != nil {
		return current
	}
	return fmt.Sprintf("%s%05d%s", prefix, n+1, suffix)
}

func nonNilMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	return in
}

func (s *Server) writeIcebergMetadataObject(location string, metadata json.RawMessage) error {
	bucket, key, ok := parseS3Location(location)
	if !ok {
		return fmt.Errorf("invalid Iceberg metadata location: %s", location)
	}
	_, err := s.backend.PutObject(context.Background(), bucket, key, bytes.NewReader(metadata), "application/json")
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func parseS3Location(location string) (bucket, key string, ok bool) {
	const prefix = "s3://"
	if !strings.HasPrefix(location, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(location, prefix)
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", false
	}
	return rest[:slash], rest[slash+1:], true
}
