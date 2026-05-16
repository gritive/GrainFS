package server

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func buildInitialIcebergMetadata(location string, schema json.RawMessage, props map[string]string) json.RawMessage {
	if len(schema) == 0 {
		schema = json.RawMessage(`{"type":"struct","schema-id":0,"fields":[]}`)
	}
	data, _ := json.Marshal(map[string]any{
		"format-version":        2,
		"table-uuid":            uuid.NewSHA1(uuid.NameSpaceURL, []byte(location)).String(),
		"location":              location,
		"last-sequence-number":  0,
		"last-updated-ms":       time.Now().UnixMilli(),
		"last-column-id":        0,
		"schemas":               []json.RawMessage{schema},
		"current-schema-id":     0,
		"partition-specs":       []map[string]any{{"spec-id": 0, "fields": []any{}}},
		"default-spec-id":       0,
		"last-partition-id":     999,
		"sort-orders":           []map[string]any{{"order-id": 0, "fields": []any{}}},
		"default-sort-order-id": 0,
		"properties":            nonNilMap(props),
		"current-snapshot-id":   -1,
		"snapshots":             []any{},
		"snapshot-log":          []any{},
		"metadata-log":          []any{},
	})
	return data
}

func nonNilMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	return in
}
