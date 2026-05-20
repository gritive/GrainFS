package audit

import (
	"encoding/json"
	"fmt"
)

func MigrateMetadataToCurrent(raw json.RawMessage, nowMs int64) (json.RawMessage, bool, error) {
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, false, err
	}
	changed := false

	if getInt64(meta, "last-column-id") < currentSchemaLastColumnID {
		var current map[string]any
		currentJSON := []byte(fmt.Sprintf(S3InitialMetadata, "migration-probe", "s3://grainfs-audit", nowMs))
		if err := json.Unmarshal(currentJSON, &current); err != nil {
			return nil, false, err
		}
		meta["last-column-id"] = current["last-column-id"]
		meta["schemas"] = current["schemas"]
		changed = true
	}

	specs, _ := meta["partition-specs"].([]any)
	if !hasDayPartitionSpec(specs) {
		newSpecID := nextPartitionSpecID(specs)
		specs = append(specs, map[string]any{
			"spec-id": float64(newSpecID),
			"fields": []any{map[string]any{
				"name": "ts_day", "transform": "day", "source-id": float64(1), "field-id": float64(1000),
			}},
		})
		meta["partition-specs"] = specs
		meta["default-spec-id"] = float64(newSpecID)
		meta["last-partition-id"] = float64(1000)
		changed = true
	}

	if !changed {
		return raw, false, nil
	}
	meta["last-updated-ms"] = float64(nowMs)
	out, err := json.Marshal(meta)
	return json.RawMessage(out), true, err
}

func hasDayPartitionSpec(specs []any) bool {
	for _, raw := range specs {
		spec, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fields, _ := spec["fields"].([]any)
		for _, rawField := range fields {
			field, ok := rawField.(map[string]any)
			if !ok {
				continue
			}
			if field["name"] == "ts_day" && field["transform"] == "day" && getInt64(field, "source-id") == 1 {
				return true
			}
		}
	}
	return false
}

func nextPartitionSpecID(specs []any) int64 {
	var max int64 = -1
	for _, raw := range specs {
		spec, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id := getInt64(spec, "spec-id"); id > max {
			max = id
		}
	}
	return max + 1
}
