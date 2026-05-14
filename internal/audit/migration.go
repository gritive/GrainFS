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

	if getInt64(meta, "last-column-id") < 23 {
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
	needsPartition := len(specs) == 0
	if len(specs) > 0 {
		fields, _ := specs[0].(map[string]any)["fields"].([]any)
		needsPartition = len(fields) == 0
	}
	if needsPartition {
		meta["partition-specs"] = []any{map[string]any{
			"spec-id": float64(0),
			"fields": []any{map[string]any{
				"name": "ts_day", "transform": "day", "source-id": float64(1), "field-id": float64(1000),
			}},
		}}
		meta["default-spec-id"] = float64(0)
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
