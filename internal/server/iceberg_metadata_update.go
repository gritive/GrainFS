package server

import "encoding/json"

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
