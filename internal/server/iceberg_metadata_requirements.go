package server

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

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
