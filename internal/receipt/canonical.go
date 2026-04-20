package receipt

import (
	"encoding/json"
	"fmt"
)

// canonicalTimestampLayout is part of the signed payload. Changing it invalidates
// every existing signature, so any update must be paired with a key rotation.
const canonicalTimestampLayout = "2006-01-02T15:04:05.999999999Z"

// canonicalize returns the JCS-compliant (RFC 8785) serialization of r,
// excluding the CanonicalPayload and Signature fields. Signing code feeds the
// result into HMAC-SHA256; verification recomputes the same form and compares.
//
// Scope: HealReceipt contains only strings, integers, arrays, and one nested
// struct (ObjectRef). No floats, no maps with non-string keys, no NaN/Inf, no
// arbitrary user JSON. Under those constraints Go's stdlib json.Marshal of a
// map[string]any produces output that matches JCS (keys sorted, no whitespace,
// UTF-8 output, canonical integer form). We build the map explicitly so field
// order is not dependent on struct definition.
func canonicalize(r *HealReceipt) ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("receipt: canonicalize nil receipt")
	}
	m := map[string]any{
		"receipt_id":     r.ReceiptID,
		"key_id":         r.KeyID,
		"timestamp":      r.Timestamp.UTC().Format(canonicalTimestampLayout),
		"object":         objectMap(r.Object),
		"shards_lost":    normalizeInt32Slice(r.ShardsLost),
		"shards_rebuilt": normalizeInt32Slice(r.ShardsRebuilt),
		"parity_used":    r.ParityUsed,
		"peers_involved": normalizeStringSlice(r.PeersInvolved),
		"duration_ms":    r.DurationMs,
		"event_ids":      normalizeStringSlice(r.EventIDs),
	}
	return json.Marshal(m)
}

func objectMap(o ObjectRef) map[string]any {
	m := map[string]any{
		"bucket": o.Bucket,
		"key":    o.Key,
	}
	if o.VersionID != "" {
		m["version_id"] = o.VersionID
	}
	return m
}

// normalizeInt32Slice coerces nil to [] so JSON output is stable ("[]" not "null").
func normalizeInt32Slice(s []int32) []int32 {
	if s == nil {
		return []int32{}
	}
	return s
}

func normalizeStringSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
