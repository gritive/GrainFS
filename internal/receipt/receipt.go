// Package receipt produces audit-ready records for healing sessions.
//
// A HealReceipt aggregates HealEvents from one correlated repair session
// (see internal/scrubber) into a single signed artifact operators can attach
// to postmortems. Signing uses HMAC-SHA256 over a JCS-canonicalized payload
// (RFC 8785), keyed by KeyID so keys can rotate while older receipts remain
// verifiable within the retention window.
package receipt

import "time"

// ObjectRef identifies the object that was repaired.
type ObjectRef struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
}

// HealReceipt is the signed summary of one repair session.
//
// CanonicalPayload and Signature are produced by Sign; callers never populate
// them directly. CanonicalPayload is the JCS-serialized form of all other
// fields (alphabetically ordered, no whitespace) and is what Signature covers.
type HealReceipt struct {
	ReceiptID        string    `json:"receipt_id"`
	KeyID            string    `json:"key_id"`
	Timestamp        time.Time `json:"timestamp"`
	Object           ObjectRef `json:"object"`
	ShardsLost       []int32   `json:"shards_lost"`
	ShardsRebuilt    []int32   `json:"shards_rebuilt"`
	ParityUsed       int       `json:"parity_used"`
	PeersInvolved    []string  `json:"peers_involved"`
	DurationMs       uint32    `json:"duration_ms"`
	EventIDs         []string  `json:"event_ids"`
	CorrelationID    string    `json:"correlation_id,omitempty"`
	CanonicalPayload string    `json:"canonical_payload,omitempty"`
	Signature        string    `json:"signature,omitempty"`
}
