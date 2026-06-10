package cluster

import (
	"bytes"
	"testing"
)

// TestSnapshot_GCFGTrailerByteDeterminism guards against config map iteration
// order leaking into snapshot bytes (testing specialist CRITICAL F#1). Two
// snapshots of byte-identical config state must produce byte-identical GCFG
// trailers; otherwise raft snapshot replication compares hashes across
// replicas and rejects valid snapshots.
func TestSnapshot_GCFGTrailerByteDeterminism(t *testing.T) {
	// Encode the same config map 16 times; every output must be identical.
	entries := map[string]string{
		"audit.deny-only":                   "true",
		"trusted-proxy.cidr":                "10.0.0.0/8,192.168.0.0/16",
		"cluster.read-only":                 "false",
		"iam.allow-anonymous-bucket-policy": "true",
	}
	first, err := encodeMetaConfigSnapshot(entries)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i := 0; i < 15; i++ {
		got, err := encodeMetaConfigSnapshot(entries)
		if err != nil {
			t.Fatalf("encode #%d: %v", i+1, err)
		}
		if !bytes.Equal(first, got) {
			t.Fatalf("encode iteration %d differs from first — map order leaked into trailer bytes", i+1)
		}
	}
}
