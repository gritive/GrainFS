package receipt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sampleReceipt() *HealReceipt {
	return &HealReceipt{
		ReceiptID:     "0197c0de-f00d-7aaa-b000-000000000001",
		KeyID:         "psk-2026-04",
		Timestamp:     time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC),
		Object:        ObjectRef{Bucket: "photos", Key: "summer/01.jpg", VersionID: "v1"},
		ShardsLost:    []int32{3, 5},
		ShardsRebuilt: []int32{3, 5},
		ParityUsed:    2,
		PeersInvolved: []string{"peer-a", "peer-b", "peer-c"},
		DurationMs:    842,
		EventIDs:      []string{"evt-1", "evt-2"},
	}
}

func TestCanonicalize_DeterministicAcrossCalls(t *testing.T) {
	r := sampleReceipt()

	a, err := canonicalize(r)
	require.NoError(t, err)
	b, err := canonicalize(r)
	require.NoError(t, err)

	require.Equal(t, a, b, "canonicalize must be byte-identical across calls")
}

func TestCanonicalize_IgnoresPayloadAndSignature(t *testing.T) {
	r := sampleReceipt()
	want, err := canonicalize(r)
	require.NoError(t, err)

	// Simulate a round-trip that already populated the output fields.
	r.CanonicalPayload = string(want)
	r.Signature = "tampered-or-not-it-does-not-matter"

	got, err := canonicalize(r)
	require.NoError(t, err)

	require.Equal(t, want, got, "canonical form must exclude CanonicalPayload + Signature fields")
}

func TestCanonicalize_FieldsAreAlphabeticallyOrdered(t *testing.T) {
	r := sampleReceipt()
	b, err := canonicalize(r)
	require.NoError(t, err)

	s := string(b)

	// JCS RFC 8785 sorts object members by UTF-16 code point, which for ASCII
	// field names collapses to lexicographic order. Verify the top-level keys
	// appear in alphabetical order in the serialized output.
	fields := []string{
		"duration_ms",
		"event_ids",
		"key_id",
		"object",
		"parity_used",
		"peers_involved",
		"receipt_id",
		"shards_lost",
		"shards_rebuilt",
		"timestamp",
	}
	prev := -1
	for _, f := range fields {
		idx := indexOfField(s, f)
		require.Greater(t, idx, prev, "field %q must appear after preceding sorted fields; got idx=%d prev=%d", f, idx, prev)
		prev = idx
	}
}

func TestCanonicalize_NoWhitespace(t *testing.T) {
	r := sampleReceipt()
	b, err := canonicalize(r)
	require.NoError(t, err)

	for _, c := range b {
		require.NotEqual(t, byte(' '), c, "whitespace not allowed in canonical form")
		require.NotEqual(t, byte('\n'), c, "newline not allowed in canonical form")
		require.NotEqual(t, byte('\t'), c, "tab not allowed in canonical form")
	}
}

// indexOfField returns the index of `"name":` within s, or -1 if absent.
func indexOfField(s, name string) int {
	target := `"` + name + `":`
	for i := 0; i+len(target) <= len(s); i++ {
		if s[i:i+len(target)] == target {
			return i
		}
	}
	return -1
}
