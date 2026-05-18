package receipt

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReceipt_RoundTripFB(t *testing.T) {
	// time.Unix(...) returns a no-monotonic-clock time, and .UTC() locks the
	// location so the marshal/unmarshal round-trip is byte-stable.
	ts := time.Unix(1700000000, 123).UTC()
	r := &HealReceipt{
		ReceiptID:        "rcpt-1",
		KeyID:            "kid-7",
		Timestamp:        ts,
		Object:           ObjectRef{Bucket: "b", Key: "k", VersionID: "v"},
		ShardsLost:       []int32{0, 3},
		ShardsRebuilt:    []int32{1, 4},
		ParityUsed:       2,
		PeersInvolved:    []string{"node-a", "node-b"},
		DurationMs:       42,
		EventIDs:         []string{"e1", "e2"},
		CorrelationID:    "cid-x",
		CanonicalPayload: `{"a":"b"}`,
		Signature:        "deadbeef",
	}
	data, err := EncodeReceipt(r)
	require.NoError(t, err)
	got, err := DecodeReceiptStorage(data)
	require.NoError(t, err)
	require.Equal(t, r, got, "all 13 fields should round-trip")
}

func TestReceipt_DecodeRejectsLegacyJSON(t *testing.T) {
	legacy := []byte(`{"receipt_id":"rcpt-1"}`)
	_, err := DecodeReceiptStorage(legacy)
	require.True(t, errors.Is(err, ErrLegacyStorageFormat))
}
