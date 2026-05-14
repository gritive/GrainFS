// internal/audit/wire_test.go
package audit_test

import (
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/stretchr/testify/require"
)

func TestDecodeS3Batch_RejectsBatchExceedingMax(t *testing.T) {
	// Craft a payload whose 4-byte count header claims 65537 events (maxDecodeBatchSize+1).
	// DecodeS3Batch must reject it before allocating event storage.
	var hdr [4]byte
	hdr[0] = 0x01 // 65537 in little-endian: 0x00010001 → bytes [01, 00, 01, 00]
	hdr[1] = 0x00
	hdr[2] = 0x01
	hdr[3] = 0x00
	_, err := audit.DecodeS3Batch(hdr[:])
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")
}

func TestWireRoundtrip(t *testing.T) {
	full := audit.S3Event{
		Ts:        1716000000000000,
		NodeID:    "node-42",
		RequestID: "req-abc-123",
		SAID:      "sa:admin",
		SourceIP:  "10.0.0.1",
		Method:    "PUT",
		Bucket:    "mybucket",
		Key:       "path/to/object",
		Status:    200,
		BytesIn:   4096,
		BytesOut:  0,
		LatencyMs: 12,
		ErrClass:  "",
	}

	tests := []struct {
		name   string
		events []audit.S3Event
	}{
		{name: "empty batch", events: []audit.S3Event{}},
		{name: "single event all fields", events: []audit.S3Event{full}},
		{name: "multi event", events: []audit.S3Event{full, {
			Ts: 1716000000000001, NodeID: "node-7", Method: "GET",
			Bucket: "b", Key: "k", Status: 404, ErrClass: "NoSuchKey",
		}}},
		{name: "method at 255-byte boundary", events: []audit.S3Event{
			{Method: strings.Repeat("X", 255), Bucket: "b", Key: "k", Status: 200},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := audit.EncodeS3Batch(tc.events)
			require.NoError(t, err)

			got, err := audit.DecodeS3Batch(enc)
			require.NoError(t, err)
			require.Equal(t, tc.events, got)
		})
	}
}
