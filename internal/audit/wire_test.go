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

func TestEncodeS3BatchRejectsOversizedFields(t *testing.T) {
	_, err := audit.EncodeS3Batch([]audit.S3Event{{
		EventID:   "evt-oversize",
		UserAgent: strings.Repeat("a", 65536),
		Method:    "GET",
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "user_agent")
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
		{name: "policy decision fields (T51')", events: []audit.S3Event{{
			Ts:              1716000000000003,
			EventID:         "evt-policy",
			NodeID:          "node-1",
			RequestID:       "req-policy-1",
			SAID:            "sa-admin",
			Method:          "GET",
			Bucket:          "b",
			Key:             "k",
			Status:          200,
			AuthStatus:      "allow",
			MatchedPolicyID: "policy:readonly",
			MatchedSID:      "AllowGet",
			AuthzLatencyUS:  127,
			ConditionContext: map[string]string{
				"aws:SourceIp": "10.0.0.1",
				"s3:prefix":    "data/",
			},
		}}},
		{name: "policy decision empty condition", events: []audit.S3Event{{
			Ts:              1716000000000004,
			EventID:         "evt-deny",
			NodeID:          "node-1",
			Method:          "PUT",
			Bucket:          "b",
			Status:          403,
			AuthStatus:      "deny",
			MatchedPolicyID: "policy:readonly",
			MatchedSID:      "DenyPut",
			AuthzLatencyUS:  42,
			// ConditionContext nil → round-trips as nil
		}}},
		{name: "extended audit fields", events: []audit.S3Event{{
			Ts:               1716000000000000,
			EventID:          "evt-1",
			NodeID:           "node-1",
			RequestID:        "req-abc-123",
			SAID:             "sa-admin",
			SourceIP:         "127.0.0.1",
			UserAgent:        "aws-sdk-go-v2/test",
			Method:           "PUT",
			Operation:        "CopyObject",
			Bucket:           "dst",
			Key:              "copied.txt",
			Subresource:      "",
			Status:           200,
			AuthStatus:       "allow",
			BytesIn:          0,
			BytesOut:         0,
			LatencyMs:        12,
			ErrClass:         "",
			ErrReason:        "",
			VersionID:        "v2",
			UploadID:         "",
			CopySourceBucket: "src",
			CopySourceKey:    "source.txt",
		}}},
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
