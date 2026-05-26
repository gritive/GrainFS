package cluster

import (
	"bytes"
	"strings"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

func TestEncodeDecodeMetaKEKRotateCmd_RoundTrip(t *testing.T) {
	cmd := KEKRotateCmd{
		PayloadVersion: 1,
		NewVersion:     5,
		WrappedNewKEK:  bytes.Repeat([]byte{0xAA}, 60),
		WrapSetHash:    bytes.Repeat([]byte{0xBB}, 32),
		RewrappedDEKs: []RewrappedDEKEntry{
			{Gen: 1, Wrapped: bytes.Repeat([]byte{0x01}, 64)},
			{Gen: 2, Wrapped: bytes.Repeat([]byte{0x02}, 64)},
		},
		Confirm:              "rotate-now",
		Actor:                "admin@uds",
		RequestID:            [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00},
		RequestedAtUnixNanos: 1717075200000000000,
		ClusterStateAtPropose: ClusterStateAtPropose{
			ActiveKEKVersion: 4,
			RetainedKEKCount: 3,
			LiveDEKGenCount:  2,
		},
	}
	enc, err := EncodeMetaKEKRotateCmd(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMetaKEKRotateCmd(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.NewVersion != cmd.NewVersion {
		t.Errorf("NewVersion = %d, want %d", got.NewVersion, cmd.NewVersion)
	}
	if !bytes.Equal(got.WrappedNewKEK, cmd.WrappedNewKEK) {
		t.Errorf("WrappedNewKEK mismatch")
	}
	if !bytes.Equal(got.WrapSetHash, cmd.WrapSetHash) {
		t.Errorf("WrapSetHash mismatch")
	}
	if len(got.RewrappedDEKs) != 2 {
		t.Errorf("rewrapped count = %d, want 2", len(got.RewrappedDEKs))
	}
	if got.RewrappedDEKs[0].Gen != 1 || !bytes.Equal(got.RewrappedDEKs[0].Wrapped, cmd.RewrappedDEKs[0].Wrapped) {
		t.Errorf("RewrappedDEKs[0] mismatch")
	}
	if got.Confirm != "rotate-now" {
		t.Errorf("Confirm = %q", got.Confirm)
	}
	if got.Actor != "admin@uds" {
		t.Errorf("Actor = %q", got.Actor)
	}
	if got.RequestID != cmd.RequestID {
		t.Errorf("RequestID mismatch")
	}
	if got.RequestedAtUnixNanos != cmd.RequestedAtUnixNanos {
		t.Errorf("RequestedAtUnixNanos mismatch")
	}
	if got.ClusterStateAtPropose.ActiveKEKVersion != 4 || got.ClusterStateAtPropose.RetainedKEKCount != 3 || got.ClusterStateAtPropose.LiveDEKGenCount != 2 {
		t.Errorf("ClusterStateAtPropose mismatch: %+v", got.ClusterStateAtPropose)
	}
	if got.PayloadVersion != 1 {
		t.Errorf("PayloadVersion = %d", got.PayloadVersion)
	}
}

func TestDecodeMetaKEKRotateCmd_TruncatedReject(t *testing.T) {
	if _, err := DecodeMetaKEKRotateCmd([]byte{0x00, 0x00, 0x00}); err == nil {
		t.Errorf("expected error on truncated payload")
	}
	if _, err := DecodeMetaKEKRotateCmd(nil); err == nil {
		t.Errorf("expected error on nil payload")
	}
}

func TestDecodeMetaKEKRotateCmd_PayloadVersionMismatch(t *testing.T) {
	cmd := KEKRotateCmd{
		PayloadVersion: 99,
		NewVersion:     5,
		WrappedNewKEK:  []byte("x"),
		WrapSetHash:    bytes.Repeat([]byte{0}, 32),
		Confirm:        "rotate-now",
	}
	// Build manually since EncodeMetaKEKRotateCmd will normalize PayloadVersion=0 → currentKEKRotatePayloadVersion.
	// For this test we need to force payload_version=99 onto the wire.
	enc := mustBuildKEKRotateCmdWithRawPayloadVersion(t, cmd, 99)
	_, err := DecodeMetaKEKRotateCmd(enc)
	if err == nil || !strings.Contains(err.Error(), "payload_version") {
		t.Errorf("expected unsupported payload version error, got: %v", err)
	}
}

func TestDecodeMetaKEKRotateCmd_RequestIDMustBe16Bytes(t *testing.T) {
	// Build cmd with malformed (8-byte) request_id.
	enc := mustBuildKEKRotateCmdWithRawRequestID(t, [...]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
	_, err := DecodeMetaKEKRotateCmd(enc)
	if err == nil || !strings.Contains(err.Error(), "request_id") {
		t.Errorf("expected request_id length error, got: %v", err)
	}
}

func TestEncodeMetaKEKRotateCmd_WrapSetHashWrongLength(t *testing.T) {
	cmd := KEKRotateCmd{
		PayloadVersion: 1,
		NewVersion:     5,
		WrappedNewKEK:  []byte("x"),
		WrapSetHash:    []byte{0x01, 0x02}, // not 32 bytes
		Confirm:        "rotate-now",
	}
	_, err := EncodeMetaKEKRotateCmd(cmd)
	if err == nil {
		t.Errorf("expected wrap_set_hash length error")
	}
}

// mustBuildKEKRotateCmdWithRawPayloadVersion constructs a flatbuffer with the given
// raw payload_version (bypassing EncodeMetaKEKRotateCmd normalization).
func mustBuildKEKRotateCmdWithRawPayloadVersion(t *testing.T, cmd KEKRotateCmd, version uint8) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(256)

	confirmOff := b.CreateString(cmd.Confirm)
	wrappedOff := b.CreateByteVector(cmd.WrappedNewKEK)
	wrapHashOff := b.CreateByteVector(cmd.WrapSetHash)
	requestIDOff := b.CreateByteVector([]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00})

	clusterpb.MetaKEKRotateCmdStart(b)
	clusterpb.MetaKEKRotateCmdAddPayloadVersion(b, version)
	clusterpb.MetaKEKRotateCmdAddNewVersion(b, cmd.NewVersion)
	clusterpb.MetaKEKRotateCmdAddWrappedNewKek(b, wrappedOff)
	clusterpb.MetaKEKRotateCmdAddWrapSetHash(b, wrapHashOff)
	clusterpb.MetaKEKRotateCmdAddConfirm(b, confirmOff)
	clusterpb.MetaKEKRotateCmdAddRequestId(b, requestIDOff)
	root := clusterpb.MetaKEKRotateCmdEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// mustBuildKEKRotateCmdWithRawRequestID constructs a flatbuffer with a malformed
// (wrong-length) request_id for negative-path testing.
func mustBuildKEKRotateCmdWithRawRequestID(t *testing.T, id [8]byte) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(256)

	confirmOff := b.CreateString("rotate-now")
	wrappedOff := b.CreateByteVector([]byte("x"))
	wrapHashOff := b.CreateByteVector(bytes.Repeat([]byte{0}, 32))
	requestIDOff := b.CreateByteVector(id[:])

	clusterpb.MetaKEKRotateCmdStart(b)
	clusterpb.MetaKEKRotateCmdAddPayloadVersion(b, 1)
	clusterpb.MetaKEKRotateCmdAddNewVersion(b, 5)
	clusterpb.MetaKEKRotateCmdAddWrappedNewKek(b, wrappedOff)
	clusterpb.MetaKEKRotateCmdAddWrapSetHash(b, wrapHashOff)
	clusterpb.MetaKEKRotateCmdAddConfirm(b, confirmOff)
	clusterpb.MetaKEKRotateCmdAddRequestId(b, requestIDOff)
	root := clusterpb.MetaKEKRotateCmdEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}
