package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestDistributedBackendApplyErrorExportedCleanupOnRead(t *testing.T) {
	b := &DistributedBackend{} // minimal — applyErrs는 nil 시작, recordApplyResult가 lazy init
	b.recordApplyResult(42, errors.New("test-err"))

	err := b.ApplyError(42)
	if err == nil || err.Error() != "test-err" {
		t.Fatalf("first read: got %v", err)
	}
	if e := b.ApplyError(42); e != nil {
		t.Fatalf("second read: got %v, want nil (cleanup)", e)
	}
}

func TestDistributedBackendRecordApplyResultIgnoresNil(t *testing.T) {
	b := &DistributedBackend{}
	b.recordApplyResult(1, nil) // should be no-op
	if e := b.ApplyError(1); e != nil {
		t.Fatalf("nil should not be stored: %v", e)
	}
}

func TestDistributedBackendRecordApplyResult1024Cleanup(t *testing.T) {
	b := &DistributedBackend{}
	b.recordApplyResult(1, errors.New("old"))
	b.recordApplyResult(2000, errors.New("new"))
	if e := b.ApplyError(1); e != nil {
		t.Fatalf("old should be cleaned, got %v", e)
	}
	if e := b.ApplyError(2000); e == nil || e.Error() != "new" {
		t.Fatalf("new should remain: %v", e)
	}
}

// Red 11c: apply loop ordering — recordApplyResult must be set BEFORE
// lastApplied.Store so the propose loop, on observing lastApplied >= idx,
// always sees the apply error (race-free). This test simulates the apply
// loop's side effects on the backend in the documented order and then
// verifies the read-side observation.
func TestDistributedBackendProposeReceivesApplyError(t *testing.T) {
	b := &DistributedBackend{}
	sentinel := errors.New("test-sentinel")

	// Simulate apply loop ordering: recordApplyResult first, then lastApplied.Store.
	const idx uint64 = 42
	b.recordApplyResult(idx, sentinel)
	b.lastApplied.Store(50) // any value >= idx

	// propose's leader path polls until lastApplied.Load() >= idx then reads
	// ApplyError(idx). Verify that read consumes the sentinel.
	if !(b.lastApplied.Load() >= idx) {
		t.Fatalf("lastApplied should be >= %d", idx)
	}
	got := b.ApplyError(idx)
	if !errors.Is(got, sentinel) {
		t.Fatalf("expected propagated apply error, got %v", got)
	}
}

// Red 11d: encode/decode round-trip for known FSM apply sentinels.
// Each sentinel must be preserved verbatim across the wire so the follower
// can errors.Is() the canonical error.
func TestApplyErrorCodecRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code applyErrorCode
	}{
		{"none", nil, applyErrCodeNone},
		{"offset_mismatch", storage.ErrAppendOffsetMismatch, applyErrCodeOffsetMismatch},
		{"cap_exceeded", storage.ErrAppendCapExceeded, applyErrCodeCapExceeded},
		{"not_supported", storage.ErrAppendNotSupported, applyErrCodeNotSupported},
		{"bucket_not_found", storage.ErrBucketNotFound, applyErrCodeBucketNotFound},
		{"object_not_found", storage.ErrObjectNotFound, applyErrCodeObjectNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, msg := encodeApplyError(c.err)
			if code != c.code {
				t.Fatalf("encode code=%d want %d", code, c.code)
			}
			decoded := decodeApplyError(code, msg)
			if c.err == nil {
				if decoded != nil {
					t.Fatalf("decoded nil want nil, got %v", decoded)
				}
				return
			}
			if !errors.Is(decoded, c.err) {
				t.Fatalf("decoded %v want sentinel %v", decoded, c.err)
			}
		})
	}
}

// Wrapped sentinels must still encode to the canonical code so that callers
// who fmt.Errorf("...%w", ErrAppendCapExceeded) survive the forward hop.
func TestApplyErrorCodecWrappedSentinel(t *testing.T) {
	wrapped := fmt.Errorf("apply failed: %w", storage.ErrAppendOffsetMismatch)
	code, msg := encodeApplyError(wrapped)
	if code != applyErrCodeOffsetMismatch {
		t.Fatalf("wrapped sentinel code=%d want %d", code, applyErrCodeOffsetMismatch)
	}
	if msg == "" {
		t.Fatal("msg must carry the wrapped error string")
	}
	decoded := decodeApplyError(code, msg)
	if !errors.Is(decoded, storage.ErrAppendOffsetMismatch) {
		t.Fatalf("decoded %v want sentinel", decoded)
	}
}

// Unknown / future codes must not panic — they fall through to a generic
// error carrying the transported message.
func TestApplyErrorCodecUnknownCode(t *testing.T) {
	err := decodeApplyError(applyErrorCode(200), "unknown apply error")
	if err == nil {
		t.Fatal("expected non-nil error for unknown code")
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error string")
	}
}

// Internal code (catch-all) must transport the original message verbatim
// so operators can correlate logs across the forward hop.
func TestApplyErrorCodecInternalPreservesMsg(t *testing.T) {
	orig := errors.New("something specific went wrong")
	code, msg := encodeApplyError(orig)
	if code != applyErrCodeInternal {
		t.Fatalf("non-sentinel err encoded as code=%d, want %d", code, applyErrCodeInternal)
	}
	decoded := decodeApplyError(code, msg)
	if decoded.Error() != orig.Error() {
		t.Fatalf("internal-code msg lost: got %q want %q", decoded.Error(), orig.Error())
	}
}

// Wire-format round-trip: encodeProposeForwardReply ↔ decodeProposeForwardReply
// over the Phase A wire (with trailing apply-error-code byte).
func TestProposeForwardReplyWireRoundTrip(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		payload := encodeProposeForwardReply(42, nil)
		idx, applyErr, tErr := decodeProposeForwardReply(payload)
		if tErr != nil {
			t.Fatalf("transport err: %v", tErr)
		}
		if idx != 42 {
			t.Fatalf("idx=%d want 42", idx)
		}
		if applyErr != nil {
			t.Fatalf("applyErr=%v want nil", applyErr)
		}
	})
	t.Run("apply_error_sentinel", func(t *testing.T) {
		payload := encodeProposeForwardReply(0, storage.ErrAppendCapExceeded)
		idx, applyErr, tErr := decodeProposeForwardReply(payload)
		if tErr != nil {
			t.Fatalf("transport err: %v", tErr)
		}
		if idx != 0 {
			t.Fatalf("idx=%d want 0", idx)
		}
		if !errors.Is(applyErr, storage.ErrAppendCapExceeded) {
			t.Fatalf("applyErr=%v want ErrAppendCapExceeded", applyErr)
		}
	})
	t.Run("legacy_wire_no_code_byte", func(t *testing.T) {
		// Simulate an old leader: [8B idx=0][4B errLen][errBytes] (no trailing code).
		msg := "leader: some old error"
		errBytes := []byte(msg)
		legacy := make([]byte, 12+len(errBytes))
		binary.BigEndian.PutUint32(legacy[8:12], uint32(len(errBytes)))
		copy(legacy[12:], errBytes)
		_, applyErr, tErr := decodeProposeForwardReply(legacy)
		if tErr != nil {
			t.Fatalf("transport err: %v", tErr)
		}
		if applyErr == nil || applyErr.Error() != msg {
			t.Fatalf("legacy wire applyErr=%v want plain error %q", applyErr, msg)
		}
	})
	t.Run("short_payload", func(t *testing.T) {
		_, _, tErr := decodeProposeForwardReply([]byte{1, 2, 3})
		if tErr == nil {
			t.Fatal("expected transport error for short payload")
		}
	})
}
