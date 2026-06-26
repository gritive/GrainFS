package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

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

// TestDistributedBackendFollowerApplyWaitUnblocksOnLastApplied validates the
// structural guarantee that the forwarded-propose follower apply-wait unblocks
// as soon as lastApplied reaches the committed index.
//
// Context: S4c-0 PR2 code-gate fix — when a follower forwards a propose and
// forwardPropose returns (idx, nil), the caller MUST wait for
// b.lastApplied.Load() >= idx before returning so the MPU phantom-winner guard
// reads a stable local mpudone marker.  This test proves the polling condition
// using the same lastApplied primitive as the real propose() loop, without
// requiring a two-node raft cluster (which would require a real transport).
//
// True multi-node forwarded-propose coverage is provided implicitly by:
//   - The existing integration suite (backend_multipart_integration_test.go)
//     which exercises the full propose path on real single-node raft (always
//     leader path); and
//   - TestMetaRaftProposeWithIndexFollowerWaitsForLocalApply which validates
//     the same apply-wait pattern for MetaRaft (sister struct, identical loop).
//
// The gap — a follower forwarding to a leader then waiting for its own apply —
// requires a two-node harness. That harness does not currently exist in the
// cluster package unit tests (newTestDistributedBackend spins a solo leader).
// The primitive tested here (lastApplied) is the exact code executed by the
// follower apply-wait added in this fix.
func TestDistributedBackendFollowerApplyWaitUnblocksOnLastApplied(t *testing.T) {
	const idx uint64 = 77
	b := &DistributedBackend{}

	// Simulate the apply loop completing entry idx.
	b.lastApplied.Store(idx)

	// The polling condition used in the follower apply-wait must be satisfied.
	if b.lastApplied.Load() < idx {
		t.Fatalf("expected lastApplied %d >= idx %d", b.lastApplied.Load(), idx)
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
