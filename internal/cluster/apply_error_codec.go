package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/storage"
)

// applyErrorCode is a stable, cross-version-safe sentinel identifier used to
// transport FSM apply errors from leader to follower across the propose-forward
// wire. New codes are appended; existing codes never change meaning.
//
// Wire layout (extends the legacy propose-forward reply):
//
//	[8B index BE][4B errLen BE][errBytes...][1B applyErrorCode]
//
// Old nodes that send only [8B][4B][errBytes...] are decoded as
// applyErrCodeNone — callers fall back to the human-readable err message
// already encoded in errBytes.
type applyErrorCode uint8

const (
	applyErrCodeNone           applyErrorCode = 0
	applyErrCodeOffsetMismatch applyErrorCode = 1
	applyErrCodeCapExceeded    applyErrorCode = 2
	applyErrCodeStalePlacement applyErrorCode = 3 // reserved (Task 17)
	applyErrCodeNotSupported   applyErrorCode = 4
	applyErrCodeBucketNotFound applyErrorCode = 5
	applyErrCodeObjectNotFound applyErrorCode = 6
	applyErrCodeInternal       applyErrorCode = 99
)

// encodeApplyError maps a sentinel error to a code + message for cross-node
// transport. Unknown errors → CodeInternal with msg = err.Error().
func encodeApplyError(err error) (applyErrorCode, string) {
	if err == nil {
		return applyErrCodeNone, ""
	}
	switch {
	case errors.Is(err, storage.ErrAppendOffsetMismatch):
		return applyErrCodeOffsetMismatch, err.Error()
	case errors.Is(err, storage.ErrAppendCapExceeded):
		return applyErrCodeCapExceeded, err.Error()
	case errors.Is(err, storage.ErrAppendNotSupported):
		return applyErrCodeNotSupported, err.Error()
	case errors.Is(err, storage.ErrBucketNotFound):
		return applyErrCodeBucketNotFound, err.Error()
	case errors.Is(err, storage.ErrObjectNotFound):
		return applyErrCodeObjectNotFound, err.Error()
	// applyErrCodeStalePlacement: cluster-level sentinel is defined in
	// Task 17 (ErrStalePlacement). Until then, such errors fall through
	// to applyErrCodeInternal with the human-readable message.
	default:
		return applyErrCodeInternal, err.Error()
	}
}

// decodeApplyError reconstructs a sentinel error from a code + message.
// Cross-version safe: unknown codes fall through to a generic error built
// from the transported message so callers can still log/surface them.
func decodeApplyError(code applyErrorCode, msg string) error {
	switch code {
	case applyErrCodeNone:
		return nil
	case applyErrCodeOffsetMismatch:
		return storage.ErrAppendOffsetMismatch
	case applyErrCodeCapExceeded:
		return storage.ErrAppendCapExceeded
	case applyErrCodeNotSupported:
		return storage.ErrAppendNotSupported
	case applyErrCodeBucketNotFound:
		return storage.ErrBucketNotFound
	case applyErrCodeObjectNotFound:
		return storage.ErrObjectNotFound
	// applyErrCodeStalePlacement: reserved — wire it up in Task 17 once
	// ErrStalePlacement is defined in the cluster package.
	case applyErrCodeInternal:
		if msg == "" {
			return errors.New("apply error: internal")
		}
		return errors.New(msg)
	default:
		return fmt.Errorf("apply error (code=%d): %s", code, msg)
	}
}

// --- propose-forward wire (Phase A, Task 16) -------------------------------
//
// Legacy wire (Task <16):  [8B index BE][4B errLen BE][errBytes...]
// Phase A wire (Task 16+): [8B index BE][4B errLen BE][errBytes...][1B applyErrorCode]
//
// The trailing apply-error-code byte is optional. Old leaders omit it; new
// followers tolerate the omission and fall back to constructing the error from
// errBytes (matches legacy behavior). New leaders always emit it.
//
// raft.ErrNotLeader is *not* an apply error — it is a propose-time error and is
// transported verbatim in errBytes with applyErrorCode == None. The follower's
// caller already special-cases that string to convert back to raft.ErrNotLeader.

// encodeProposeForwardReply builds the leader→follower reply for the
// propose-forward wire. If err is nil, index is the committed log index;
// otherwise index is 0 and err is encoded both as the legacy err string and as
// a stable apply-error code (when the error is an FSM apply sentinel).
func encodeProposeForwardReply(index uint64, err error) []byte {
	if err == nil {
		resp := make([]byte, 13)
		binary.BigEndian.PutUint64(resp[0:8], index)
		// errLen = 0, applyErrorCode = None — both already zero.
		return resp
	}
	errBytes := []byte(err.Error())
	code, _ := encodeApplyError(err)
	resp := make([]byte, 12+len(errBytes)+1)
	// index = 0 (already zeroed)
	binary.BigEndian.PutUint32(resp[8:12], uint32(len(errBytes)))
	copy(resp[12:], errBytes)
	resp[12+len(errBytes)] = byte(code)
	return resp
}

// decodeProposeForwardReply parses the leader→follower reply. Returns
// (index, applyError, transportError). transportError is non-nil only for
// truncated payloads. applyError carries the FSM apply error reconstructed via
// decodeApplyError, or — on legacy wire without an apply code — a generic
// error built from the err bytes.
//
// Backward-compat: payloads of exactly 12+errLen bytes (no trailing code) are
// accepted and treated as applyErrCodeNone, falling through to errors.New(msg).
func decodeProposeForwardReply(payload []byte) (uint64, error, error) {
	if len(payload) < 12 {
		return 0, nil, fmt.Errorf("propose-forward: short payload: %d bytes", len(payload))
	}
	index := binary.BigEndian.Uint64(payload[0:8])
	errLen := binary.BigEndian.Uint32(payload[8:12])
	if errLen == 0 {
		// Success path. New wire has 1 trailing byte (=0), legacy has none.
		return index, nil, nil
	}
	if len(payload) < 12+int(errLen) {
		return 0, nil, fmt.Errorf("propose-forward: errLen=%d but payload=%d", errLen, len(payload))
	}
	msg := string(payload[12 : 12+int(errLen)])
	// Optional trailing apply-error-code byte (Phase A wire).
	var code applyErrorCode
	if len(payload) >= 12+int(errLen)+1 {
		code = applyErrorCode(payload[12+int(errLen)])
	}
	if code != applyErrCodeNone {
		return index, decodeApplyError(code, msg), nil
	}
	// Legacy or non-apply error — surface msg as a plain error.
	return index, errors.New(msg), nil
}
