package storage

import (
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"

	"crypto/md5"
)

// Composite-ETag utilities for S3 Express AppendObject semantics. Extracted from
// append.go ahead of the LocalBackend removal: production cluster code
// (internal/cluster/appendable_object.go) depends on the exported functions
// below, so they must outlive the LocalBackend test fixture. The unexported
// helpers are shared with the remaining LocalBackend append glue (same package).

// CompositeETag returns the S3-multipart-style composite ETag:
// md5(concat(callMD5s)) + "-<N>".
//
// callMD5s is one raw 16-byte MD5 digest per AppendObject call (S3
// AppendObject semantics). nil/empty input is allowed: it produces the
// stable "<md5 of empty>-0" placeholder used by Task 3.1 wire-up until
// per-call MD5s are captured.
func CompositeETag(callMD5s [][]byte) string {
	h := md5.New()
	for _, d := range callMD5s {
		h.Write(d)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(callMD5s))
}

func appendETagStateAppend(state []byte, count int, digest []byte) ([]byte, int, error) {
	h := md5.New()
	if len(state) > 0 {
		unmarshaler, ok := h.(encoding.BinaryUnmarshaler)
		if !ok {
			return nil, 0, errors.New("append etag state: md5 does not support binary unmarshal")
		}
		if err := unmarshaler.UnmarshalBinary(state); err != nil {
			return nil, 0, fmt.Errorf("append etag state: unmarshal: %w", err)
		}
	}
	if _, err := h.Write(digest); err != nil {
		return nil, 0, fmt.Errorf("append etag state: write digest: %w", err)
	}
	marshaler, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, 0, errors.New("append etag state: md5 does not support binary marshal")
	}
	next, err := marshaler.MarshalBinary()
	if err != nil {
		return nil, 0, fmt.Errorf("append etag state: marshal: %w", err)
	}
	return next, count + 1, nil
}

func AppendETagStateAppend(state []byte, count int, digest []byte) ([]byte, int, error) {
	return appendETagStateAppend(state, count, digest)
}

func compositeETagFromState(state []byte, count int) (string, error) {
	h := md5.New()
	if len(state) > 0 {
		unmarshaler, ok := h.(encoding.BinaryUnmarshaler)
		if !ok {
			return "", errors.New("append etag state: md5 does not support binary unmarshal")
		}
		if err := unmarshaler.UnmarshalBinary(state); err != nil {
			return "", fmt.Errorf("append etag state: unmarshal: %w", err)
		}
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), count), nil
}

func CompositeETagFromState(state []byte, count int) (string, error) {
	return compositeETagFromState(state, count)
}

func appendETagStateFromDigests(digests [][]byte) ([]byte, int, error) {
	var (
		state []byte
		count int
		err   error
	)
	for _, digest := range digests {
		state, count, err = appendETagStateAppend(state, count, digest)
		if err != nil {
			return nil, 0, err
		}
	}
	return state, count, nil
}

func AppendETagStateFromDigests(digests [][]byte) ([]byte, int, error) {
	return appendETagStateFromDigests(digests)
}
