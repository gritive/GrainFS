package server

import "testing"

// TestRangeRequestFallback documents the design decision:
// Range requests use the standard (non-zero-copy) path even for large files,
// because sendfile(2) cannot transfer partial ranges.
// Actual range request behavior is tested in sendfile_e2e_require_test.go.
func TestRangeRequestFallback(t *testing.T) {
	t.Skip("design decision documented; behavior tested in TestE2EZeroCopyRangeRequest")
}
