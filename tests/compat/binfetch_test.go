//go:build compat

package compat

import (
	"os"
	"testing"
)

// prevBinary returns the path to the previous-version grainfs binary.
// It requires COMPAT_PREV_BIN to be set; otherwise it calls t.Skip.
func prevBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("COMPAT_PREV_BIN")
	if path == "" {
		t.Skip("COMPAT_PREV_BIN not set — previous-binary tests skipped")
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("COMPAT_PREV_BIN=%s not found: %v", path, err)
	}
	return path
}
