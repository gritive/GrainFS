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
	}
	if _, err := os.Stat(path); err != nil {
	}
	return path
}
