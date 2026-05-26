package serveruntime

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestCheckKeystoreDiskSpace_OK(t *testing.T) {
	dir := t.TempDir()
	if err := CheckKeystoreDiskSpace(dir, 1024); err != nil {
		t.Errorf("CheckKeystoreDiskSpace: %v", err)
	}
}

func TestCheckKeystoreDiskSpace_BelowThreshold(t *testing.T) {
	dir := t.TempDir()
	// Threshold larger than any reasonable filesystem free space.
	const huge = uint64(1) << 62
	err := CheckKeystoreDiskSpace(dir, huge)
	if err == nil || !errors.Is(err, ErrKeystoreDiskLow) {
		t.Errorf("expected ErrKeystoreDiskLow, got %v", err)
	}
}

func TestCheckKeystoreDiskSpace_NonexistentDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "definitely-not-here")
	if err := CheckKeystoreDiskSpace(dir, 1024); err == nil {
		t.Errorf("expected error on nonexistent dir, got nil")
	}
}

func TestMinKeystoreFreeBytes_NotZero(t *testing.T) {
	if MinKeystoreFreeBytes == 0 {
		t.Errorf("MinKeystoreFreeBytes should not be zero")
	}
}
