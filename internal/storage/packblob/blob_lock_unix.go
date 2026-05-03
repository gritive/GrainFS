//go:build unix

package packblob

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func acquireBlobDirLock(dir string) (*os.File, error) {
	lockPath := filepath.Join(dir, "blob.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("blob dir already locked by another process: %w", err)
	}
	return f, nil
}

func releaseBlobDirLock(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}
