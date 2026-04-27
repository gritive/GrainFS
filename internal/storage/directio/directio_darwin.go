//go:build darwin

package directio

import (
	"fmt"
	"os"
	"syscall"
)

// macOS imposes no buffer alignment requirement for F_NOCACHE — the kernel
// just bypasses the unified buffer cache for this fd. PageSize 1 means
// AlignedCopy is a no-op pass-through.
const pageSize = 1

// openDirect opens path normally, then applies F_NOCACHE via fcntl. If the
// fcntl fails, the file is closed and the error returned — we don't silently
// fall back to a buffered fd because the caller asked for direct I/O on
// purpose. Callers that want graceful fallback can catch the error and
// retry with os.OpenFile.
func openDirect(path string, flag int, mode os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, flag, mode)
	if err != nil {
		return nil, err
	}
	if _, err := fcntlNoCache(f.Fd()); err != nil {
		f.Close()
		return nil, fmt.Errorf("apply F_NOCACHE: %w", err)
	}
	return f, nil
}

// fcntlNoCache enables F_NOCACHE on fd. The constant is not exported by
// syscall on darwin, so we use the literal value (48) — see <fcntl.h>.
const fNoCache = 48

func fcntlNoCache(fd uintptr) (int, error) {
	r1, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, fNoCache, 1)
	if errno != 0 {
		return int(r1), errno
	}
	return int(r1), nil
}

// alignedCopyImpl is a pass-through on macOS — F_NOCACHE has no alignment
// requirement, so we hand the caller's slice straight back without copying.
func alignedCopyImpl(data []byte) ([]byte, int) {
	return data, len(data)
}
