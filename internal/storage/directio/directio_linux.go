//go:build linux

package directio

import (
	"os"
	"syscall"
	"unsafe"
)

const pageSize = 4096

// openDirect adds O_DIRECT to flag. The filesystem must support it — some
// overlayfs and tmpfs configurations return EINVAL, in which case the caller
// should fall back to the standard write path.
func openDirect(path string, flag int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_DIRECT, mode)
}

// alignedCopyImpl returns a 4096-aligned buffer of the smallest length that
// is a multiple of 4096 and at least len(data) bytes, with data copied into
// the head and zeros filling the tail.
//
// We over-allocate by pageSize so we can find a page-aligned offset in the
// caller's address space. Go's runtime gives no public way to request an
// aligned allocation, so this offset trick is the standard idiom.
func alignedCopyImpl(data []byte) ([]byte, int) {
	want := (len(data) + pageSize - 1) &^ (pageSize - 1)
	if want == 0 {
		want = pageSize
	}
	raw := make([]byte, want+pageSize)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	off := int((pageSize - int(addr%uintptr(pageSize))) % pageSize)
	buf := raw[off : off+want]
	copy(buf, data)
	// Tail of buf past len(data) is already zero from make().
	return buf, want
}
