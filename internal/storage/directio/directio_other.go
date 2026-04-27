//go:build !linux && !darwin

package directio

import "os"

// pageSize 1 means AlignedCopy is a no-op pass-through on platforms that
// don't have a direct-I/O hint we recognize. The caller still gets a
// working write path; it just goes through the OS page cache like any
// other os.OpenFile write.
const pageSize = 1

// openDirect delegates to os.OpenFile. The package contract says reads,
// writes, sync, close all behave normally — we honor that by simply not
// adding any flag the platform wouldn't understand.
func openDirect(path string, flag int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, mode)
}

// alignedCopyImpl is a pass-through on platforms without alignment needs.
func alignedCopyImpl(data []byte) ([]byte, int) {
	return data, len(data)
}
