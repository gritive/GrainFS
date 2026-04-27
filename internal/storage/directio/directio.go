// Package directio gives callers a thin, platform-aware wrapper for opening
// files with direct I/O hints. The kernel page cache is bypassed so a write
// goes straight to the disk, which is a meaningful win for the EC shard hot
// path (1-4 MB writes, see internal/cluster/shardio_directio_bench_test.go
// for the measurement that justifies this package).
//
// Platform behavior:
//
//	Linux:  O_DIRECT flag is OR'd into the open flags. Writes MUST originate
//	        from a 4096-byte aligned buffer whose length is a multiple of
//	        4096. Use AlignedCopy to satisfy both constraints in one shot,
//	        then ftruncate the file to the original payload size after the
//	        write. PageSize() returns 4096.
//	macOS:  The file is opened normally, then F_NOCACHE is applied via fcntl.
//	        No buffer alignment or size constraints — write the payload as-is.
//	        PageSize() returns 1 (no alignment).
//	Other:  No-op fallback. OpenFile delegates to os.OpenFile, AlignedCopy
//	        returns the input slice unchanged, PageSize returns 1.
//
// Direct I/O does NOT imply durability. Callers must still call f.Sync()
// before close to flush disk firmware caches, exactly the same as the
// non-direct path. The atomic-write recipe (tmp + sync + rename + parent
// sync) is unchanged.
package directio

import "os"

// OpenFile opens path with direct-I/O hints applied. Semantically equivalent
// to os.OpenFile from the caller's perspective once the file is returned —
// reads, writes, sync, close all behave normally. The only constraint is
// that on Linux the buffer passed to Write MUST satisfy the alignment rules
// described in the package doc.
//
// Returns the same errors as os.OpenFile, plus any platform-specific failure
// from applying the direct-I/O hint (e.g. F_NOCACHE fcntl on macOS, or
// EINVAL on Linux when O_DIRECT is unsupported by the underlying filesystem
// such as some overlayfs or tmpfs configurations).
func OpenFile(path string, flag int, mode os.FileMode) (*os.File, error) {
	return openDirect(path, flag, mode)
}

// PageSize returns the buffer alignment unit required by Write on this
// platform. Linux returns 4096; macOS and other platforms return 1.
//
// AlignedCopy uses this to size its output buffer; callers usually do not
// need to call PageSize directly.
func PageSize() int { return pageSize }

// AlignedCopy returns a buffer suitable for a direct-I/O write of data on
// this platform.
//
// Linux: returns (alignedBuf, padToWrite). alignedBuf is a 4096-aligned
//
//	slice whose first len(data) bytes are a copy of data and whose length
//	is rounded up to the next 4096 multiple. padToWrite is the rounded-up
//	length, which is what should be passed to f.Write. After Sync, call
//	f.Truncate(int64(len(data))) to drop the trailing zero pad.
//
// macOS / fallback: returns (data, len(data)). No copy. No truncate needed.
//
// In both cases, hand alignedBuf to f.Write and use len(data) as the final
// on-disk size when truncating.
func AlignedCopy(data []byte) (aligned []byte, alignedLen int) {
	return alignedCopyImpl(data)
}
