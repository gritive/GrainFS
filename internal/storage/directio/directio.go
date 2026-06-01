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

import (
	"os"
	"sync/atomic"
	"syscall"
)

// SyncMode selects the data-plane fsync policy used by Sync. It governs the
// single live data-plane durability fsync (shard finalize when no WAL is
// wired, otherwise the datawal flush — the two are wiring-exclusive), so one
// knob controls whichever is active. It does NOT touch the raft/consensus
// fsync (raft state is not reconstructable from erasure coding, and its sync
// is off the PUT hot path).
type SyncMode int32

const (
	// SyncFull is full power-loss durability: os.File.Sync, which is
	// F_FULLFSYNC on darwin (a full drive-cache→platter barrier, ~10-20ms,
	// surviving power loss). The safe default.
	SyncFull SyncMode = iota
	// SyncFast issues a plain fsync(2): far cheaper but on darwin it only
	// reaches the drive write cache (data lost on power loss). On Linux
	// os.File.Sync and fsync(2) are the same call, so SyncFast == SyncFull
	// there.
	SyncFast
	// SyncOff skips the fsync entirely. Data reaches the page cache only;
	// durability is delegated to cross-node erasure-coding redundancy
	// (reconstruct a lost node's shards from peers). This is UNSAFE for
	// single-node and for correlated power loss across a cluster (EC tolerates
	// node loss, not all nodes losing un-synced page cache at once). Opt-in
	// only, with a loud startup warning.
	SyncOff
)

// fsyncMode holds the current SyncMode. Default SyncFull.
var fsyncMode atomic.Int32

func init() {
	switch os.Getenv("GRAINFS_FSYNC_MODE") {
	case "fast":
		fsyncMode.Store(int32(SyncFast))
	case "off":
		fsyncMode.Store(int32(SyncOff))
	}
	// Back-compat: GRAINFS_FSYNC_FAST=1 is equivalent to GRAINFS_FSYNC_MODE=fast.
	if os.Getenv("GRAINFS_FSYNC_FAST") == "1" {
		fsyncMode.Store(int32(SyncFast))
	}
}

// SetSyncMode overrides the fsync policy at runtime (e.g. wired from a serve
// flag). Default (SyncFull) keeps full power-loss durability.
func SetSyncMode(m SyncMode) { fsyncMode.Store(int32(m)) }

// CurrentSyncMode reports the active fsync policy (for logging/metrics).
func CurrentSyncMode() SyncMode { return SyncMode(fsyncMode.Load()) }

// FastFsyncEnabled reports whether the current mode skips the F_FULLFSYNC
// barrier (true for SyncFast and SyncOff). Retained for datawal.syncWAL, which
// branches on it before applying its own mode handling.
func FastFsyncEnabled() bool { return CurrentSyncMode() != SyncFull }

// Sync flushes f to stable storage per the active SyncMode. Shard / WAL
// durability fsyncs route through here so the policy lives in one place.
func Sync(f *os.File) error {
	switch CurrentSyncMode() {
	case SyncOff:
		return nil
	case SyncFast:
		return syscall.Fsync(int(f.Fd()))
	default:
		return f.Sync()
	}
}

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

// ApplyNoCacheHint asks the kernel to bypass the page cache for the given
// open file, without touching the open flags. On macOS this issues
// fcntl(F_NOCACHE), giving the same page-cache-bypass effect as
// directio.OpenFile but without imposing the alignment rules that O_DIRECT
// would on Linux. Useful for write paths that emit many small unaligned
// chunks (the encrypted EC shard chunked writer is the motivating case —
// O_DIRECT would EINVAL on its 1 MiB-plus-tag writes, but F_NOCACHE works).
//
// On Linux and unsupported platforms this is a no-op that returns nil; the
// file goes through the page cache as usual. Callers wanting Linux O_DIRECT
// must use OpenFile + AlignedCopy instead.
func ApplyNoCacheHint(f *os.File) error {
	return applyNoCacheHint(f)
}

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
