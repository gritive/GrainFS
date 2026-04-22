// Package eccodec provides reusable shard I/O primitives shared by
// every erasure-coded backend. The on-disk format is a payload followed
// by a 4-byte little-endian CRC32-IEEE footer.
//
// Slice 2 (refactor/unify-storage-paths): eccodec is introduced so Slice 8
// can drop internal/erasure/ while keeping the footer layout consistent. The
// current cluster ShardService writes raw bytes without a footer, so CRC
// verification is wired in through Scrubbable only when upstream switches
// to the eccodec-backed path.
package eccodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// ErrCRCMismatch is returned when the CRC footer does not match the payload,
// or when a shard is shorter than the 4-byte footer.
var ErrCRCMismatch = errors.New("eccodec: CRC mismatch (bit-rot detected)")

// footerLen is the size of the CRC32 footer appended to every shard.
const footerLen = 4

// encodeWithCRC appends a 4-byte little-endian CRC32-IEEE footer to data.
func encodeWithCRC(data []byte) []byte {
	out := make([]byte, len(data)+footerLen)
	copy(out, data)
	binary.LittleEndian.PutUint32(out[len(data):], crc32.ChecksumIEEE(data))
	return out
}

// stripVerifyCRC verifies the footer and returns the payload slice.
// Returns ErrCRCMismatch if the shard is truncated or the checksum is wrong.
func stripVerifyCRC(data []byte) ([]byte, error) {
	if len(data) < footerLen {
		return nil, fmt.Errorf("%w: shard too short (%d bytes)", ErrCRCMismatch, len(data))
	}
	payload := data[:len(data)-footerLen]
	stored := binary.LittleEndian.Uint32(data[len(data)-footerLen:])
	if crc32.ChecksumIEEE(payload) != stored {
		return nil, ErrCRCMismatch
	}
	return payload, nil
}

// WriteShardAtomic writes data (with CRC32 footer) to path using the
// write-tmp → fsync → rename → fsync-parent pattern so a crash mid-write
// never leaves a torn shard at the destination.
func WriteShardAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir shard dir: %w", err)
	}
	payload := encodeWithCRC(data)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp shard: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync tmp shard: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

// ReadShardVerified reads a shard from disk and returns its payload after
// verifying the CRC32 footer. Returns ErrCRCMismatch on corruption.
func ReadShardVerified(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return stripVerifyCRC(raw)
}
