package snapshot

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// Snapshot is the on-disk representation of a metadata snapshot.
type Snapshot struct {
	Seq         uint64                   `json:"seq"`
	Timestamp   time.Time                `json:"timestamp"`
	WALOffset   uint64                   `json:"wal_offset"`
	Reason      string                   `json:"reason,omitempty"`
	ObjectCount int                      `json:"object_count"`
	SizeBytes   int64                    `json:"size_bytes"`
	Buckets     []string                 `json:"buckets"`
	Objects     []storage.SnapshotObject `json:"objects"`
	// BucketMeta is populated when the backend implements storage.BucketSnapshotable.
	// Older snapshots omit this field; Restore treats nil as a no-op for bucket state.
	BucketMeta []storage.SnapshotBucket `json:"bucket_meta,omitempty"`
}

var ErrUnsupportedSnapshotFormat = errors.New("unsupported snapshot format")

var snapshotMagic = [8]byte{'G', 'F', 'S', 'N', 'A', 'P', '0', '1'}

const (
	currentSnapshotWriterFormat uint32 = 1
	currentSnapshotReaderFormat uint32 = 1
	snapshotHeaderLen                  = 8 + 4 + 4 + 8
)

func writeSnapshot(path string, snap *Snapshot) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	if _, err := f.Write(snapshotMagic[:]); err != nil {
		f.Close()
		return err
	}
	if err := binary.Write(f, binary.BigEndian, currentSnapshotReaderFormat); err != nil {
		f.Close()
		return err
	}
	if err := binary.Write(f, binary.BigEndian, currentSnapshotWriterFormat); err != nil {
		f.Close()
		return err
	}
	if err := binary.Write(f, binary.BigEndian, time.Now().UnixNano()); err != nil {
		f.Close()
		return err
	}

	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)
	enc.SetIndent("", "")
	if err := enc.Encode(snap); err != nil {
		gz.Close()
		f.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func readSnapshot(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readSnapshotFromReader(f)
}

func readSnapshotFromReader(r io.Reader) (*Snapshot, error) {
	br := bufio.NewReader(r)

	prefix, err := br.Peek(2)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(prefix, []byte{0x1f, 0x8b}) {
		return decodeSnapshotGzip(br)
	}

	header, err := br.Peek(snapshotHeaderLen)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(header[:8], snapshotMagic[:]) {
		return nil, fmt.Errorf("%w: unknown snapshot envelope", ErrUnsupportedSnapshotFormat)
	}

	minReader := binary.BigEndian.Uint32(header[8:12])
	if minReader > currentSnapshotReaderFormat {
		return nil, fmt.Errorf("%w: min reader format %d exceeds current reader format %d", ErrUnsupportedSnapshotFormat, minReader, currentSnapshotReaderFormat)
	}

	if _, err := br.Discard(snapshotHeaderLen); err != nil {
		return nil, err
	}
	return decodeSnapshotGzip(br)
}

func decodeSnapshotGzip(r io.Reader) (*Snapshot, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
