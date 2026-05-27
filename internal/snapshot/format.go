package snapshot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/klauspost/compress/zstd"
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

// encodeSnapshotFramed encodes snap into the GFSNAP01 framed format (magic +
// versions + timestamp + zstd-JSON) and returns the raw bytes. This is the
// plaintext body that gets sealed by sealSnapshotBlob.
func encodeSnapshotFramed(snap *Snapshot) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(snapshotMagic[:])
	if err := binary.Write(&buf, binary.BigEndian, currentSnapshotReaderFormat); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, currentSnapshotWriterFormat); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, time.Now().UnixNano()); err != nil {
		return nil, err
	}
	zw, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(zw)
	enc.SetIndent("", "")
	if err := enc.Encode(snap); err != nil {
		zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *Manager) writeSnapshot(path string, snap *Snapshot) error {
	framed, err := encodeSnapshotFramed(snap)
	if err != nil {
		return err
	}
	sealed, err := m.sealSnapshotBlob(framed)
	if err != nil {
		return err
	}
	return os.WriteFile(path, sealed, 0o644)
}

func (m *Manager) readSnapshot(path string) (*Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	plain, err := m.openSnapshotBlob(raw)
	if err != nil {
		return nil, err
	}
	return readSnapshotFromReader(bytes.NewReader(plain))
}

func readSnapshotFromReader(r io.Reader) (*Snapshot, error) {
	br := bufio.NewReader(r)

	prefix, err := br.Peek(2)
	if err != nil {
		return nil, fmt.Errorf("%w: short snapshot envelope", ErrUnsupportedSnapshotFormat)
	}
	if bytes.Equal(prefix, []byte{0x1f, 0x8b}) {
		return nil, fmt.Errorf("%w: gzip snapshot payloads are no longer supported", ErrUnsupportedSnapshotFormat)
	}

	header, err := br.Peek(snapshotHeaderLen)
	if err != nil {
		return nil, fmt.Errorf("%w: short snapshot envelope", ErrUnsupportedSnapshotFormat)
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
	return decodeSnapshotZstd(br)
}

func decodeSnapshotZstd(r io.Reader) (*Snapshot, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// maxSnapshotSeqFromFilenames scans snapshot filenames (current .json.zst and
// legacy .json.gz) WITHOUT decrypting bodies, so nextSeq seeding never depends
// on KEK availability and cannot under-seed when a body is unreadable.
func maxSnapshotSeqFromFilenames(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read snapshot dir: %w", err)
	}
	var maxSeq uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "snapshot-") {
			continue
		}
		name := e.Name()
		var seqText string
		switch {
		case strings.HasSuffix(name, ".json.zst"):
			seqText = strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".json.zst")
		case strings.HasSuffix(name, ".json.gz"):
			seqText = strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".json.gz")
		default:
			continue
		}
		if seq, err := strconv.ParseUint(seqText, 10, 64); err == nil && seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq, nil
}
