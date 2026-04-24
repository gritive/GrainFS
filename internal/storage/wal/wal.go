// Package wal implements a data-plane Write-Ahead Log for PITR support.
// WAL writes are advisory: failures are logged but do not block S3 operations.
package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	fileMagic = uint32(0x57414C31) // "WAL1"

	// fileVersionV1 is the legacy wire format (no VersionID field).
	fileVersionV1 = uint32(1)
	// fileVersionV2 adds VersionID to each entry so PITR can replay
	// object-level version history across Put/Delete/DeleteVersion.
	fileVersionV2 = uint32(2)
	// fileVersion is the version written by new segments.
	fileVersion = fileVersionV2

	OpPut           = byte(0)
	OpDelete        = byte(1)
	OpDeleteVersion = byte(2)

	maxSegmentBytes   = 64 * 1024 * 1024 // 64 MB
	maxSegmentEntries = 10_000
	chanCap           = 4096
)

// Entry is a single WAL record.
type Entry struct {
	Seq         uint64
	Timestamp   int64 // UnixNano
	Op          byte
	Bucket      string
	Key         string
	ETag        string
	ContentType string
	Size        int64
	// VersionID is the object version associated with the mutation.
	// Empty for legacy (fileVersionV1) entries and for operations that do not
	// carry a version (e.g. backends without versioning).
	VersionID string
}

// WAL manages append-only log files in dir.
// A single background goroutine drains a channel and writes to disk.
type WAL struct {
	dir string

	mu        sync.Mutex
	file      *os.File
	fileBytes int
	fileCount int

	ch   chan Entry
	done chan struct{}
	wg   sync.WaitGroup

	lastSeq atomic.Uint64
}

// Open opens (or creates) a WAL in dir. Starts the background writer.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}
	w := &WAL{
		dir:  dir,
		ch:   make(chan Entry, chanCap),
		done: make(chan struct{}),
	}
	// Seed lastSeq from existing files
	maxSeq, err := w.scanMaxSeq()
	if err != nil {
		return nil, err
	}
	w.lastSeq.Store(maxSeq)

	w.wg.Add(1)
	go w.writer()
	return w, nil
}

// Close drains pending writes and closes the WAL file.
func (w *WAL) Close() error {
	close(w.done)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			log.Warn().Err(err).Msg("wal: sync on close failed")
		}
		return w.file.Close()
	}
	return nil
}

// AppendAsync queues an entry for async write. If the channel is full,
// writes synchronously (backpressure, never drops).
func (w *WAL) AppendAsync(e Entry) {
	e.Seq = w.lastSeq.Add(1)
	e.Timestamp = time.Now().UnixNano()
	select {
	case w.ch <- e:
	default:
		log.Warn().Msg("wal: channel full, writing synchronously")
		w.mu.Lock()
		if err := w.writeEntry(e); err != nil {
			log.Error().Err(err).Msg("wal: sync write failed")
		}
		w.mu.Unlock()
	}
}

// CurrentSeq returns the sequence number of the last entry assigned (not yet
// necessarily flushed to disk, but recorded in lastSeq).
func (w *WAL) CurrentSeq() uint64 { return w.lastSeq.Load() }

// Flush syncs the current WAL file to disk.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

// writer is the single background goroutine that drains the channel.
func (w *WAL) writer() {
	defer w.wg.Done()
	for {
		select {
		case e := <-w.ch:
			w.mu.Lock()
			if err := w.writeEntry(e); err != nil {
				log.Error().Err(err).Msg("wal: write failed")
			}
			w.mu.Unlock()
		case <-w.done:
			// Drain remaining entries
			for {
				select {
				case e := <-w.ch:
					w.mu.Lock()
					if err := w.writeEntry(e); err != nil {
						log.Error().Err(err).Msg("wal: drain write failed")
					}
					w.mu.Unlock()
				default:
					return
				}
			}
		}
	}
}

// writeEntry writes e to the current segment file, rotating if necessary.
// Must be called with w.mu held.
func (w *WAL) writeEntry(e Entry) error {
	if w.file == nil || w.fileBytes >= maxSegmentBytes || w.fileCount >= maxSegmentEntries {
		if err := w.rotate(e.Seq); err != nil {
			return err
		}
	}
	n, err := marshalEntry(w.file, e)
	if err != nil {
		return err
	}
	w.fileBytes += n
	w.fileCount++
	return nil
}

// rotate closes the current segment and opens a new one named after firstSeq.
// Must be called with w.mu held.
func (w *WAL) rotate(firstSeq uint64) error {
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			log.Warn().Err(err).Msg("wal: sync on rotate failed")
		}
		w.file.Close()
		w.file = nil
	}
	name := filepath.Join(w.dir, fmt.Sprintf("wal-%010d.bin", firstSeq))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment %s: %w", name, err)
	}
	// Write header only if new file
	fi, _ := f.Stat()
	if fi.Size() == 0 {
		if err := writeHeader(f); err != nil {
			f.Close()
			return err
		}
	}
	w.file = f
	w.fileBytes = int(fi.Size())
	w.fileCount = 0
	return nil
}

// scanMaxSeq reads all existing WAL segments and returns the highest seq seen.
func (w *WAL) scanMaxSeq() (uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, fmt.Errorf("wal: read dir: %w", err)
	}
	var maxSeq uint64
	for _, e := range entries {
		if !walFilename(e.Name()) {
			continue
		}
		seq, err := lastSeqInFile(filepath.Join(w.dir, e.Name()))
		if err != nil {
			log.Warn().Str("file", e.Name()).Err(err).Msg("wal: scan failed")
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq, nil
}

// segmentFiles returns WAL segment paths sorted ascending by first-seq in filename.
func segmentFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if walFilename(e.Name()) {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files) // lexicographic = numeric because of zero-padded names
	return files, nil
}

func walFilename(name string) bool {
	return strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".bin")
}

// Replay reads WAL entries with seq > fromSeq and timestamp <= targetTime,
// calling fn for each. Returns the number of entries processed.
func Replay(dir string, fromSeq uint64, targetTime time.Time, fn func(Entry)) (int, error) {
	target := targetTime.UnixNano()
	files, err := segmentFiles(dir)
	if err != nil {
		return 0, err
	}
	var count int
	for _, path := range files {
		n, err := replayFile(path, fromSeq, target, fn)
		count += n
		if err != nil {
			log.Warn().Str("file", path).Err(err).Msg("wal: replay file error")
			// Continue with next file
		}
	}
	return count, nil
}

func replayFile(path string, fromSeq uint64, targetNs int64, fn func(Entry)) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	ver, err := readHeader(f)
	if err != nil {
		return 0, err
	}

	var count int
	for {
		e, err := unmarshalEntry(f, ver)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("wal: read entry: %w", err)
		}
		if e.Seq <= fromSeq {
			continue
		}
		if e.Timestamp > targetNs {
			continue
		}
		fn(e)
		count++
	}
	return count, nil
}

func lastSeqInFile(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	ver, err := readHeader(f)
	if err != nil {
		return 0, err
	}
	var lastSeq uint64
	for {
		e, err := unmarshalEntry(f, ver)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return lastSeq, nil
		}
		lastSeq = e.Seq
	}
	return lastSeq, nil
}

// --- Binary format ---

func writeHeader(w io.Writer) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], fileMagic)
	binary.BigEndian.PutUint32(buf[4:8], fileVersion)
	_, err := w.Write(buf)
	return err
}

// readHeader reads and validates the segment header. Returns the wire version
// so the caller can dispatch per-entry decoding appropriately.
func readHeader(r io.Reader) (uint32, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, fmt.Errorf("wal: read header: %w", err)
	}
	if binary.BigEndian.Uint32(buf[0:4]) != fileMagic {
		return 0, fmt.Errorf("wal: invalid magic")
	}
	v := binary.BigEndian.Uint32(buf[4:8])
	if v != fileVersionV1 && v != fileVersionV2 {
		return 0, fmt.Errorf("wal: unsupported version %d", v)
	}
	return v, nil
}

// marshalEntry writes an entry in binary format and returns bytes written.
// Format (v1): [8:seq][8:ts][1:op][2:bucket_len][bucket][2:key_len][key][2:etag_len][etag][2:ct_len][ct][8:size]
// Format (v2): v1 fields + [2:versionid_len][versionid]
// New segments always use v2.
func marshalEntry(w io.Writer, e Entry) (int, error) {
	bucket := []byte(e.Bucket)
	key := []byte(e.Key)
	etag := []byte(e.ETag)
	ct := []byte(e.ContentType)
	vid := []byte(e.VersionID)

	size := 8 + 8 + 1 + 2 + len(bucket) + 2 + len(key) + 2 + len(etag) + 2 + len(ct) + 8 + 2 + len(vid)
	buf := make([]byte, size)
	off := 0

	binary.BigEndian.PutUint64(buf[off:], e.Seq)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Timestamp))
	off += 8
	buf[off] = e.Op
	off++
	binary.BigEndian.PutUint16(buf[off:], uint16(len(bucket)))
	off += 2
	copy(buf[off:], bucket)
	off += len(bucket)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(key)))
	off += 2
	copy(buf[off:], key)
	off += len(key)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(etag)))
	off += 2
	copy(buf[off:], etag)
	off += len(etag)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(ct)))
	off += 2
	copy(buf[off:], ct)
	off += len(ct)
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Size))
	off += 8
	binary.BigEndian.PutUint16(buf[off:], uint16(len(vid)))
	off += 2
	copy(buf[off:], vid)

	n, err := w.Write(buf)
	return n, err
}

// unmarshalEntry reads an entry in the given wire version.
// v1 returns empty VersionID; v2 reads the trailing VersionID field.
func unmarshalEntry(r io.Reader, wireVer uint32) (Entry, error) {
	var fixed [8 + 8 + 1]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return Entry{}, err
	}
	e := Entry{
		Seq:       binary.BigEndian.Uint64(fixed[0:8]),
		Timestamp: int64(binary.BigEndian.Uint64(fixed[8:16])),
		Op:        fixed[16],
	}

	readStr := func() (string, error) {
		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return "", err
		}
		l := binary.BigEndian.Uint16(lenBuf[:])
		if l == 0 {
			return "", nil
		}
		data := make([]byte, l)
		if _, err := io.ReadFull(r, data); err != nil {
			return "", err
		}
		return string(data), nil
	}

	var err error
	if e.Bucket, err = readStr(); err != nil {
		return e, err
	}
	if e.Key, err = readStr(); err != nil {
		return e, err
	}
	if e.ETag, err = readStr(); err != nil {
		return e, err
	}
	if e.ContentType, err = readStr(); err != nil {
		return e, err
	}

	var sizeBuf [8]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return e, err
	}
	e.Size = int64(binary.BigEndian.Uint64(sizeBuf[:]))

	if wireVer >= fileVersionV2 {
		if e.VersionID, err = readStr(); err != nil {
			return e, err
		}
	}
	return e, nil
}
