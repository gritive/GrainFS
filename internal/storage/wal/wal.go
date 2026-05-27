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

	"github.com/gritive/GrainFS/internal/encrypt"
)

const (
	fileMagic = uint32(0x57414C31) // "WAL1"

	// fileVersionV1 is the legacy wire format (no VersionID field).
	fileVersionV1 = uint32(1)
	// fileVersionV2 adds VersionID to each entry so PITR can replay
	// object-level version history across Put/Delete/DeleteVersion.
	fileVersionV2 = uint32(2)
	// fileVersionV3 stores seq/timestamp in a plaintext frame and encrypts
	// the mutation body.
	fileVersionV3 = uint32(3)
	// fileVersionV4 is the seam-based encrypted format; the header carries
	// dek_gen so replay can pin the generation each ciphertext was sealed under.
	fileVersionV4 = uint32(4)

	OpPut           = byte(0)
	OpDelete        = byte(1)
	OpDeleteVersion = byte(2)

	maxSegmentBytes   = 64 * 1024 * 1024 // 64 MB
	maxSegmentEntries = 10_000
	chanCap           = 4096
	maxEntryBodyBytes = 16 * 1024 * 1024
)

// RecordSealer is the wal package's view of the storage.DataEncryptor seam.
// Declared locally so package wal never imports storage; *storage.EncryptorAdapter
// satisfies it structurally.
type RecordSealer interface {
	Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
}

func walEntryAADFields(namespace string, seq uint64) []encrypt.AADField {
	return []encrypt.AADField{encrypt.FieldString(namespace), encrypt.FieldUint64(seq)}
}

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

	lastSeq   atomic.Uint64
	sealer    RecordSealer
	namespace string
	dekGen    uint32
	version   uint32
}

// Open opens (or creates) a WAL in dir. Starts the background writer.
func Open(dir string) (*WAL, error) {
	return open(dir, nil, "", fileVersionV2)
}

// OpenEncrypted opens (or creates) a WAL that encrypts mutation bodies.
func OpenEncrypted(dir string, sealer RecordSealer, namespace string) (*WAL, error) {
	if sealer == nil {
		return nil, fmt.Errorf("encrypted WAL requires sealer")
	}
	return open(dir, sealer, namespace, fileVersionV4)
}

func open(dir string, sealer RecordSealer, namespace string, version uint32) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}
	w := &WAL{
		dir:       dir,
		ch:        make(chan Entry, chanCap),
		done:      make(chan struct{}),
		sealer:    sealer,
		namespace: namespace,
		version:   version,
	}
	// Seed lastSeq from existing files; pin the active segment's header gen.
	maxSeq, dekGen, err := w.scanMaxSeq()
	if err != nil {
		return nil, err
	}
	w.lastSeq.Store(maxSeq)
	w.dekGen = dekGen

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
	n, err := marshalEntry(w.file, e, w.sealer, w.namespace, w.dekGen)
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
		if err := writeHeader(f, w.version, w.dekGen); err != nil {
			f.Close()
			return err
		}
	}
	w.file = f
	w.fileBytes = int(fi.Size())
	w.fileCount = 0
	return nil
}

// scanMaxSeq reads all existing WAL segments and returns the highest seq seen
// plus the dek_gen from the active (highest-seq) segment's header. Segments are
// scanned in seq order so the active segment is the last one with entries.
func (w *WAL) scanMaxSeq() (uint64, uint32, error) {
	files, err := segmentFiles(w.dir)
	if err != nil {
		return 0, 0, err
	}
	var maxSeq uint64
	var activeGen uint32
	for _, path := range files {
		seq, gen, err := lastSeqInFile(path, w.sealer, w.namespace)
		if err != nil {
			log.Warn().Str("file", path).Err(err).Msg("wal: scan failed")
			if w.sealer != nil {
				return 0, 0, err
			}
		}
		if seq >= maxSeq {
			maxSeq = seq
			activeGen = gen
		}
	}
	return maxSeq, activeGen, nil
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

// seqMonotonic enforces strict ascending seq across all segments of a replay.
type seqMonotonic struct {
	prev uint64
	have bool
}

func (m *seqMonotonic) check(seq uint64) error {
	if m.have && seq <= m.prev {
		return fmt.Errorf("wal: non-monotonic seq %d after %d", seq, m.prev)
	}
	m.prev, m.have = seq, true
	return nil
}

// Replay reads WAL entries with seq > fromSeq and timestamp <= targetTime,
// calling fn for each. Returns the number of entries processed.
func Replay(dir string, fromSeq uint64, targetTime time.Time, fn func(Entry)) (int, error) {
	return replay(dir, fromSeq, targetTime, nil, "", false, fn)
}

// ReplayEncrypted reads WAL entries written by OpenEncrypted.
func ReplayEncrypted(dir string, fromSeq uint64, targetTime time.Time, sealer RecordSealer, namespace string, fn func(Entry)) (int, error) {
	if sealer == nil {
		return 0, fmt.Errorf("encrypted WAL replay requires sealer")
	}
	return replay(dir, fromSeq, targetTime, sealer, namespace, true, fn)
}

func replay(dir string, fromSeq uint64, targetTime time.Time, sealer RecordSealer, namespace string, strict bool, fn func(Entry)) (int, error) {
	target := targetTime.UnixNano()
	files, err := segmentFiles(dir)
	if err != nil {
		return 0, err
	}
	mono := &seqMonotonic{}
	var count int
	for _, path := range files {
		n, err := replayFile(path, fromSeq, target, sealer, namespace, mono, fn)
		count += n
		if err != nil {
			if strict {
				return count, err
			}
			log.Warn().Str("file", path).Err(err).Msg("wal: replay file error")
			// Continue with next file
		}
	}
	return count, nil
}

func replayFile(path string, fromSeq uint64, targetNs int64, sealer RecordSealer, namespace string, mono *seqMonotonic, fn func(Entry)) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	ver, headerGen, err := readHeader(f)
	if err != nil {
		return 0, err
	}

	var count int
	for {
		e, body, err := readEntryFrame(f, ver)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if ver == fileVersionV4 {
				return count, err
			}
			break
		}
		if err != nil {
			return count, fmt.Errorf("wal: read entry: %w", err)
		}
		if ver == fileVersionV4 {
			if sealer == nil {
				return count, fmt.Errorf("wal: encrypted entry requires sealer")
			}
			e, err = decryptEntryBody(e, body, sealer, namespace, headerGen)
			if err != nil {
				return count, err
			}
		}
		if err := mono.check(e.Seq); err != nil {
			return count, err
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

func lastSeqInFile(path string, sealer RecordSealer, namespace string) (uint64, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	ver, headerGen, err := readHeader(f)
	if err != nil {
		return 0, 0, err
	}
	var lastSeq uint64
	for {
		e, body, err := readEntryFrame(f, ver)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			if ver == fileVersionV4 {
				return lastSeq, headerGen, err
			}
			return lastSeq, headerGen, nil
		}
		if ver == fileVersionV4 {
			if sealer == nil {
				return lastSeq, headerGen, fmt.Errorf("wal: encrypted segment requires sealer")
			}
			if _, err := decryptEntryBody(e, body, sealer, namespace, headerGen); err != nil {
				return lastSeq, headerGen, err
			}
		}
		lastSeq = e.Seq
	}
	return lastSeq, headerGen, nil
}

// --- Binary format ---

func writeHeader(w io.Writer, version uint32, dekGen uint32) error {
	if version == fileVersionV4 {
		var buf [12]byte
		binary.BigEndian.PutUint32(buf[0:4], fileMagic)
		binary.BigEndian.PutUint32(buf[4:8], version)
		binary.BigEndian.PutUint32(buf[8:12], dekGen)
		_, err := w.Write(buf[:])
		return err
	}
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], fileMagic)
	binary.BigEndian.PutUint32(buf[4:8], version)
	_, err := w.Write(buf[:])
	return err
}

// readHeader reads and validates the segment header. Returns the wire version
// (for per-entry decode dispatch) and the dek_gen (0 for non-v4 layouts).
func readHeader(r io.Reader) (uint32, uint32, error) {
	var fixed [8]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return 0, 0, err
	}
	if binary.BigEndian.Uint32(fixed[0:4]) != fileMagic {
		return 0, 0, fmt.Errorf("wal: invalid magic")
	}
	version := binary.BigEndian.Uint32(fixed[4:8])
	switch version {
	case fileVersionV1, fileVersionV2, fileVersionV3:
		return version, 0, nil
	case fileVersionV4:
		var gen [4]byte
		if _, err := io.ReadFull(r, gen[:]); err != nil {
			return 0, 0, err
		}
		return version, binary.BigEndian.Uint32(gen[:]), nil
	default:
		return 0, 0, fmt.Errorf("wal: unsupported version %d", version)
	}
}

const poolMaxBufSize = 256 * 1024 // 256 KB is way more than enough for metadata entries

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, poolMaxBufSize)
		return &b
	},
}

func getBuffer(size int) []byte {
	if size > poolMaxBufSize {
		return make([]byte, size)
	}
	ptr := bufferPool.Get().(*[]byte)
	return (*ptr)[:size]
}

func putBuffer(buf []byte) {
	if cap(buf) < poolMaxBufSize {
		return
	}
	clear(buf[:cap(buf)])
	b := buf[:poolMaxBufSize]
	bufferPool.Put(&b)
}

func putString16(dst []byte, off int, s string) int {
	binary.BigEndian.PutUint16(dst[off:], uint16(len(s)))
	off += 2
	copy(dst[off:], s)
	return off + len(s)
}

// marshalEntry writes an entry in binary format and returns bytes written.
func marshalEntry(w io.Writer, e Entry, sealer RecordSealer, namespace string, wantGen uint32) (int, error) {
	if sealer == nil {
		return marshalPlainEntry(w, e)
	}
	bodySize := 1 + 2 + len(e.Bucket) + 2 + len(e.Key) + 2 + len(e.ETag) + 2 + len(e.ContentType) + 8 + 2 + len(e.VersionID)
	bodyBuf := getBuffer(bodySize)
	defer putBuffer(bodyBuf)

	body := marshalEntryBodyTo(bodyBuf[:0], e, true)

	sealed, gen, err := sealer.Seal(encrypt.DomainWAL, walEntryAADFields(namespace, e.Seq), body)
	if err != nil {
		return 0, fmt.Errorf("wal: encrypt entry body: %w", err)
	}
	if gen != wantGen {
		return 0, fmt.Errorf("wal: entry gen %d != pinned header gen %d", gen, wantGen)
	}

	frameSize := 8 + 8 + 4 + len(sealed)
	frameBuf := getBuffer(frameSize)
	defer putBuffer(frameBuf)

	off := 0
	binary.BigEndian.PutUint64(frameBuf[off:], e.Seq)
	off += 8
	binary.BigEndian.PutUint64(frameBuf[off:], uint64(e.Timestamp))
	off += 8
	binary.BigEndian.PutUint32(frameBuf[off:], uint32(len(sealed)))
	off += 4
	copy(frameBuf[off:], sealed)

	n, err := w.Write(frameBuf[:frameSize])
	return n, err
}

// marshalPlainEntry writes the legacy plaintext v2 record format.
// Format (v1): [8:seq][8:ts][1:op][2:bucket_len][bucket][2:key_len][key][2:etag_len][etag][2:ct_len][ct][8:size]
// Format (v2): v1 fields + [2:versionid_len][versionid]
func marshalPlainEntry(w io.Writer, e Entry) (int, error) {
	size := 8 + 8 + 1 + 2 + len(e.Bucket) + 2 + len(e.Key) + 2 + len(e.ETag) + 2 + len(e.ContentType) + 8 + 2 + len(e.VersionID)
	buf := getBuffer(size)
	defer putBuffer(buf)
	off := 0

	binary.BigEndian.PutUint64(buf[off:], e.Seq)
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Timestamp))
	off += 8
	buf[off] = e.Op
	off++
	off = putString16(buf, off, e.Bucket)
	off = putString16(buf, off, e.Key)
	off = putString16(buf, off, e.ETag)
	off = putString16(buf, off, e.ContentType)
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Size))
	off += 8
	_ = putString16(buf, off, e.VersionID)

	n, err := w.Write(buf)
	return n, err
}

func marshalEntryBodyTo(dst []byte, e Entry, includeVersionID bool) []byte {
	size := 1 + 2 + len(e.Bucket) + 2 + len(e.Key) + 2 + len(e.ETag) + 2 + len(e.ContentType) + 8
	if includeVersionID {
		size += 2 + len(e.VersionID)
	}
	if cap(dst) < size {
		dst = make([]byte, size)
	}
	buf := dst[:size]
	off := 0
	buf[off] = e.Op
	off++
	off = putString16(buf, off, e.Bucket)
	off = putString16(buf, off, e.Key)
	off = putString16(buf, off, e.ETag)
	off = putString16(buf, off, e.ContentType)
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Size))
	off += 8
	if includeVersionID {
		_ = putString16(buf, off, e.VersionID)
	}
	return buf
}

func readEntryFrame(r io.Reader, wireVer uint32) (Entry, []byte, error) {
	if wireVer == fileVersionV4 {
		var fixed [8 + 8 + 4]byte
		if _, err := io.ReadFull(r, fixed[:]); err != nil {
			return Entry{}, nil, err
		}
		bodyLen := binary.BigEndian.Uint32(fixed[16:20])
		if bodyLen > maxEntryBodyBytes {
			return Entry{}, nil, fmt.Errorf("wal: encrypted entry body too large: %d", bodyLen)
		}
		body := make([]byte, bodyLen)
		if bodyLen > 0 {
			if _, err := io.ReadFull(r, body); err != nil {
				if err == io.EOF {
					// fixed header consumed but body missing → torn tail, not clean EOF.
					return Entry{}, nil, io.ErrUnexpectedEOF
				}
				return Entry{}, nil, err
			}
		}
		return Entry{
			Seq:       binary.BigEndian.Uint64(fixed[0:8]),
			Timestamp: int64(binary.BigEndian.Uint64(fixed[8:16])),
		}, body, nil
	}
	e, err := readPlainEntry(r, wireVer)
	return e, nil, err
}

func decryptEntryBody(frame Entry, encryptedBody []byte, sealer RecordSealer, namespace string, gen uint32) (Entry, error) {
	body, err := sealer.Open(encrypt.DomainWAL, walEntryAADFields(namespace, frame.Seq), gen, encryptedBody)
	if err != nil {
		return Entry{}, fmt.Errorf("wal: decrypt entry body: %w", err)
	}
	e, err := unmarshalEntryBody(body, true)
	clear(body)
	if err != nil {
		return Entry{}, err
	}
	e.Seq = frame.Seq
	e.Timestamp = frame.Timestamp
	return e, nil
}

// readPlainEntry reads a legacy plaintext entry in the given wire version.
// v1 returns empty VersionID; v2 reads the trailing VersionID field.
func readPlainEntry(r io.Reader, wireVer uint32) (Entry, error) {
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

func unmarshalEntryBody(body []byte, includeVersionID bool) (Entry, error) {
	if len(body) < 1 {
		return Entry{}, io.ErrUnexpectedEOF
	}
	e := Entry{Op: body[0]}
	off := 1
	readStr := func() (string, error) {
		if len(body[off:]) < 2 {
			return "", io.ErrUnexpectedEOF
		}
		l := int(binary.BigEndian.Uint16(body[off:]))
		off += 2
		if len(body[off:]) < l {
			return "", io.ErrUnexpectedEOF
		}
		s := string(body[off : off+l])
		off += l
		return s, nil
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
	if len(body[off:]) < 8 {
		return e, io.ErrUnexpectedEOF
	}
	e.Size = int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	if includeVersionID {
		if e.VersionID, err = readStr(); err != nil {
			return e, err
		}
	}
	return e, nil
}
