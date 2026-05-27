package datawal

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	fileMagic       = uint32(0x4457414c) // "DWAL"
	fileVersion     = uint32(2)          // v2: adds dek_gen to the header (D-wal-data)
	fileHeaderBytes = 16

	fileModePlain     = byte(1)
	fileModeEncrypted = byte(2)
)

type WAL struct {
	dir       string
	sealer    RecordSealer
	namespace string
	dekGen    uint32 // generation pinned for this WAL's active files (0 under EncryptorAdapter)

	mu            sync.Mutex
	file          walFile
	lastSeq       uint64
	lastTimestamp int64

	// Group commit fields
	flushedSeq uint64
	isSyncing  bool
	syncCond   *sync.Cond
}

type walFile interface {
	io.Reader
	io.Writer
	Close() error
	Seek(offset int64, whence int) (int64, error)
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
}

func Open(dir string, sealer RecordSealer, namespace string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("datawal: create dir: %w", err)
	}
	state, err := scanState(dir, sealer, namespace)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, sealer: sealer, namespace: namespace, dekGen: state.dekGen, lastSeq: state.seq, lastTimestamp: state.timestamp}
	w.syncCond = sync.NewCond(&w.mu)
	if err := w.openAppendFile(state); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) Dir() string {
	return w.dir
}

func (w *WAL) Append(ctx context.Context, rec Record) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(rec.Payload) > MaxPayloadBytes {
		return 0, fmt.Errorf("datawal: payload too large: %d", len(rec.Payload))
	}

	return w.appendRecord(ctx, rec)
}

func (w *WAL) AppendReader(ctx context.Context, rec Record, r io.Reader) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var payload []byte
	type lenReader interface {
		Len() int
	}
	if lr, ok := r.(lenReader); ok {
		sz := lr.Len()
		if sz > MaxPayloadBytes {
			return 0, fmt.Errorf("datawal: payload too large: %d", sz)
		}
		payload = make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, err
		}
	} else {
		var err error
		payload, err = io.ReadAll(io.LimitReader(r, MaxPayloadBytes+1))
		if err != nil {
			return 0, err
		}
		if len(payload) > MaxPayloadBytes {
			return 0, fmt.Errorf("datawal: payload too large: %d", len(payload))
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	rec.Payload = payload

	return w.appendRecord(ctx, rec)
}

func (w *WAL) appendRecord(ctx context.Context, rec Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if w.file == nil {
		return 0, fmt.Errorf("datawal: wal is closed")
	}
	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	prevSeq := w.lastSeq
	prevTimestamp := w.lastTimestamp
	nextSeq, err := w.nextSeq()
	if err != nil {
		return 0, err
	}
	nextTimestamp, err := w.nextTimestamp(time.Now().UnixNano())
	if err != nil {
		return 0, err
	}
	w.lastSeq = nextSeq
	rec.Seq = nextSeq
	rec.Timestamp = nextTimestamp
	if w.sealer != nil {
		var gen uint32
		gen, err = encodeEncryptedRecordGen(w.file, rec, w.sealer, w.namespace)
		if err == nil && gen != w.dekGen {
			err = fmt.Errorf("datawal: record gen %d != pinned header gen %d", gen, w.dekGen)
		}
	} else {
		err = EncodeRecord(w.file, rec)
	}
	if err != nil {
		w.lastSeq = prevSeq
		w.lastTimestamp = prevTimestamp
		if rollbackErr := w.rollbackAppend(offset); rollbackErr != nil {
			return 0, fmt.Errorf("%w; rollback failed: %v", err, rollbackErr)
		}
		return 0, err
	}
	return rec.Seq, nil
}

func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}

	targetSeq := w.lastSeq
	if w.flushedSeq >= targetSeq {
		return nil
	}

	for w.isSyncing {
		w.syncCond.Wait()
		if w.file == nil {
			return nil
		}
		if w.flushedSeq >= targetSeq {
			return nil
		}
	}

	// Double check after waiting
	if w.flushedSeq >= targetSeq {
		return nil
	}

	w.isSyncing = true
	file := w.file
	w.mu.Unlock()

	err := file.Sync()

	w.mu.Lock()
	w.isSyncing = false
	if err == nil {
		if targetSeq > w.flushedSeq {
			w.flushedSeq = targetSeq
		}
	}
	w.syncCond.Broadcast()

	return err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.isSyncing {
		w.syncCond.Wait()
	}
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		w.file = nil
		return err
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func Replay(ctx context.Context, dir string, fromSeq uint64, sealer RecordSealer, namespace string, fn func(Record) error) error {
	files, err := segmentFiles(dir)
	if err != nil {
		return err
	}
	mono := &seqMonotonic{}
	for i, path := range files {
		if err := replayFile(ctx, path, i == len(files)-1, fromSeq, sealer, namespace, mono, fn); err != nil {
			return err
		}
	}
	return nil
}

func AppendRawForTest(dir string, data []byte) error {
	files, err := segmentFiles(dir)
	if err != nil {
		return err
	}
	var path string
	if len(files) == 0 {
		path = filepath.Join(dir, segmentName(1))
	} else {
		path = files[len(files)-1]
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if info, err := f.Stat(); err == nil && info.Size() == 0 {
		if err := writeHeader(f, fileModePlain, 0); err != nil {
			return err
		}
	}
	_, err = f.Write(data)
	return err
}

func (w *WAL) openAppendFile(state walState) error {
	files, err := segmentFiles(w.dir)
	if err != nil {
		return err
	}
	var path string
	if len(files) == 0 {
		// Task 1 intentionally uses one active segment; rotation will be added with later materializers.
		path = filepath.Join(w.dir, segmentName(w.lastSeq+1))
	} else {
		path = files[len(files)-1]
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("datawal: open segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	repairedHeader := false
	if info.Size() == 0 {
		if err := initSegment(f, w.dir, modeForSealer(w.sealer), w.dekGen); err != nil {
			f.Close()
			return err
		}
		repairedHeader = true
	} else if path == state.activePath && info.Size() < fileHeaderBytes && state.activeGoodBytes == 0 {
		if err := f.Truncate(0); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return err
		}
		if err := initSegment(f, w.dir, modeForSealer(w.sealer), w.dekGen); err != nil {
			f.Close()
			return err
		}
		repairedHeader = true
		info, err = f.Stat()
		if err != nil {
			f.Close()
			return err
		}
	} else if _, err := readHeaderForMode(f, modeForSealer(w.sealer), w.dekGen); err != nil {
		f.Close()
		return err
	}
	if !repairedHeader && path == state.activePath && state.activeGoodBytes < info.Size() {
		if err := f.Truncate(state.activeGoodBytes); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Seek(state.activeGoodBytes, io.SeekStart); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	w.file = f
	return nil
}

func initSegment(f walFile, dir string, mode byte, dekGen uint32) error {
	if err := writeHeader(f, mode, dekGen); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (w *WAL) rollbackAppend(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		_ = w.file.Close()
		w.file = nil
		return err
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		_ = w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

func (w *WAL) nextSeq() (uint64, error) {
	if w.lastSeq == math.MaxUint64 {
		return 0, fmt.Errorf("datawal: sequence overflow")
	}
	return w.lastSeq + 1, nil
}

func (w *WAL) nextTimestamp(now int64) (int64, error) {
	if now <= w.lastTimestamp {
		if w.lastTimestamp == math.MaxInt64 {
			return 0, fmt.Errorf("datawal: timestamp overflow")
		}
		now = w.lastTimestamp + 1
	}
	w.lastTimestamp = now
	return now, nil
}

// seqMonotonic enforces strictly increasing record seq across an entire WAL
// scan (all segments), completing positional binding: AAD binds (namespace,
// seq), and this rejects a frame replayed at a lower/equal position even if it
// was physically moved to the start of a later segment.
type seqMonotonic struct {
	prev uint64
	have bool
}

func (m *seqMonotonic) check(seq uint64) error {
	if m.have && seq <= m.prev {
		return fmt.Errorf("datawal: non-monotonic seq %d after %d", seq, m.prev)
	}
	m.prev, m.have = seq, true
	return nil
}

type walState struct {
	seq             uint64
	timestamp       int64
	activePath      string
	activeGoodBytes int64
	dekGen          uint32
}

func scanState(dir string, sealer RecordSealer, namespace string) (walState, error) {
	files, err := segmentFiles(dir)
	if err != nil {
		return walState{}, err
	}
	var state walState
	mono := &seqMonotonic{}
	for i, path := range files {
		goodBytes, gen, err := scanFileWithOffset(path, sealer, namespace, mono, i == len(files)-1, func(rec Record) error {
			if rec.Seq > state.seq {
				state.seq = rec.Seq
			}
			if rec.Timestamp > state.timestamp {
				state.timestamp = rec.Timestamp
			}
			return nil
		})
		if err != nil {
			return walState{}, err
		}
		state.activePath = path
		state.activeGoodBytes = goodBytes
		state.dekGen = gen // last (active) segment's header gen is the pin
	}
	return state, nil
}

func replayFile(ctx context.Context, path string, allowTruncatedTail bool, fromSeq uint64, sealer RecordSealer, namespace string, mono *seqMonotonic, fn func(Record) error) error {
	return scanFile(path, sealer, namespace, mono, allowTruncatedTail, func(rec Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rec.Seq <= fromSeq {
			return nil
		}
		return fn(rec)
	})
}

func scanFile(path string, sealer RecordSealer, namespace string, mono *seqMonotonic, allowTruncatedTail bool, fn func(Record) error) error {
	_, _, err := scanFileWithOffset(path, sealer, namespace, mono, allowTruncatedTail, fn)
	return err
}

func scanFileWithOffset(path string, sealer RecordSealer, namespace string, mono *seqMonotonic, allowTruncatedTail bool, fn func(Record) error) (int64, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if info.Size() < fileHeaderBytes {
		if allowTruncatedTail {
			return 0, 0, nil
		}
		return 0, 0, io.ErrUnexpectedEOF
	}
	mode, gen, err := readHeader(f)
	if err != nil {
		return 0, 0, err
	}
	if err := checkMode(mode, sealer); err != nil {
		return 0, 0, err
	}
	goodBytes, err := scanRecords(f, mode, sealer, namespace, gen, mono, allowTruncatedTail, fn)
	return goodBytes, gen, err
}

func scanRecords(r io.ReadSeeker, mode byte, sealer RecordSealer, namespace string, gen uint32, mono *seqMonotonic, allowTruncatedTail bool, fn func(Record) error) (int64, error) {
	goodBytes, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	for {
		beforeFrame, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return goodBytes, err
		}
		var rec Record
		if mode == fileModeEncrypted {
			rec, err = DecodeEncryptedRecord(r, sealer, namespace, gen)
		} else {
			rec, err = DecodeRecord(r)
		}
		if err == io.EOF {
			return beforeFrame, nil
		}
		if err == io.ErrUnexpectedEOF && allowTruncatedTail {
			return beforeFrame, nil
		}
		if err != nil {
			return goodBytes, err
		}
		if err := mono.check(rec.Seq); err != nil {
			return goodBytes, err
		}
		if err := fn(rec); err != nil {
			return goodBytes, err
		}
		goodBytes, err = r.Seek(0, io.SeekCurrent)
		if err != nil {
			return goodBytes, err
		}
	}
}

func writeHeader(w io.Writer, mode byte, dekGen uint32) error {
	var buf [fileHeaderBytes]byte
	binary.BigEndian.PutUint32(buf[0:4], fileMagic)
	binary.BigEndian.PutUint32(buf[4:8], fileVersion)
	buf[8] = mode
	// buf[9:12] reserved (zero)
	binary.BigEndian.PutUint32(buf[12:16], dekGen)
	return writeAll(w, buf[:])
}

func readHeader(r io.Reader) (mode byte, dekGen uint32, err error) {
	var buf [fileHeaderBytes]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, 0, err
	}
	if binary.BigEndian.Uint32(buf[0:4]) != fileMagic {
		return 0, 0, fmt.Errorf("datawal: invalid magic")
	}
	if version := binary.BigEndian.Uint32(buf[4:8]); version != fileVersion {
		return 0, 0, fmt.Errorf("datawal: unsupported version %d", version)
	}
	mode = buf[8]
	if mode != fileModePlain && mode != fileModeEncrypted {
		return 0, 0, fmt.Errorf("datawal: unsupported mode %d", mode)
	}
	return mode, binary.BigEndian.Uint32(buf[12:16]), nil
}

func readHeaderForMode(f walFile, want byte, wantGen uint32) (byte, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	mode, gen, err := readHeader(f)
	if err != nil {
		return 0, err
	}
	if mode != want {
		return 0, modeMismatch(mode, want)
	}
	if mode == fileModeEncrypted && gen != wantGen {
		return 0, fmt.Errorf("datawal: header gen %d != pinned gen %d", gen, wantGen)
	}
	_, err = f.Seek(0, io.SeekEnd)
	return mode, err
}

func checkMode(mode byte, sealer RecordSealer) error {
	want := modeForSealer(sealer)
	if mode != want {
		return modeMismatch(mode, want)
	}
	return nil
}

func modeMismatch(got, want byte) error {
	return fmt.Errorf("datawal: segment mode mismatch: got %s, want %s", modeName(got), modeName(want))
}

func modeForSealer(s RecordSealer) byte {
	if s == nil {
		return fileModePlain
	}
	return fileModeEncrypted
}

func modeName(mode byte) string {
	switch mode {
	case fileModePlain:
		return "plain"
	case fileModeEncrypted:
		return "encrypted"
	default:
		return fmt.Sprintf("unknown(%d)", mode)
	}
}

func segmentFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("datawal: read dir: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if isSegmentName(entry.Name()) {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

func isSegmentName(name string) bool {
	if len(name) != len("datawal-0000000000.bin") {
		return false
	}
	if !strings.HasPrefix(name, "datawal-") || !strings.HasSuffix(name, ".bin") {
		return false
	}
	for _, ch := range name[len("datawal-") : len("datawal-")+10] {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func segmentName(seq uint64) string {
	return fmt.Sprintf("datawal-%010d.bin", seq)
}
