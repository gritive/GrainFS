package datawal

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const (
	fileMagic   = uint32(0x4457414c) // "DWAL"
	fileVersion = uint32(1)

	fileModePlain     = byte(1)
	fileModeEncrypted = byte(2)
)

type WAL struct {
	dir string
	enc *encrypt.Encryptor

	mu            sync.Mutex
	file          walFile
	lastSeq       uint64
	lastTimestamp int64
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

func Open(dir string, enc *encrypt.Encryptor) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("datawal: create dir: %w", err)
	}
	state, err := scanState(dir, enc)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, enc: enc, lastSeq: state.seq, lastTimestamp: state.timestamp}
	if err := w.openAppendFile(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) Dir() string {
	return w.dir
}

func (w *WAL) Append(ctx context.Context, rec Record) (uint64, error) {
	return w.AppendReader(ctx, rec, bytes.NewReader(rec.Payload))
}

func (w *WAL) AppendReader(ctx context.Context, rec Record, r io.Reader) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	payload, err := io.ReadAll(io.LimitReader(r, MaxPayloadBytes+1))
	if err != nil {
		return 0, err
	}
	if len(payload) > MaxPayloadBytes {
		return 0, fmt.Errorf("datawal: payload too large: %d", len(payload))
	}
	rec.Payload = append([]byte(nil), payload...)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, fmt.Errorf("datawal: wal is closed")
	}
	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	prevSeq := w.lastSeq
	prevTimestamp := w.lastTimestamp
	w.lastSeq++
	rec.Seq = w.lastSeq
	rec.Timestamp = w.nextTimestamp(time.Now().UnixNano())
	if w.enc != nil {
		err = EncodeEncryptedRecord(w.file, rec, w.enc)
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
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
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

func Replay(ctx context.Context, dir string, fromSeq uint64, enc *encrypt.Encryptor, fn func(Record) error) error {
	files, err := segmentFiles(dir)
	if err != nil {
		return err
	}
	for _, path := range files {
		if err := replayFile(ctx, path, fromSeq, enc, fn); err != nil {
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
		if err := writeHeader(f, fileModePlain); err != nil {
			return err
		}
	}
	_, err = f.Write(data)
	return err
}

func (w *WAL) openAppendFile() error {
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
	if info.Size() == 0 {
		if err := writeHeader(f, modeForEncryptor(w.enc)); err != nil {
			f.Close()
			return err
		}
	} else if _, err := readHeaderForMode(f, modeForEncryptor(w.enc)); err != nil {
		f.Close()
		return err
	}
	w.file = f
	return nil
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

func (w *WAL) nextTimestamp(now int64) int64 {
	if now <= w.lastTimestamp {
		now = w.lastTimestamp + 1
	}
	w.lastTimestamp = now
	return now
}

type walState struct {
	seq       uint64
	timestamp int64
}

func scanState(dir string, enc *encrypt.Encryptor) (walState, error) {
	files, err := segmentFiles(dir)
	if err != nil {
		return walState{}, err
	}
	var state walState
	for _, path := range files {
		if err := scanFile(path, enc, func(rec Record) error {
			if rec.Seq > state.seq {
				state.seq = rec.Seq
			}
			if rec.Timestamp > state.timestamp {
				state.timestamp = rec.Timestamp
			}
			return nil
		}); err != nil {
			return walState{}, err
		}
	}
	return state, nil
}

func replayFile(ctx context.Context, path string, fromSeq uint64, enc *encrypt.Encryptor, fn func(Record) error) error {
	return scanFile(path, enc, func(rec Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rec.Seq <= fromSeq {
			return nil
		}
		return fn(rec)
	})
}

func scanFile(path string, enc *encrypt.Encryptor, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	mode, err := readHeader(f)
	if err != nil {
		return err
	}
	if err := checkMode(mode, enc); err != nil {
		return err
	}
	return scanRecords(f, mode, enc, fn)
}

func scanRecords(r io.Reader, mode byte, enc *encrypt.Encryptor, fn func(Record) error) error {
	for {
		var rec Record
		body, err := readFrame(r)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
		if mode == fileModeEncrypted {
			plain, err := enc.OpenValueAAD([]byte(encryptedRecordAAD), body)
			if err != nil {
				return fmt.Errorf("datawal: decrypt record: %w", err)
			}
			rec, err = unmarshalRecordBody(plain)
			clear(plain)
			if err != nil {
				return err
			}
		} else {
			rec, err = unmarshalRecordBody(body)
			if err != nil {
				return err
			}
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

func writeHeader(w io.Writer, mode byte) error {
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:4], fileMagic)
	binary.BigEndian.PutUint32(buf[4:8], fileVersion)
	buf[8] = mode
	return writeAll(w, buf[:])
}

func readHeader(r io.Reader) (byte, error) {
	var buf [12]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint32(buf[0:4]) != fileMagic {
		return 0, fmt.Errorf("datawal: invalid magic")
	}
	if version := binary.BigEndian.Uint32(buf[4:8]); version != fileVersion {
		return 0, fmt.Errorf("datawal: unsupported version %d", version)
	}
	mode := buf[8]
	if mode != fileModePlain && mode != fileModeEncrypted {
		return 0, fmt.Errorf("datawal: unsupported mode %d", mode)
	}
	return mode, nil
}

func readHeaderForMode(f walFile, want byte) (byte, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	mode, err := readHeader(f)
	if err != nil {
		return 0, err
	}
	if mode != want {
		return 0, modeMismatch(mode, want)
	}
	_, err = f.Seek(0, io.SeekEnd)
	return mode, err
}

func checkMode(mode byte, enc *encrypt.Encryptor) error {
	want := modeForEncryptor(enc)
	if mode != want {
		return modeMismatch(mode, want)
	}
	return nil
}

func modeMismatch(got, want byte) error {
	return fmt.Errorf("datawal: segment mode mismatch: got %s, want %s", modeName(got), modeName(want))
}

func modeForEncryptor(enc *encrypt.Encryptor) byte {
	if enc == nil {
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
