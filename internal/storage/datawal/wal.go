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
)

type WAL struct {
	dir string
	enc *encrypt.Encryptor

	mu      sync.Mutex
	file    *os.File
	lastSeq uint64
}

func Open(dir string, enc *encrypt.Encryptor) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("datawal: create dir: %w", err)
	}
	maxSeq, err := scanMaxSeq(dir, enc)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, enc: enc, lastSeq: maxSeq}
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
	w.lastSeq++
	rec.Seq = w.lastSeq
	rec.Timestamp = time.Now().UnixNano()
	if w.enc != nil {
		err = EncodeEncryptedRecord(w.file, rec, w.enc)
	} else {
		err = EncodeRecord(w.file, rec)
	}
	if err != nil {
		w.lastSeq--
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
		if err := writeHeader(f); err != nil {
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
		if err := writeHeader(f); err != nil {
			f.Close()
			return err
		}
	}
	w.file = f
	return nil
}

func scanMaxSeq(dir string, enc *encrypt.Encryptor) (uint64, error) {
	files, err := segmentFiles(dir)
	if err != nil {
		return 0, err
	}
	var maxSeq uint64
	for _, path := range files {
		if err := scanFile(path, enc, func(rec Record) error {
			if rec.Seq > maxSeq {
				maxSeq = rec.Seq
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return maxSeq, nil
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
	if err := readHeader(f); err != nil {
		return err
	}
	for {
		var rec Record
		if enc != nil {
			rec, err = DecodeEncryptedRecord(f, enc)
		} else {
			rec, err = DecodeRecord(f)
		}
		if err == nil {
			if err := fn(rec); err != nil {
				return err
			}
			continue
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		return err
	}
}

func writeHeader(w io.Writer) error {
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], fileMagic)
	binary.BigEndian.PutUint32(buf[4:8], fileVersion)
	return writeAll(w, buf[:])
}

func readHeader(r io.Reader) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(buf[0:4]) != fileMagic {
		return fmt.Errorf("datawal: invalid magic")
	}
	if version := binary.BigEndian.Uint32(buf[4:8]); version != fileVersion {
		return fmt.Errorf("datawal: unsupported version %d", version)
	}
	return nil
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
	return strings.HasPrefix(name, "datawal-") && strings.HasSuffix(name, ".bin")
}

func segmentName(seq uint64) string {
	return fmt.Sprintf("datawal-%010d.bin", seq)
}
