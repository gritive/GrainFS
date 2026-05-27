package datawal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeSealer is a real-crypto RecordSealer for datawal tests. It mirrors
// storage.EncryptorAdapter (gen 0) without importing storage (which would form
// a cycle: storage imports datawal).
type fakeSealer struct {
	enc       *encrypt.Encryptor
	clusterID []byte
}

func newFakeSealer() *fakeSealer {
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		panic(err)
	}
	return &fakeSealer{enc: enc, clusterID: make([]byte, 16)}
}

func (s *fakeSealer) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	ct, err := s.enc.SealValueAADTo(nil, encrypt.BuildAAD(domain, s.clusterID, fields...), plain)
	return ct, 0, err
}

func (s *fakeSealer) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return s.enc.OpenValueAADTo(nil, encrypt.BuildAAD(domain, s.clusterID, fields...), ct)
}

func TestHeaderV2RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHeader(&buf, fileModeEncrypted, 7); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	if buf.Len() != fileHeaderBytes {
		t.Fatalf("header len = %d, want %d", buf.Len(), fileHeaderBytes)
	}
	mode, gen, err := readHeader(&buf)
	if err != nil {
		t.Fatalf("readHeader: %v", err)
	}
	if mode != fileModeEncrypted || gen != 7 {
		t.Fatalf("got mode=%d gen=%d, want 2/7", mode, gen)
	}
}

func TestEncryptedRecordSeqBoundFrame(t *testing.T) {
	s := newFakeSealer()
	rec := Record{Op: OpObjectWriteAt, Seq: 42, Bucket: "b", Key: "k", Payload: []byte("hello")}
	var buf bytes.Buffer
	if err := EncodeEncryptedRecord(&buf, rec, s, "datawal"); err != nil {
		t.Fatalf("encode: %v", err)
	}
	// First 8 bytes are the plaintext seq.
	if got := binary.BigEndian.Uint64(buf.Bytes()[:8]); got != 42 {
		t.Fatalf("plaintext seq = %d, want 42", got)
	}
	got, err := DecodeEncryptedRecord(&buf, s, "datawal", 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Seq != 42 || string(got.Payload) != "hello" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

func TestReadEncryptedFramePrefixOnlyIsTorn(t *testing.T) {
	// 8-byte seq prefix with no following frame must be reported as a torn
	// tail (io.ErrUnexpectedEOF), never a clean io.EOF — otherwise a missing
	// frame on a non-active segment would be silently accepted.
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], 99)
	_, _, err := readEncryptedFrame(bytes.NewReader(prefix[:]))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
	// Zero bytes = clean EOF.
	if _, _, err := readEncryptedFrame(bytes.NewReader(nil)); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestEncryptedRecordWrongNamespaceRejected(t *testing.T) {
	s := newFakeSealer()
	rec := Record{Op: OpObjectWriteAt, Seq: 1, Payload: []byte("x")}
	var buf bytes.Buffer
	if err := EncodeEncryptedRecord(&buf, rec, s, "datawal"); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := DecodeEncryptedRecord(&buf, s, "other-ns", 0); err == nil {
		t.Fatal("expected decode failure under wrong namespace")
	}
}

func TestAppendRollbackAfterPartialWrite(t *testing.T) {
	f := &failingWALFile{failAfter: 10}
	w := &WAL{file: f}

	_, err := w.Append(context.Background(), Record{Op: OpSegmentPut, Payload: []byte("first")})
	require.Error(t, err)
	require.Equal(t, uint64(0), w.lastSeq)
	require.Empty(t, f.data)

	f.failAfter = -1
	seq, err := w.Append(context.Background(), Record{Op: OpSegmentPut, Payload: []byte("second")})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)

	rec, err := DecodeRecord(bytes.NewReader(f.data))
	require.NoError(t, err)
	require.Equal(t, []byte("second"), rec.Payload)
}

func TestAppendTimestampMonotonicWhenClockMovesBackward(t *testing.T) {
	f := &failingWALFile{failAfter: -1}
	w := &WAL{file: f, lastTimestamp: time.Now().Add(time.Hour).UnixNano()}

	_, err := w.Append(context.Background(), Record{Op: OpSegmentPut, Payload: []byte("a")})
	require.NoError(t, err)
	first := w.lastTimestamp
	_, err = w.Append(context.Background(), Record{Op: OpSegmentPut, Payload: []byte("b")})
	require.NoError(t, err)
	require.Greater(t, w.lastTimestamp, first)

	var timestamps []int64
	_, err = scanRecords(bytes.NewReader(f.data), fileModePlain, nil, "datawal", 0, &seqMonotonic{}, true, func(rec Record) error {
		timestamps = append(timestamps, rec.Timestamp)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, timestamps, 2)
	require.Greater(t, timestamps[0], time.Now().UnixNano())
	require.Greater(t, timestamps[1], timestamps[0])
}

func TestAppendReaderStopsWhenContextCanceledAfterPayloadRead(t *testing.T) {
	f := &failingWALFile{failAfter: -1}
	w := &WAL{file: f}
	ctx, cancel := context.WithCancel(context.Background())

	_, err := w.AppendReader(ctx, Record{Op: OpSegmentPut}, &cancelAfterReadReader{cancel: cancel, data: []byte("payload")})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(0), w.lastSeq)
	require.Empty(t, f.data)
}

func TestAppendRejectsSequenceOverflow(t *testing.T) {
	f := &failingWALFile{failAfter: -1}
	w := &WAL{file: f, lastSeq: math.MaxUint64}

	_, err := w.Append(context.Background(), Record{Op: OpSegmentPut})
	require.Error(t, err)
	require.Equal(t, uint64(math.MaxUint64), w.lastSeq)
	require.Empty(t, f.data)
}

func TestAppendRejectsTimestampOverflow(t *testing.T) {
	f := &failingWALFile{failAfter: -1}
	w := &WAL{file: f, lastTimestamp: math.MaxInt64}

	_, err := w.Append(context.Background(), Record{Op: OpSegmentPut})
	require.Error(t, err)
	require.Equal(t, int64(math.MaxInt64), w.lastTimestamp)
	require.Empty(t, f.data)
}

func TestEncryptedRecordRejectsSealedFrameAboveDecodeLimit(t *testing.T) {
	s := newFakeSealer()
	payload := bytes.Repeat([]byte{'p'}, MaxPayloadBytes)
	bucket := strings.Repeat("b", maxRecordBodyBytes-MaxPayloadBytes-recordBodyFixedBytes)

	var buf bytes.Buffer
	err := EncodeEncryptedRecord(&buf, Record{Op: OpSegmentPut, Bucket: bucket, Payload: payload}, s, "datawal")
	require.Error(t, err)
	require.Empty(t, buf.Bytes())
}

func TestNonLatestSegmentTornTailFailsReplayAndOpen(t *testing.T) {
	dir := t.TempDir()
	writePlainSegmentForTest(t, dir, 1, []Record{{Seq: 1, Timestamp: 1, Op: OpSegmentPut, Key: "first"}}, []byte{0x00, 0x00, 0x00, 0x20, 0xaa})
	writePlainSegmentForTest(t, dir, 2, []Record{{Seq: 2, Timestamp: 2, Op: OpSegmentPut, Key: "second"}}, nil)

	err := Replay(context.Background(), dir, 0, nil, "datawal", func(Record) error { return nil })
	require.Error(t, err)

	_, err = Open(dir, nil, "datawal")
	require.Error(t, err)
}

func TestOpenRecoversEmptyLatestSegment(t *testing.T) {
	dir := t.TempDir()
	writePlainSegmentForTest(t, dir, 1, []Record{{Seq: 1, Timestamp: 1, Op: OpSegmentPut, Key: "first"}}, nil)
	require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(2)), nil, 0o644))

	w, err := Open(dir, nil, "datawal")
	require.NoError(t, err)
	_, err = w.Append(context.Background(), Record{Op: OpSegmentPut, Key: "second"})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var keys []string
	require.NoError(t, Replay(context.Background(), dir, 0, nil, "datawal", func(rec Record) error {
		keys = append(keys, rec.Key)
		return nil
	}))
	require.Equal(t, []string{"first", "second"}, keys)
}

func TestOpenRecoversPartialHeaderLatestSegment(t *testing.T) {
	dir := t.TempDir()
	writePlainSegmentForTest(t, dir, 1, []Record{{Seq: 1, Timestamp: 1, Op: OpSegmentPut, Key: "first"}}, nil)
	require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(2)), []byte{0x44, 0x57, 0x41}, 0o644))

	w, err := Open(dir, nil, "datawal")
	require.NoError(t, err)
	_, err = w.Append(context.Background(), Record{Op: OpSegmentPut, Key: "second"})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var keys []string
	require.NoError(t, Replay(context.Background(), dir, 0, nil, "datawal", func(rec Record) error {
		keys = append(keys, rec.Key)
		return nil
	}))
	require.Equal(t, []string{"first", "second"}, keys)
}

func TestNonLatestEmptyOrPartialHeaderSegmentFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "partial", data: []byte{0x44, 0x57, 0x41}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(1)), tc.data, 0o644))
			writePlainSegmentForTest(t, dir, 2, []Record{{Seq: 2, Timestamp: 2, Op: OpSegmentPut, Key: "second"}}, nil)

			err := Replay(context.Background(), dir, 0, nil, "datawal", func(Record) error { return nil })
			require.Error(t, err)

			_, err = Open(dir, nil, "datawal")
			require.Error(t, err)
		})
	}
}

type cancelAfterReadReader struct {
	cancel context.CancelFunc
	data   []byte
	done   bool
}

func (r *cancelAfterReadReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	copy(p, r.data)
	r.cancel()
	return len(r.data), io.EOF
}

type failingWALFile struct {
	data      []byte
	off       int64
	failAfter int
}

func (f *failingWALFile) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (f *failingWALFile) Write(p []byte) (int, error) {
	if f.failAfter >= 0 && int(f.off)+len(p) > f.failAfter {
		n := f.failAfter - int(f.off)
		if n < 0 {
			n = 0
		}
		f.write(p[:n])
		return n, errors.New("injected write failure")
	}
	f.write(p)
	return len(p), nil
}

func (f *failingWALFile) Close() error { return nil }

func (f *failingWALFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.off = offset
	case io.SeekEnd:
		f.off = int64(len(f.data)) + offset
	default:
		return 0, errors.New("unsupported seek")
	}
	return f.off, nil
}

func (f *failingWALFile) Stat() (os.FileInfo, error) {
	return osFileInfo(len(f.data)), nil
}

func (f *failingWALFile) Sync() error { return nil }

func (f *failingWALFile) Truncate(size int64) error {
	f.data = f.data[:size]
	if f.off > size {
		f.off = size
	}
	return nil
}

func (f *failingWALFile) write(p []byte) {
	end := int(f.off) + len(p)
	if end > len(f.data) {
		f.data = append(f.data, make([]byte, end-len(f.data))...)
	}
	copy(f.data[f.off:], p)
	f.off = int64(end)
}

type osFileInfo int

func (o osFileInfo) Name() string       { return "" }
func (o osFileInfo) Size() int64        { return int64(o) }
func (o osFileInfo) Mode() os.FileMode  { return 0 }
func (o osFileInfo) ModTime() time.Time { return time.Time{} }
func (o osFileInfo) IsDir() bool        { return false }
func (o osFileInfo) Sys() any           { return nil }

func writePlainSegmentForTest(t *testing.T, dir string, seq uint64, records []Record, tail []byte) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, segmentName(seq)))
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, writeHeader(f, fileModePlain, 0))
	for _, rec := range records {
		require.NoError(t, EncodeRecord(f, rec))
	}
	_, err = f.Write(tail)
	require.NoError(t, err)
}
