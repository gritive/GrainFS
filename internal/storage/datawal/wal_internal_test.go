package datawal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

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
	_, err = scanRecords(bytes.NewReader(f.data), fileModePlain, nil, func(rec Record) error {
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
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x77}, 32))
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{'p'}, MaxPayloadBytes)
	bucket := strings.Repeat("b", maxRecordBodyBytes-MaxPayloadBytes-recordBodyFixedBytes)

	var buf bytes.Buffer
	err = EncodeEncryptedRecord(&buf, Record{Op: OpSegmentPut, Bucket: bucket, Payload: payload}, enc)
	require.Error(t, err)
	require.Empty(t, buf.Bytes())
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
