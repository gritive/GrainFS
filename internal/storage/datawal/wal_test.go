package datawal_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

func TestRecordRoundTrip(t *testing.T) {
	rec := datawal.Record{
		Seq:       42,
		Timestamp: 99,
		Op:        datawal.OpSegmentPut,
		Bucket:    "b",
		Key:       "k",
		Target:    "blob-1",
		Offset:    7,
		Size:      5,
		Payload:   []byte("hello"),
	}
	var buf bytes.Buffer
	require.NoError(t, datawal.EncodeRecord(&buf, rec))
	got, err := datawal.DecodeRecord(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, rec.Seq, got.Seq)
	require.Equal(t, rec.Timestamp, got.Timestamp)
	require.Equal(t, rec.Op, got.Op)
	require.Equal(t, rec.Bucket, got.Bucket)
	require.Equal(t, rec.Key, got.Key)
	require.Equal(t, rec.Target, got.Target)
	require.Equal(t, rec.Offset, got.Offset)
	require.Equal(t, rec.Size, got.Size)
	require.Equal(t, rec.Payload, got.Payload)
	require.NotEmpty(t, got.Checksum)
}

func TestWALAppendFlushReplay(t *testing.T) {
	w, err := datawal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Append(context.Background(), datawal.Record{
		Op: datawal.OpObjectWriteAt, Bucket: "b", Key: "k", Offset: 3, Payload: []byte("abc"),
	})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	var got []datawal.Record
	err = datawal.Replay(context.Background(), w.Dir(), 0, nil, func(rec datawal.Record) error {
		got = append(got, rec)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, datawal.OpObjectWriteAt, got[0].Op)
	require.Equal(t, []byte("abc"), got[0].Payload)
}

func TestWALEncryptedRoundTrip(t *testing.T) {
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x77}, 32))
	require.NoError(t, err)
	w, err := datawal.Open(t.TempDir(), enc)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "secret", Payload: []byte("plaintext")})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	var got []datawal.Record
	err = datawal.Replay(context.Background(), w.Dir(), 0, enc, func(rec datawal.Record) error {
		got = append(got, rec)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []byte("plaintext"), got[0].Payload)

	raw, err := os.ReadFile(filepath.Join(w.Dir(), "datawal-0000000001.bin"))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "plaintext")

	wrong, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x78}, 32))
	require.NoError(t, err)
	err = datawal.Replay(context.Background(), w.Dir(), 0, wrong, func(datawal.Record) error { return nil })
	require.Error(t, err)
}

func TestReplayStopsAtCleanEOFForTruncatedTail(t *testing.T) {
	w, err := datawal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpShardPut, Bucket: "b", Key: "k", Target: "0", Payload: []byte("ok")})
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	err = datawal.AppendRawForTest(w.Dir(), []byte{0x99, 0x88})
	require.NoError(t, err)

	var count int
	err = datawal.Replay(context.Background(), w.Dir(), 0, nil, func(datawal.Record) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestDecodeRejectsChecksumMismatch(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, datawal.EncodeRecord(&buf, datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k", Payload: []byte("hello")}))
	raw := buf.Bytes()
	raw[len(raw)-1] ^= 0xff
	_, err := datawal.DecodeRecord(bytes.NewReader(raw))
	require.ErrorIs(t, err, datawal.ErrChecksumMismatch)
}

func TestAppendCopiesPayload(t *testing.T) {
	w, err := datawal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer w.Close()
	payload := []byte("abc")
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k", Payload: payload})
	require.NoError(t, err)
	payload[0] = 'z'
	require.NoError(t, w.Flush())

	err = datawal.Replay(context.Background(), w.Dir(), 0, nil, func(rec datawal.Record) error {
		require.Equal(t, []byte("abc"), rec.Payload)
		return nil
	})
	require.NoError(t, err)
}

func TestReplayFromCheckpointSkipsOldRecords(t *testing.T) {
	w, err := datawal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer w.Close()
	seq1, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "a", Payload: []byte("1")})
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "b", Payload: []byte("2")})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	var keys []string
	err = datawal.Replay(context.Background(), w.Dir(), seq1, nil, func(rec datawal.Record) error {
		keys = append(keys, rec.Key)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, keys)
}

func TestRecordReaderStreamsPayload(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, datawal.EncodeRecord(&buf, datawal.Record{Op: datawal.OpShardPut, Bucket: "b", Key: "k", Payload: []byte("payload")}))
	rec, err := datawal.DecodeRecord(&buf)
	require.NoError(t, err)
	r := datawal.PayloadReader(rec)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}
