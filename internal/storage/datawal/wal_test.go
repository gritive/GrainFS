package datawal_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func TestRejectsInvalidOpOnEncodeDecodeAndReplay(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, datawal.EncodeRecord(&buf, datawal.Record{Op: 0xff, Payload: []byte("bad")}))

	raw := encodeRawPlainRecord(t, 0xff, "", "", "", []byte("bad"))
	_, err := datawal.DecodeRecord(bytes.NewReader(raw))
	require.Error(t, err)

	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Payload: []byte("ok")})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, datawal.AppendRawForTest(dir, raw))

	err = datawal.Replay(context.Background(), dir, 0, nil, func(datawal.Record) error { return nil })
	require.Error(t, err)
}

func TestDecodeRejectsTrailingBytesInCompleteRecord(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, datawal.EncodeRecord(&buf, datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k", Payload: []byte("hello")}))
	raw := append([]byte(nil), buf.Bytes()...)
	bodyLen := int(binary.BigEndian.Uint32(raw[:4]))
	body := append([]byte(nil), raw[4:4+bodyLen]...)
	body = append(body, 0xaa)
	binary.BigEndian.PutUint32(raw[:4], uint32(len(body)))
	raw = raw[:4]
	raw = append(raw, body...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	raw = append(raw, crc[:]...)

	_, err := datawal.DecodeRecord(bytes.NewReader(raw))
	require.Error(t, err)

	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Payload: []byte("ok")})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, datawal.AppendRawForTest(dir, raw))

	err = datawal.Replay(context.Background(), dir, 0, nil, func(datawal.Record) error { return nil })
	require.Error(t, err)
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

type recordingMaterializer struct {
	applied  []datawal.Record
	existing map[string]bool
}

func (m *recordingMaterializer) Materialize(ctx context.Context, rec datawal.Record) error {
	_ = ctx
	m.applied = append(m.applied, rec)
	return nil
}

func (m *recordingMaterializer) HasReplacement(ctx context.Context, rec datawal.Record) (bool, error) {
	_ = ctx
	return m.existing[rec.Target], nil
}

func TestRecoverSkipsExistingReplacementButReplaysPatch(t *testing.T) {
	w, err := datawal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Bucket: "b", Key: "k", Target: "blob", Payload: []byte("full")})
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpObjectWriteAt, Bucket: "b", Key: "k", Offset: 2, Payload: []byte("patch")})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	m := &recordingMaterializer{existing: map[string]bool{"blob": true}}
	require.NoError(t, datawal.Recover(context.Background(), w.Dir(), 0, nil, m))
	require.Len(t, m.applied, 1)
	require.Equal(t, datawal.OpObjectWriteAt, m.applied[0].Op)
}

func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, datawal.SaveCheckpoint(dir, 42))
	seq, err := datawal.LoadCheckpoint(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(42), seq)
}

func TestSaveCheckpointCreatesMissingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	require.NoError(t, datawal.SaveCheckpoint(dir, 99))
	seq, err := datawal.LoadCheckpoint(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(99), seq)
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

func TestAppendReaderRejectsOversizePayload(t *testing.T) {
	w, err := datawal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer w.Close()

	_, err = w.AppendReader(context.Background(), datawal.Record{Op: datawal.OpSegmentPut}, io.LimitReader(zeroReader{}, datawal.MaxPayloadBytes+1))
	require.Error(t, err)
}

func TestOpenScansRecordsForNextSequence(t *testing.T) {
	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	seq1, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Key: "a"})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	w, err = datawal.Open(dir, nil)
	require.NoError(t, err)
	defer w.Close()
	seq2, err := w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Key: "b"})
	require.NoError(t, err)
	require.Equal(t, seq1+1, seq2)
}

func TestOpenTruncatesTornTailBeforeAppending(t *testing.T) {
	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Key: "first", Payload: []byte("1")})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, datawal.AppendRawForTest(dir, []byte{0x00, 0x00, 0x00, 0x20, 0xaa, 0xbb}))

	w, err = datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Key: "second", Payload: []byte("2")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var keys []string
	err = datawal.Replay(context.Background(), dir, 0, nil, func(rec datawal.Record) error {
		keys = append(keys, rec.Key)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, keys)
}

func TestEncryptedWALRejectsNilEncryptor(t *testing.T) {
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x77}, 32))
	require.NoError(t, err)
	dir := t.TempDir()
	w, err := datawal.Open(dir, enc)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Payload: []byte("secret")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	err = datawal.Replay(context.Background(), dir, 0, nil, func(datawal.Record) error { return nil })
	require.Error(t, err)
	_, err = datawal.Open(dir, nil)
	require.Error(t, err)
}

func TestPlainWALRejectsEncryptorMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Payload: []byte("plain")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x77}, 32))
	require.NoError(t, err)
	err = datawal.Replay(context.Background(), dir, 0, enc, func(datawal.Record) error { return nil })
	require.Error(t, err)
	_, err = datawal.Open(dir, enc)
	require.Error(t, err)
}

func TestReplayRejectsMalformedCompleteFrame(t *testing.T) {
	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Payload: []byte("ok")})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var frame bytes.Buffer
	body := make([]byte, 8+8+1+8+8+4)
	binary.BigEndian.PutUint32(body[len(body)-4:], 100)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	frame.Write(hdr[:])
	frame.Write(body)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	frame.Write(crc[:])
	require.NoError(t, datawal.AppendRawForTest(dir, frame.Bytes()))

	err = datawal.Replay(context.Background(), dir, 0, nil, func(datawal.Record) error { return nil })
	require.Error(t, err)
}

func TestEncodeRejectsOversizeMetadata(t *testing.T) {
	var buf bytes.Buffer
	err := datawal.EncodeRecord(&buf, datawal.Record{
		Op:      datawal.OpSegmentPut,
		Bucket:  strings.Repeat("b", 1<<20+1),
		Payload: []byte("ok"),
	})
	require.Error(t, err)
}

func TestDecodeRejectsOversizeAggregateMetadata(t *testing.T) {
	raw := encodeRawPlainRecord(t, byte(datawal.OpSegmentPut), strings.Repeat("b", 600<<10), strings.Repeat("k", 600<<10), "", []byte("ok"))

	_, err := datawal.DecodeRecord(bytes.NewReader(raw))
	require.Error(t, err)

	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Payload: []byte("ok")})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, datawal.AppendRawForTest(dir, raw))

	err = datawal.Replay(context.Background(), dir, 0, nil, func(datawal.Record) error { return nil })
	require.Error(t, err)
}

func TestSegmentDiscoveryRequiresExactName(t *testing.T) {
	dir := t.TempDir()
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	_, err = w.Append(context.Background(), datawal.Record{Op: datawal.OpSegmentPut, Key: "real"})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.NoError(t, os.WriteFile(filepath.Join(dir, "datawal-junk.bin"), []byte("bad"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "datawal-0000000002-extra.bin"), []byte("bad"), 0o644))

	var keys []string
	err = datawal.Replay(context.Background(), dir, 0, nil, func(rec datawal.Record) error {
		keys = append(keys, rec.Key)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"real"}, keys)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func encodeRawPlainRecord(t *testing.T, op byte, bucket, key, target string, payload []byte) []byte {
	t.Helper()
	var body bytes.Buffer
	var fixed [8]byte
	binary.BigEndian.PutUint64(fixed[:], 1)
	body.Write(fixed[:])
	binary.BigEndian.PutUint64(fixed[:], 1)
	body.Write(fixed[:])
	body.WriteByte(op)
	binary.BigEndian.PutUint64(fixed[:], 0)
	body.Write(fixed[:])
	binary.BigEndian.PutUint64(fixed[:], uint64(len(payload)))
	body.Write(fixed[:])
	writeRawString(t, &body, bucket)
	writeRawString(t, &body, key)
	writeRawString(t, &body, target)
	sum := sha256.Sum256(payload)
	body.Write(sum[:])
	binary.BigEndian.PutUint64(fixed[:], uint64(len(payload)))
	body.Write(fixed[:])
	body.Write(payload)

	var frame bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(body.Len()))
	frame.Write(lenBuf[:])
	frame.Write(body.Bytes())
	binary.BigEndian.PutUint32(lenBuf[:], crc32.ChecksumIEEE(body.Bytes()))
	frame.Write(lenBuf[:])
	return frame.Bytes()
}

func writeRawString(t *testing.T, buf *bytes.Buffer, s string) {
	t.Helper()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
	_, err := buf.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = buf.WriteString(s)
	require.NoError(t, err)
}
