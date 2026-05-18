package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNBD(t *testing.T) (*Server, net.Conn) {
	t.Helper()
	return setupNBDWithReadIndexer(t, nil)
}

func setupNBDWithReadIndexer(t *testing.T, ri ReadIndexer) (*Server, net.Conn) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	return setupNBDWithBackend(t, backend, ri)
}

func setupNBDWithBackend(t *testing.T, backend storage.Backend, ri ReadIndexer) (*Server, net.Conn) {
	t.Helper()
	mgr := volume.NewManager(backend)
	_, err := mgr.Create("nbd-test", 1024*1024)
	require.NoError(t, err)

	srv := NewServer(mgr, "nbd-test")
	if ri != nil {
		srv.SetReadIndexer(ri)
	}

	// Use a pipe for testing
	client, server := net.Pipe()
	go srv.handleConn(server)

	// Step 1: Read server's newstyle header (18 bytes: magic + opt_magic + flags)
	hdr := make([]byte, 18)
	_, err = io.ReadFull(client, hdr)
	require.NoError(t, err)

	magic := binary.BigEndian.Uint64(hdr[0:8])
	assert.Equal(t, nbdMagic, magic)
	optMagic := binary.BigEndian.Uint64(hdr[8:16])
	assert.Equal(t, nbdOptionMagic, optMagic)

	// Step 2: Send client flags (4 bytes: NBD_FLAG_C_FIXED_NEWSTYLE)
	var clientFlags [4]byte
	binary.BigEndian.PutUint32(clientFlags[:], 1) // NBD_FLAG_C_FIXED_NEWSTYLE
	_, err = client.Write(clientFlags[:])
	require.NoError(t, err)

	// Step 3: Send OPT_EXPORT_NAME to complete handshake
	exportName := []byte("nbd-test")
	optHdr := make([]byte, 16+len(exportName))
	binary.BigEndian.PutUint64(optHdr[0:8], nbdOptionMagic)
	binary.BigEndian.PutUint32(optHdr[8:12], nbdOptExportName)
	binary.BigEndian.PutUint32(optHdr[12:16], uint32(len(exportName)))
	copy(optHdr[16:], exportName)
	_, err = client.Write(optHdr)
	require.NoError(t, err)

	// Step 4: Read export data reply (134 bytes: size + flags + zeros)
	exportReply := make([]byte, 134)
	_, err = io.ReadFull(client, exportReply)
	require.NoError(t, err)

	exportSize := binary.BigEndian.Uint64(exportReply[0:8])
	assert.Equal(t, uint64(1024*1024), exportSize)

	t.Cleanup(func() {
		client.Close()
	})

	return srv, client
}

type asyncCommitTrackingBackend struct {
	*storage.LocalBackend

	blockCommitFns atomic.Int32
}

func setupNBDWithAsyncCommitTracking(t *testing.T) (*asyncCommitTrackingBackend, net.Conn) {
	t.Helper()
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	backend := &asyncCommitTrackingBackend{LocalBackend: local}
	_, conn := setupNBDWithBackend(t, backend, nil)
	return backend, conn
}

func (b *asyncCommitTrackingBackend) PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error) {
	obj, err := b.LocalBackend.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := volume.NameFromBlockKey(key); !ok {
		return obj, func() error { return nil }, nil
	}

	return obj, func() error {
		b.blockCommitFns.Add(1)
		return nil
	}, nil
}

func TestServerClosesConnectionWhenConfiguredVolumeMissing(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	mgr := volume.NewManager(backend)
	srv := NewServer(mgr, "missing")
	client, server := net.Pipe()
	go srv.handleConn(server)
	defer client.Close()

	hdr := make([]byte, 18)
	_, err = io.ReadFull(client, hdr)
	require.ErrorIs(t, err, io.EOF)
}

func TestServerBuffersPoolCommonRequestSizes(t *testing.T) {
	srv := NewServer(nil, "")

	for _, size := range []uint32{nbdPoolBufSize, nbdLargePoolBufSize, nbdHugePoolBufSize} {
		buf := srv.getBuf(size)
		require.Len(t, buf, int(size))
		srv.putBuf(buf)

		reused := srv.getBuf(size)
		require.Len(t, reused, int(size))
		srv.putBuf(reused)
	}

	odd := srv.getBuf(12345)
	require.Len(t, odd, 12345)
	srv.putBuf(odd)
}

type fakeReadIndexer struct {
	readCalls atomic.Int32
	waitCalls atomic.Int32
	readErr   error
	waitErr   error
	index     uint64
}

func (f *fakeReadIndexer) ReadIndex(context.Context) (uint64, error) {
	f.readCalls.Add(1)
	return f.index, f.readErr
}

func (f *fakeReadIndexer) WaitApplied(context.Context, uint64) error {
	f.waitCalls.Add(1)
	return f.waitErr
}

func TestNBDHandshake(t *testing.T) {
	_, _ = setupNBD(t)
	// If we get here, handshake succeeded
}

// sendWriteConn writes data at offset over conn and reads the reply.
func sendWriteConn(t *testing.T, conn net.Conn, offset uint64, data []byte) {
	t.Helper()
	req := make([]byte, 28+len(data))
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdWrite))
	binary.BigEndian.PutUint64(req[8:16], 1)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], uint32(len(data)))
	copy(req[28:], data)
	_, err := conn.Write(req)
	require.NoError(t, err)
	reply := make([]byte, 16)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(reply[4:8]), "write should succeed")
}

// sendFlushConn sends NBD_CMD_FLUSH and reads the reply.
func sendFlushConn(t *testing.T, conn net.Conn) {
	t.Helper()
	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdFlush))
	_, err := conn.Write(req)
	require.NoError(t, err)
	reply := make([]byte, 16)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(reply[4:8]), "flush should succeed")
}

// sendReadConn reads length bytes at offset from conn.
func sendReadConn(t *testing.T, conn net.Conn, offset uint64, length int) []byte {
	t.Helper()
	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdRead))
	binary.BigEndian.PutUint64(req[8:16], 2)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], uint32(length))
	_, err := conn.Write(req)
	require.NoError(t, err)
	reply := make([]byte, 16+length)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(reply[4:8]), "read should succeed")
	return reply[16:]
}

// TestNBDFlushWriteOrdering verifies command-level last-write-wins readback
// across flush using the default local backend.
func TestNBDFlushWriteOrdering(t *testing.T) {
	_, conn := setupNBD(t)
	const blockSize = 4096

	first := make([]byte, blockSize)
	for i := range first {
		first[i] = 0xAA
	}
	second := make([]byte, blockSize)
	for i := range second {
		second[i] = 0xBB
	}

	sendWriteConn(t, conn, 0, first)
	sendWriteConn(t, conn, 0, second)
	sendFlushConn(t, conn)

	got := sendReadConn(t, conn, 0, blockSize)
	require.Equal(t, second, got, "second write must win after flush")
}

func TestNBDFlushRunsDeferredSameBlockDifferentOffsetCommits(t *testing.T) {
	backend, conn := setupNBDWithAsyncCommitTracking(t)
	const blockSize = 4096
	const halfBlockSize = blockSize / 2

	initial := make([]byte, blockSize)
	for i := range initial {
		initial[i] = 0x11
	}
	front := make([]byte, halfBlockSize)
	for i := range front {
		front[i] = 0x22
	}
	back := make([]byte, halfBlockSize)
	for i := range back {
		back[i] = 0x33
	}

	sendWriteConn(t, conn, 0, initial)
	sendWriteConn(t, conn, 0, front)
	sendWriteConn(t, conn, halfBlockSize, back)
	sendFlushConn(t, conn)

	require.GreaterOrEqual(t, int(backend.blockCommitFns.Load()), 2, "flush should run deferred block commits")

	// Scheduler ordering is covered by TestMutationQueueSerializesSameBlockDifferentOffsets
	// and related mutationQueue tests. PutObjectAsync writes local data before FLUSH,
	// so readback here is command-level data smoke rather than ordering evidence.
	got := sendReadConn(t, conn, 0, blockSize)
	require.Equal(t, front, got[:halfBlockSize], "front half must match last front write after flush")
	require.Equal(t, back, got[halfBlockSize:], "back half must match last back write after flush")
}

func TestNBDWriteRead(t *testing.T) {
	_, conn := setupNBD(t)

	// Write request
	data := []byte("Hello NBD!")
	writeReq := make([]byte, 28+len(data))
	binary.BigEndian.PutUint32(writeReq[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(writeReq[4:6], 0) // flags
	binary.BigEndian.PutUint16(writeReq[6:8], uint16(nbdCmdWrite))
	binary.BigEndian.PutUint64(writeReq[8:16], 1)  // handle
	binary.BigEndian.PutUint64(writeReq[16:24], 0) // offset
	binary.BigEndian.PutUint32(writeReq[24:28], uint32(len(data)))
	copy(writeReq[28:], data)

	_, err := conn.Write(writeReq)
	require.NoError(t, err)

	// Read write reply (use io.ReadFull to avoid partial reads on pipe)
	reply := make([]byte, 16)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	assert.Equal(t, nbdReplyMagic, binary.BigEndian.Uint32(reply[0:4]))
	assert.Equal(t, uint32(0), binary.BigEndian.Uint32(reply[4:8]))

	// Read request
	readReq := make([]byte, 28)
	binary.BigEndian.PutUint32(readReq[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(readReq[4:6], 0)
	binary.BigEndian.PutUint16(readReq[6:8], uint16(nbdCmdRead))
	binary.BigEndian.PutUint64(readReq[8:16], 2)  // handle
	binary.BigEndian.PutUint64(readReq[16:24], 0) // offset
	binary.BigEndian.PutUint32(readReq[24:28], uint32(len(data)))

	_, err = conn.Write(readReq)
	require.NoError(t, err)

	// Read reply header + data (use io.ReadFull to avoid partial reads on pipe)
	replyBuf := make([]byte, 16+len(data))
	_, err = io.ReadFull(conn, replyBuf)
	require.NoError(t, err)
	assert.Equal(t, nbdReplyMagic, binary.BigEndian.Uint32(replyBuf[0:4]))
	assert.Equal(t, data, replyBuf[16:])
}

func TestNBDReadUsesReadIndexer(t *testing.T) {
	ri := &fakeReadIndexer{index: 9}
	_, conn := setupNBDWithReadIndexer(t, ri)

	sendReadConn(t, conn, 0, 4)

	assert.Equal(t, int32(1), ri.readCalls.Load())
	assert.Equal(t, int32(1), ri.waitCalls.Load())
}

func TestNBDReadReturnsErrorWhenReadIndexFails(t *testing.T) {
	ri := &fakeReadIndexer{readErr: errors.New("not leader")}
	_, conn := setupNBDWithReadIndexer(t, ri)

	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdRead))
	binary.BigEndian.PutUint64(req[8:16], 2)
	binary.BigEndian.PutUint32(req[24:28], 4)
	_, err := conn.Write(req)
	require.NoError(t, err)

	reply := make([]byte, 16)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	assert.Equal(t, nbdErrEIO, binary.BigEndian.Uint32(reply[4:8]))
	assert.Equal(t, int32(1), ri.readCalls.Load())
	assert.Equal(t, int32(0), ri.waitCalls.Load())
}
