package nfs4server

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// FATTR4 bitmap bit positions
const (
	fattr4Size          = 4  // word0 bit 4
	fattr4Mode          = 33 // word1 bit 1
	fattr4TimeModify    = 53 // word1 bit 21 — GETATTR (read-only)
	fattr4TimeModifySet = 54 // word1 bit 22 — SETATTR (writable)
)

// buildSetAttrOp encodes a SETATTR4args op.
func buildSetAttrOp(bm0, bm1 uint32, attrVals []byte) []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpSetAttr))
	// stateid: all zeros (anonymous stateid)
	w.buf.Write(make([]byte, 16))
	// bitmap4<>: 2 words
	w.WriteUint32(2)
	w.WriteUint32(bm0)
	w.WriteUint32(bm1)
	// attrlist4
	w.WriteOpaque(attrVals)
	return w.Bytes()
}

// buildGetAttrOp encodes a GETATTR4args op requesting the given bitmap.
func buildGetAttrOp(bm0, bm1 uint32) []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpGetAttr))
	w.WriteUint32(2)
	w.WriteUint32(bm0)
	w.WriteUint32(bm1)
	return w.Bytes()
}

// createAndWriteFile creates a file at root/name and writes data to it.
func createAndWriteFile(t *testing.T, conn net.Conn, xid uint32, name string, data []byte) []byte {
	t.Helper()
	writeOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(uint32(OpWrite))
		w.buf.Write(make([]byte, 16)) // stateid
		w.WriteUint64(0)              // offset
		w.WriteUint32(2)              // stable = DATA_SYNC4
		w.WriteOpaque(data)
		return w.Bytes()
	}()
	closeOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(uint32(OpClose))
		w.WriteUint32(0)              // seqid
		w.buf.Write(make([]byte, 16)) // stateid
		return w.Bytes()
	}()

	compound := buildCompound40(
		buildPutRootFHOp(),
		buildOpenCreateOp(name),
		buildGetFHOp(),
		writeOp,
		closeOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status, "createAndWriteFile compound failed")

	r.ReadUint32() // opCount
	r.ReadUint32()
	r.ReadUint32() // PUTROOTFH
	r.ReadUint32()
	r.ReadUint32()    // OPEN opcode+status
	skipOpenResult(r) // OPEN result
	r.ReadUint32()
	r.ReadUint32() // GETFH opcode+status
	fh, _ := r.ReadOpaque()
	return fh
}

// readFileSize sends PUTROOTFH + LOOKUP(name) + GETATTR(size) and returns the size.
func readFileSize(t *testing.T, conn net.Conn, xid uint32, name string) uint64 {
	t.Helper()
	getAttrSizeOp := buildGetAttrOp(1<<fattr4Size, 0)
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp(name),
		getAttrSizeOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status, "readFileSize compound failed")

	r.ReadUint32() // opCount
	r.ReadUint32()
	r.ReadUint32() // PUTROOTFH
	r.ReadUint32()
	r.ReadUint32() // LOOKUP
	r.ReadUint32()
	r.ReadUint32() // GETATTR opcode+status
	bLen, _ := r.ReadUint32()
	for i := uint32(0); i < bLen; i++ {
		r.ReadUint32()
	}
	attrBytes, _ := r.ReadOpaque()
	ar := NewXDRReader(attrBytes)
	size, _ := ar.ReadUint64()
	return size
}

// readFileMode returns the mode attribute for root/name via LOOKUP + GETATTR.
func readFileMode(t *testing.T, conn net.Conn, xid uint32, name string) uint32 {
	t.Helper()
	getAttrModeOp := buildGetAttrOp(0, 1<<(fattr4Mode-32))
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp(name),
		getAttrModeOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	bLen, _ := r.ReadUint32()
	for i := uint32(0); i < bLen; i++ {
		r.ReadUint32()
	}
	attrBytes, _ := r.ReadOpaque()
	ar := NewXDRReader(attrBytes)
	mode, _ := ar.ReadUint32()
	return mode
}

// readFileMtime returns TIME_MODIFY (seconds since epoch) for root/name.
func readFileMtime(t *testing.T, conn net.Conn, xid uint32, name string) int64 {
	t.Helper()
	getAttrMtimeOp := buildGetAttrOp(0, 1<<(fattr4TimeModify-32))
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp(name),
		getAttrMtimeOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	bLen, _ := r.ReadUint32()
	for i := uint32(0); i < bLen; i++ {
		r.ReadUint32()
	}
	attrBytes, _ := r.ReadOpaque()
	ar := NewXDRReader(attrBytes)
	secs, _ := ar.ReadUint64()
	return int64(secs)
}

func TestSetAttr_NoFH(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	modeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(0750)
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(0, 1<<(fattr4Mode-32), modeVals)
	compound := buildCompound40(setAttrOp)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(1, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4ERR_NOFILEHANDLE), status)
	r.ReadUint32()
	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpSetAttr), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4ERR_NOFILEHANDLE), opStatus)
}

func TestSetAttr_Mode(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "mode.txt", []byte("hello"))

	modeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(0750)
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(0, 1<<(fattr4Mode-32), modeVals)
	setAttrCompound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("mode.txt"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, setAttrCompound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	got := readFileMode(t, conn, 30, "mode.txt")
	assert.Equal(t, uint32(0750), got)
}

func TestSetAttr_Mtime(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "mtime.txt", []byte("hello"))

	targetSecs := int64(1704067200)

	mtimeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(1) // SET_TO_CLIENT_TIME4
		w.WriteUint32(uint32(targetSecs >> 32))
		w.WriteUint32(uint32(targetSecs))
		w.WriteUint32(0) // nseconds
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(0, 1<<(fattr4TimeModifySet-32), mtimeVals)
	setAttrCompound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("mtime.txt"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, setAttrCompound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	gotSecs := readFileMtime(t, conn, 30, "mtime.txt")
	assert.Equal(t, targetSecs, gotSecs)
}

func TestSetAttr_Truncate(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i & 0xff)
	}
	createAndWriteFile(t, conn, 10, "trunc.bin", data)

	sizeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint64(512)
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(1<<fattr4Size, 0, sizeVals)
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("trunc.bin"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	got := readFileSize(t, conn, 30, "trunc.bin")
	assert.Equal(t, uint64(512), got)
}

func TestSetAttr_TruncateExtend(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "extend.bin", bytes.Repeat([]byte{0xAB}, 100))

	sizeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint64(1024)
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(1<<fattr4Size, 0, sizeVals)
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("extend.bin"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	got := readFileSize(t, conn, 30, "extend.bin")
	assert.Equal(t, uint64(1024), got)

	readOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(uint32(OpRead))
		w.buf.Write(make([]byte, 16)) // stateid
		w.WriteUint64(100)            // offset
		w.WriteUint32(100)            // count
		return w.Bytes()
	}()
	readCompound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("extend.bin"),
		readOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(40, readCompound)))
	readReply, err := readRPCFrame(conn)
	require.NoError(t, err)
	readStatus, rr := parseCompoundReply(t, readReply)
	require.Equal(t, uint32(NFS4_OK), readStatus)
	rr.ReadUint32() // opCount
	rr.ReadUint32()
	rr.ReadUint32() // PUTROOTFH
	rr.ReadUint32()
	rr.ReadUint32() // LOOKUP
	rr.ReadUint32()
	rr.ReadUint32() // READ opcode+status
	rr.ReadUint32() // eof flag
	readData, _ := rr.ReadOpaque()
	assert.Equal(t, 100, len(readData))
	assert.Equal(t, bytes.Repeat([]byte{0}, 100), readData, "zero-extended region should be zeroes")
}

func TestSetAttr_TruncateZero(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	data := make([]byte, 1024*1024)
	createAndWriteFile(t, conn, 10, "zero.bin", data)

	sizeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint64(0)
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(1<<fattr4Size, 0, sizeVals)
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("zero.bin"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	got := readFileSize(t, conn, 30, "zero.bin")
	assert.Equal(t, uint64(0), got)
}

func TestSetAttr_MultiAttr(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "multi.bin", bytes.Repeat([]byte{0xCC}, 1024))

	attrVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint64(512)  // SIZE
		w.WriteUint32(0600) // MODE
		return w.Bytes()
	}()
	bm0 := uint32(1 << fattr4Size)
	bm1 := uint32(1 << (fattr4Mode - 32))
	setAttrOp := buildSetAttrOp(bm0, bm1, attrVals)
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("multi.bin"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	assert.Equal(t, uint64(512), readFileSize(t, conn, 30, "multi.bin"))
	assert.Equal(t, uint32(0600), readFileMode(t, conn, 40, "multi.bin"))
}

func TestSetAttr_Unknown(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "unknown.txt", []byte("hello"))

	setAttrOp := buildSetAttrOp(1<<30, 0, nil)
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("unknown.txt"),
		setAttrOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status, "unknown attrs should be silently ignored")
}

func TestSetAttr_ConcurrentWrite(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn2.Close()

	data := make([]byte, 1024*1024)
	createAndWriteFile(t, conn1, 10, "concurrent.bin", data)

	done := make(chan error, 2)

	go func() {
		sizeVals := func() []byte {
			w := &XDRWriter{}
			w.WriteUint64(512)
			return w.Bytes()
		}()
		op := buildSetAttrOp(1<<fattr4Size, 0, sizeVals)
		compound := buildCompound40(buildPutRootFHOp(), buildLookupOp("concurrent.bin"), op)
		if err := writeRPCFrame(conn1, buildRPCCallFrame(20, compound)); err != nil {
			done <- err
			return
		}
		_, err := readRPCFrame(conn1)
		done <- err
	}()

	go func() {
		writeOp := func() []byte {
			w := &XDRWriter{}
			w.WriteUint32(uint32(OpWrite))
			w.buf.Write(make([]byte, 16))
			w.WriteUint64(0)
			w.WriteUint32(2)
			w.WriteOpaque(make([]byte, 100))
			return w.Bytes()
		}()
		compound := buildCompound40(buildPutRootFHOp(), buildLookupOp("concurrent.bin"), writeOp)
		if err := writeRPCFrame(conn2, buildRPCCallFrame(21, compound)); err != nil {
			done <- err
			return
		}
		_, err := readRPCFrame(conn2)
		done <- err
	}()

	require.NoError(t, <-done)
	require.NoError(t, <-done)

	size := readFileSize(t, conn1, 30, "concurrent.bin")
	assert.True(t, size == 100 || size == 512 || size == 1024*1024,
		"concurrent truncate+write: unexpected size %d", size)
}

func TestSetAttr_PersistAfterRestart(t *testing.T) {
	dir := t.TempDir()

	backend1, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	require.NoError(t, backend1.CreateBucket(context.Background(), nfs4Bucket))
	srv1 := NewServer(backend1)

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv1.mu.Lock()
	srv1.listener = ln1
	srv1.mu.Unlock()
	go func() {
		for {
			c, err := ln1.Accept()
			if err != nil {
				return
			}
			go srv1.handleConn(c)
		}
	}()

	conn1, err := net.DialTimeout("tcp", ln1.Addr().String(), 2*time.Second)
	require.NoError(t, err)

	createAndWriteFile(t, conn1, 10, "persist.txt", []byte("hello"))

	modeVals := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(0750)
		return w.Bytes()
	}()
	setAttrOp := buildSetAttrOp(0, 1<<(fattr4Mode-32), modeVals)
	compound := buildCompound40(buildPutRootFHOp(), buildLookupOp("persist.txt"), setAttrOp)
	require.NoError(t, writeRPCFrame(conn1, buildRPCCallFrame(20, compound)))
	_, err = readRPCFrame(conn1)
	require.NoError(t, err)

	conn1.Close()
	srv1.Close()
	backend1.Close() // release BadgerDB lock so backend2 can open the same dir

	backend2, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	srv2 := NewServer(backend2)
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv2.mu.Lock()
	srv2.listener = ln2
	srv2.mu.Unlock()
	go func() {
		for {
			c, err := ln2.Accept()
			if err != nil {
				return
			}
			go srv2.handleConn(c)
		}
	}()
	t.Cleanup(func() { srv2.Close() })

	conn2, err := net.DialTimeout("tcp", ln2.Addr().String(), 2*time.Second)
	require.NoError(t, err)
	defer conn2.Close()

	got := readFileMode(t, conn2, 30, "persist.txt")
	assert.Equal(t, uint32(0750), got, "mode must survive server restart via sidecar")
}
