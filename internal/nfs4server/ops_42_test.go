package nfs4server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildCompound42 builds a COMPOUND4args with minorversion=2.
func buildCompound42(ops ...[]byte) []byte {
	w := &XDRWriter{}
	w.WriteString("") // tag
	w.WriteUint32(2)  // minor version = 2
	w.WriteUint32(uint32(len(ops)))
	for _, op := range ops {
		w.buf.Write(op)
	}
	return w.Bytes()
}

// writeFileVia41 writes data to path via NFSv4.1 WRITE op on the given connection.
// Returns the xid used (caller provides base xid).
func writeFileVia41(t *testing.T, conn net.Conn, sid SessionID, baseXID uint32, filePath string, data []byte) {
	t.Helper()

	// PUTROOTFH
	putrootOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpPutRootFH)
		return w.Bytes()
	}

	// LOOKUP for each path component (assumes single-level path like "filename")
	lookupOp := func(name string) []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpLookup)
		w.WriteString(name)
		return w.Bytes()
	}

	// CREATE (type=NF4REG=1)
	createOp := func(name string) []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpCreate)
		w.WriteUint32(1) // NF4REG
		w.WriteString(name)
		// fattr4: empty bitmap + empty attrlist
		w.WriteUint32(0) // bitmap length = 0
		w.WriteOpaque(nil)
		return w.Bytes()
	}

	// WRITE (stateid = zeros, stable=FILE_SYNC=2)
	writeOp := func(payload []byte) []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpWrite)
		// stateid: seqid(4)=0 + other(12)=0
		var zero [16]byte
		w.buf.Write(zero[:])
		w.WriteUint64(0) // offset
		w.WriteUint32(2) // FILE_SYNC
		w.WriteOpaque(payload)
		return w.Bytes()
	}

	seqOp := buildSequenceOp(sid, 1, 0, 0, false)
	compound := buildCompound41(seqOp, putrootOp(), createOp(filePath), writeOp(data))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(baseXID, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status, "write setup compound failed")

	// LOOKUP to verify the file exists now
	seqOp2 := buildSequenceOp(sid, 2, 0, 0, false)
	compound2 := buildCompound41(seqOp2, putrootOp(), lookupOp(filePath))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(baseXID+1, compound2)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	status2, _ := parseCompoundReply(t, reply2)
	require.Equal(t, uint32(NFS4_OK), status2, "lookup after write failed")
}

// buildPutRootFHLookup42 builds PUTROOTFH + LOOKUP chain for a single filename in v4.2.
func buildPutRootFHLookup42Ops(name string) [][]byte {
	putrootOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpPutRootFH)
		return w.Bytes()
	}
	lookupOp := func(n string) []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpLookup)
		w.WriteString(n)
		return w.Bytes()
	}
	return [][]byte{putrootOp(), lookupOp(name)}
}

func buildSeekOp42(offset uint64, whence uint32) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpSeek)
	var zero [16]byte
	w.buf.Write(zero[:]) // stateid
	w.WriteUint64(offset)
	w.WriteUint32(whence)
	return w.Bytes()
}

func buildAllocateOp42(offset, length uint64) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpAllocate)
	var zero [16]byte
	w.buf.Write(zero[:]) // stateid
	w.WriteUint64(offset)
	w.WriteUint64(length)
	return w.Bytes()
}

func buildDeallocateOp42(offset, length uint64) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpDeallocate)
	var zero [16]byte
	w.buf.Write(zero[:]) // stateid
	w.WriteUint64(offset)
	w.WriteUint64(length)
	return w.Bytes()
}

func TestSeek_DataWhence(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 700)

	const fname = "seekfile-data"
	payload := make([]byte, 8192)
	writeFileVia41(t, conn, sid, 701, fname, payload)

	// SEEK via minorversion=2: whence=DATA(0), offset=0
	lookupOps := buildPutRootFHLookup42Ops(fname)
	seqOp := buildSequenceOp(sid, 3, 0, 0, false)
	allOps := [][]byte{seqOp}
	allOps = append(allOps, lookupOps...)
	allOps = append(allOps, buildSeekOp42(0, 0))
	compound := buildCompound42(allOps...)

	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(703, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status)

	opCount, _ := r.ReadUint32()
	require.Equal(t, uint32(len(allOps)), opCount)

	// Skip SEQUENCE result
	r.ReadUint32()  // opCode
	r.ReadUint32()  // opStatus
	r.ReadFixed(16) //nolint:errcheck
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()

	// Skip PUTROOTFH result
	r.ReadUint32()
	r.ReadUint32()

	// Skip LOOKUP result
	r.ReadUint32()
	r.ReadUint32()

	// SEEK result
	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpSeek), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), opStatus)
	eof, _ := r.ReadUint32()
	resultOffset, _ := r.ReadUint64()
	assert.Equal(t, uint32(0), eof, "DATA whence: file has data, not EOF")
	assert.Equal(t, uint64(0), resultOffset, "DATA whence: offset should be 0")
}

func TestSeek_HoleWhence(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 710)

	const fname = "seekfile-hole"
	const size = 4096
	payload := make([]byte, size)
	writeFileVia41(t, conn, sid, 711, fname, payload)

	lookupOps := buildPutRootFHLookup42Ops(fname)
	seqOp := buildSequenceOp(sid, 3, 0, 0, false)
	allOps := [][]byte{seqOp}
	allOps = append(allOps, lookupOps...)
	allOps = append(allOps, buildSeekOp42(0, 1)) // whence=HOLE=1

	compound := buildCompound42(allOps...)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(713, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status)

	opCount, _ := r.ReadUint32()
	require.Equal(t, uint32(len(allOps)), opCount)

	// Skip SEQUENCE result
	r.ReadUint32()
	r.ReadUint32()
	r.ReadFixed(16) //nolint:errcheck
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()
	r.ReadUint32()

	// Skip PUTROOTFH
	r.ReadUint32()
	r.ReadUint32()

	// Skip LOOKUP
	r.ReadUint32()
	r.ReadUint32()

	// SEEK result: HOLE whence → GrainFS reports EOF at fileSize
	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpSeek), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), opStatus)
	eof, _ := r.ReadUint32()
	resultOffset, _ := r.ReadUint64()
	assert.Equal(t, uint32(1), eof, "HOLE whence: no holes → EOF=1")
	assert.Equal(t, uint64(size), resultOffset, "HOLE whence: offset should equal file size")
}

func TestAllocate_NoOp(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 720)

	fname := fmt.Sprintf("allocfile-%d", 720)
	writeFileVia41(t, conn, sid, 721, fname, make([]byte, 1024))

	lookupOps := buildPutRootFHLookup42Ops(fname)
	seqOp := buildSequenceOp(sid, 3, 0, 0, false)
	allOps := [][]byte{seqOp}
	allOps = append(allOps, lookupOps...)
	allOps = append(allOps, buildAllocateOp42(0, 4096))

	compound := buildCompound42(allOps...)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(723, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, _ := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status)
}

func TestAllocate_UsesTruncateBackend(t *testing.T) {
	backend := &allocateTruncateBackend{size: 1024}
	d := &Dispatcher{
		backend:     backend,
		state:       NewStateManager(),
		currentPath: "/alloc-fast.bin",
	}

	result := d.opAllocate(buildAllocateArgs42(0, 4096))

	require.Equal(t, NFS4_OK, result.Status)
	require.Equal(t, 1, backend.truncateCalls)
	assert.Equal(t, nfs4Bucket, backend.truncateBucket)
	assert.Equal(t, "alloc-fast.bin", backend.truncateKey)
	assert.Equal(t, int64(4096), backend.truncateSize)
	assert.Zero(t, backend.getCalls, "ALLOCATE with Truncatable backend should not read the object")
	assert.Zero(t, backend.putCalls, "ALLOCATE with Truncatable backend should not rewrite the object")
}

func TestDeallocate_ZeroRange(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 730)

	fname := fmt.Sprintf("deallocfile-%d", 730)
	payload := []byte("hello world abcdefghij")
	writeFileVia41(t, conn, sid, 731, fname, payload)

	lookupOps := buildPutRootFHLookup42Ops(fname)
	seqOp := buildSequenceOp(sid, 3, 0, 0, false)
	allOps := [][]byte{seqOp}
	allOps = append(allOps, lookupOps...)
	allOps = append(allOps, buildDeallocateOp42(0, uint64(len(payload))))

	compound := buildCompound42(allOps...)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(733, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, _ := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status)
}

func buildAllocateArgs42(offset, length uint64) []byte {
	w := &XDRWriter{}
	var zero [16]byte
	w.buf.Write(zero[:]) // stateid
	w.WriteUint64(offset)
	w.WriteUint64(length)
	return w.Bytes()
}

type allocateTruncateBackend struct {
	size int64

	getCalls       int
	putCalls       int
	truncateCalls  int
	truncateBucket string
	truncateKey    string
	truncateSize   int64
}

func (b *allocateTruncateBackend) CreateBucket(string) error { return nil }
func (b *allocateTruncateBackend) HeadBucket(string) error   { return nil }
func (b *allocateTruncateBackend) DeleteBucket(string) error { return nil }
func (b *allocateTruncateBackend) ListBuckets() ([]string, error) {
	return nil, nil
}
func (b *allocateTruncateBackend) PutObject(string, string, io.Reader, string) (*storage.Object, error) {
	b.putCalls++
	return nil, errors.New("unexpected PutObject")
}
func (b *allocateTruncateBackend) GetObject(string, string) (io.ReadCloser, *storage.Object, error) {
	b.getCalls++
	return nil, nil, errors.New("unexpected GetObject")
}
func (b *allocateTruncateBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	return &storage.Object{Key: key, Size: b.size}, nil
}
func (b *allocateTruncateBackend) DeleteObject(string, string) error { return nil }
func (b *allocateTruncateBackend) ListObjects(string, string, int) ([]*storage.Object, error) {
	return nil, nil
}
func (b *allocateTruncateBackend) WalkObjects(string, string, func(*storage.Object) error) error {
	return nil
}
func (b *allocateTruncateBackend) CreateMultipartUpload(string, string, string) (*storage.MultipartUpload, error) {
	return nil, nil
}
func (b *allocateTruncateBackend) UploadPart(string, string, string, int, io.Reader) (*storage.Part, error) {
	return nil, nil
}
func (b *allocateTruncateBackend) CompleteMultipartUpload(string, string, string, []storage.Part) (*storage.Object, error) {
	return nil, nil
}
func (b *allocateTruncateBackend) AbortMultipartUpload(string, string, string) error {
	return nil
}
func (b *allocateTruncateBackend) Truncate(bucket, key string, size int64) error {
	b.truncateCalls++
	b.truncateBucket = bucket
	b.truncateKey = key
	b.truncateSize = size
	b.size = size
	return nil
}
