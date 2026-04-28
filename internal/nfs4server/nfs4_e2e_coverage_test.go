package nfs4server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nfs4Client wraps a TCP connection with helpers for building NFS4 compounds.
type nfs4Client struct {
	t    *testing.T
	conn net.Conn
	xid  uint32
}

func newNFS4Client(t *testing.T, addr string) *nfs4Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &nfs4Client{t: t, conn: conn}
}

func (c *nfs4Client) sendCompound(ops []byte, opCount uint32) []byte {
	c.t.Helper()
	c.xid++

	compound := &XDRWriter{}
	compound.WriteString("")       // tag
	compound.WriteUint32(0)        // minor version
	compound.WriteUint32(opCount)  // op count
	compound.buf.Write(ops)

	frame := buildRPCCallFrame(c.xid, compound.Bytes())
	require.NoError(c.t, writeRPCFrame(c.conn, frame))

	reply, err := readRPCFrame(c.conn)
	require.NoError(c.t, err)
	return reply
}

func (c *nfs4Client) parseCompoundReply(reply []byte) (status uint32, results *XDRReader) {
	c.t.Helper()
	r := NewXDRReader(reply)
	r.ReadUint32() // XID
	r.ReadUint32() // msg_type
	r.ReadUint32() // reply_stat
	r.ReadUint32() // verf flavor
	r.ReadOpaque() // verf body
	r.ReadUint32() // accept_stat
	status, _ = r.ReadUint32()
	r.ReadString()  // tag
	r.ReadUint32()  // op count
	return status, r
}

// writeTestFile creates a file via PUTROOTFH + OPEN(CREATE) + WRITE + CLOSE.
func (c *nfs4Client) writeTestFile(name string, data []byte) {
	c.t.Helper()
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))

	ops.WriteUint32(uint32(OpOpen))
	ops.WriteUint32(0)              // seqid
	ops.WriteUint32(2)              // OPEN4_SHARE_ACCESS_WRITE
	ops.WriteUint32(0)              // OPEN4_SHARE_DENY_NONE
	ops.WriteUint64(1)              // owner clientid
	ops.WriteString("owner")
	ops.WriteUint32(1)              // opentype = OPEN4_CREATE
	ops.WriteUint32(0)              // createmode = UNCHECKED4
	ops.WriteUint32(0)              // fattr bitmap len = 0
	ops.WriteOpaque(nil)
	ops.WriteUint32(0)              // claim = CLAIM_NULL
	ops.WriteString(name)

	ops.WriteUint32(uint32(OpWrite))
	ops.WriteUint32(1); ops.WriteUint64(0); ops.WriteUint32(0) // stateid
	ops.WriteUint64(0)              // offset
	ops.WriteUint32(2)              // FILE_SYNC
	ops.WriteOpaque(data)

	ops.WriteUint32(uint32(OpClose))
	ops.WriteUint32(0); ops.WriteUint32(0); ops.WriteUint64(0); ops.WriteUint32(0) // seqid + stateid

	reply := c.sendCompound(ops.Bytes(), 4)
	status, _ := c.parseCompoundReply(reply)
	assert.Equal(c.t, uint32(NFS4_OK), status, "writeTestFile %q should succeed", name)
}

// TestE2E_ReadDir verifies that files written to the root appear in READDIR output.
func TestE2E_ReadDir(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	// Write 3 files
	c.writeTestFile("alpha.txt", []byte("aaa"))
	c.writeTestFile("beta.txt", []byte("bbb"))
	c.writeTestFile("gamma.txt", []byte("ccc"))

	// PUTROOTFH + READDIR
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpReadDir))
	ops.WriteUint64(0)    // cookie = 0 (start)
	ops.WriteUint64(0) // cookieverf (8 bytes)
	ops.WriteUint32(4096) // dircount
	ops.WriteUint32(4096) // maxcount
	ops.WriteUint32(0)    // attr bitmap len = 0

	reply := c.sendCompound(ops.Bytes(), 2)
	status, r := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status)

	// Skip PUTROOTFH result
	r.ReadUint32(); r.ReadUint32()

	// Parse READDIR result
	readDirOp, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpReadDir), readDirOp)
	readDirStatus, _ := r.ReadUint32()
	require.Equal(t, uint32(NFS4_OK), readDirStatus)

	// Parse cookieverf (8 bytes) + entries
	r.ReadUint64() // cookieverf

	var names []string
	for {
		follows, err := r.ReadUint32()
		if err != nil || follows == 0 {
			break
		}
		r.ReadUint64()        // cookie
		name, _ := r.ReadString()
		r.ReadUint32()        // bitmap len (0)
		r.ReadOpaque()        // attrvals (empty)
		names = append(names, name)
	}

	assert.Contains(t, names, "alpha.txt")
	assert.Contains(t, names, "beta.txt")
	assert.Contains(t, names, "gamma.txt")
}

// TestE2E_GetAttr_File verifies that GETATTR returns correct type and size.
func TestE2E_GetAttr_File(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	content := []byte("hello getattr test")
	c.writeTestFile("attrs.txt", content)

	// PUTROOTFH + LOOKUP + GETATTR
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpLookup))
	ops.WriteString("attrs.txt")
	ops.WriteUint32(uint32(OpGetAttr))
	ops.WriteUint32(2)     // bitmap len = 2
	ops.WriteUint32(0x12)  // request type (bit1) + size (bit4)
	ops.WriteUint32(0)

	reply := c.sendCompound(ops.Bytes(), 3)
	status, r := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status)

	// Skip PUTROOTFH + LOOKUP results
	r.ReadUint32(); r.ReadUint32()
	r.ReadUint32(); r.ReadUint32()

	// Parse GETATTR result
	getAttrOp, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpGetAttr), getAttrOp)
	getAttrStatus, _ := r.ReadUint32()
	require.Equal(t, uint32(NFS4_OK), getAttrStatus)

	// Parse: bitmap + attrvals
	bitmapLen, _ := r.ReadUint32()
	for range bitmapLen {
		r.ReadUint32()
	}
	attrVals, err := r.ReadOpaque()
	require.NoError(t, err)

	// attrVals contains: type(4) + size(8)
	require.GreaterOrEqual(t, len(attrVals), 12)
	attrReader := NewXDRReader(attrVals)
	fileType, _ := attrReader.ReadUint32()
	fileSize, _ := attrReader.ReadUint64()

	assert.Equal(t, uint32(NF4REG), fileType, "should be a regular file")
	assert.Equal(t, uint64(len(content)), fileSize, "size should match content length")
}

// TestE2E_ReadAtOffset verifies partial reads from a non-zero offset.
func TestE2E_ReadAtOffset(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	c.writeTestFile("offset.txt", []byte("hello world"))

	// PUTROOTFH + LOOKUP + READ at offset 6
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpLookup))
	ops.WriteString("offset.txt")
	ops.WriteUint32(uint32(OpRead))
	ops.WriteUint32(0); ops.WriteUint64(0); ops.WriteUint32(0) // stateid
	ops.WriteUint64(6)    // offset = 6
	ops.WriteUint32(1024) // count

	reply := c.sendCompound(ops.Bytes(), 3)
	status, r := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status)

	// Skip PUTROOTFH + LOOKUP results
	r.ReadUint32(); r.ReadUint32()
	r.ReadUint32(); r.ReadUint32()

	readOp, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpRead), readOp)
	readStatus, _ := r.ReadUint32()
	require.Equal(t, uint32(NFS4_OK), readStatus)

	r.ReadUint32() // eof
	data, err := r.ReadOpaque()
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), data)
}

// TestE2E_ReadBeyondEOF verifies that reading past EOF returns eof=true.
func TestE2E_ReadBeyondEOF(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	c.writeTestFile("short.txt", []byte("hi"))

	// PUTROOTFH + LOOKUP + READ with count > file size
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpLookup))
	ops.WriteString("short.txt")
	ops.WriteUint32(uint32(OpRead))
	ops.WriteUint32(0); ops.WriteUint64(0); ops.WriteUint32(0) // stateid
	ops.WriteUint64(0)    // offset = 0
	ops.WriteUint32(1024) // count = 1024 (much larger than file)

	reply := c.sendCompound(ops.Bytes(), 3)
	status, r := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32(); r.ReadUint32() // LOOKUP
	r.ReadUint32()                 // READ op code
	r.ReadUint32()                 // READ status

	eof, _ := r.ReadUint32()
	data, err := r.ReadOpaque()
	require.NoError(t, err)

	assert.Equal(t, uint32(1), eof, "should be EOF")
	assert.Equal(t, []byte("hi"), data)
}

// TestE2E_Access verifies that ACCESS echoes back the requested permission bits.
func TestE2E_Access(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpAccess))
	ops.WriteUint32(0x1F) // REQUEST_ALL

	reply := c.sendCompound(ops.Bytes(), 2)
	status, r := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32(); r.ReadUint32() // PUTROOTFH

	accessOp, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpAccess), accessOp)
	accessStatus, _ := r.ReadUint32()
	require.Equal(t, uint32(NFS4_OK), accessStatus)

	r.ReadUint32() // supported
	access, _ := r.ReadUint32()
	assert.Equal(t, uint32(0x1F), access, "ACCESS should grant all requested bits")
}

// TestE2E_SetClientIDConfirm verifies the full SETCLIENTID → SETCLIENTIDCONFIRM sequence.
func TestE2E_SetClientIDConfirm(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	// Step 1: SETCLIENTID
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpSetClientID))
	ops.WriteUint64(99999) // verifier
	ops.WriteString("confirm-test-client")
	ops.WriteUint32(0)     // cb_program
	ops.WriteString("tcp") // netid
	ops.WriteString("127.0.0.1.0.0") // addr
	ops.WriteUint32(0)     // callback_ident

	reply := c.sendCompound(ops.Bytes(), 1)
	status, r := c.parseCompoundReply(reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32(); r.ReadUint32() // SETCLIENTID op code + status
	clientID, _ := r.ReadUint64()
	r.ReadUint64() // confirm verifier

	// Step 2: SETCLIENTIDCONFIRM
	ops2 := &XDRWriter{}
	ops2.WriteUint32(uint32(OpSetClientIDConfirm))
	ops2.WriteUint64(clientID) // clientid
	ops2.WriteUint64(clientID) // confirm verifier (same as clientid in our impl)

	reply2 := c.sendCompound(ops2.Bytes(), 1)
	status2, _ := c.parseCompoundReply(reply2)
	assert.Equal(t, uint32(NFS4_OK), status2)
}

// TestE2E_PutFH_GetFH verifies that a retrieved filehandle can be re-used via PUTFH.
func TestE2E_PutFH_GetFH(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	// Step 1: Get root FH
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	ops.WriteUint32(uint32(OpGetFH))

	reply := c.sendCompound(ops.Bytes(), 2)
	_, r := c.parseCompoundReply(reply)
	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32(); r.ReadUint32() // GETFH op + status
	rootFH, err := r.ReadOpaque()
	require.NoError(t, err)
	require.Len(t, rootFH, 16)

	// Step 2: Use PUTFH with the retrieved filehandle + GETFH to verify same FH
	ops2 := &XDRWriter{}
	ops2.WriteUint32(uint32(OpPutFH))
	ops2.WriteOpaque(rootFH)
	ops2.WriteUint32(uint32(OpGetFH))

	reply2 := c.sendCompound(ops2.Bytes(), 2)
	status2, r2 := c.parseCompoundReply(reply2)
	require.Equal(t, uint32(NFS4_OK), status2)

	r2.ReadUint32(); r2.ReadUint32() // PUTFH
	r2.ReadUint32(); r2.ReadUint32() // GETFH op + status
	returnedFH, err := r2.ReadOpaque()
	require.NoError(t, err)
	assert.Equal(t, rootFH, returnedFH, "PUTFH + GETFH should return same filehandle")
}

// TestE2E_Renew verifies RENEW operation is accepted (lease renewal).
func TestE2E_Renew(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	c := newNFS4Client(t, addr)

	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpRenew))
	ops.WriteUint64(12345) // clientid

	reply := c.sendCompound(ops.Bytes(), 1)
	status, _ := c.parseCompoundReply(reply)
	assert.Equal(t, uint32(NFS4_OK), status)
}
