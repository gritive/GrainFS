package nfs4server

import (
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nfs4Client wraps a TCP connection with helpers for building NFS4 compounds.
type nfs4Client struct {
	t    nfsTestTB
	conn net.Conn
	xid  uint32
}

func newNFS4Client(t nfsTestTB, addr string) *nfs4Client {
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
	compound.WriteString("")      // tag
	compound.WriteUint32(0)       // minor version
	compound.WriteUint32(opCount) // op count
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
	r.ReadString() // tag
	r.ReadUint32() // op count
	return status, r
}

func writeLookupLegacyExport(w *XDRWriter) {
	w.WriteUint32(uint32(OpLookup))
	w.WriteString(legacyNFS4Bucket)
}

func writeLookupFile(w *XDRWriter, name string) {
	w.WriteUint32(uint32(OpLookup))
	w.WriteString(name)
}

// writeTestFile creates a file via PUTROOTFH + LOOKUP(export) + OPEN(CREATE) + WRITE + CLOSE.
func (c *nfs4Client) writeTestFile(name string, data []byte) {
	c.t.Helper()
	ops := &XDRWriter{}
	ops.WriteUint32(uint32(OpPutRootFH))
	writeLookupLegacyExport(ops)

	ops.WriteUint32(uint32(OpOpen))
	ops.WriteUint32(0) // seqid
	ops.WriteUint32(2) // OPEN4_SHARE_ACCESS_WRITE
	ops.WriteUint32(0) // OPEN4_SHARE_DENY_NONE
	ops.WriteUint64(1) // owner clientid
	ops.WriteString("owner")
	ops.WriteUint32(1) // opentype = OPEN4_CREATE
	ops.WriteUint32(0) // createmode = UNCHECKED4
	ops.WriteUint32(0) // fattr bitmap len = 0
	ops.WriteOpaque(nil)
	ops.WriteUint32(0) // claim = CLAIM_NULL
	ops.WriteString(name)

	ops.WriteUint32(uint32(OpWrite))
	ops.WriteUint32(1)
	ops.WriteUint64(0)
	ops.WriteUint32(0) // stateid
	ops.WriteUint64(0) // offset
	ops.WriteUint32(2) // FILE_SYNC
	ops.WriteOpaque(data)

	ops.WriteUint32(uint32(OpClose))
	ops.WriteUint32(0)
	ops.WriteUint32(0)
	ops.WriteUint64(0)
	ops.WriteUint32(0) // seqid + stateid

	reply := c.sendCompound(ops.Bytes(), 5)
	status, _ := c.parseCompoundReply(reply)
	assert.Equal(c.t, uint32(NFS4_OK), status, "writeTestFile %q should succeed", name)
}

var _ = Describe("NFS4 integration coverage", func() {
	var (
		t nfsTestTB
		c *nfs4Client
	)

	BeforeEach(func() {
		t = GinkgoT()
		addr, _ := startTestNFS4Server(t)
		c = newNFS4Client(t, addr)
	})

	It("lists files written to the legacy export", func() {
		c.writeTestFile("alpha.txt", []byte("aaa"))
		c.writeTestFile("beta.txt", []byte("bbb"))
		c.writeTestFile("gamma.txt", []byte("ccc"))

		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(ops)
		ops.WriteUint32(uint32(OpReadDir))
		ops.WriteUint64(0)
		ops.WriteUint64(0)
		ops.WriteUint32(4096)
		ops.WriteUint32(4096)
		ops.WriteUint32(0)

		reply := c.sendCompound(ops.Bytes(), 3)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()

		readDirOp, _ := r.ReadUint32()
		assert.Equal(t, uint32(OpReadDir), readDirOp)
		readDirStatus, _ := r.ReadUint32()
		require.Equal(t, uint32(NFS4_OK), readDirStatus)

		r.ReadUint64()
		var names []string
		for {
			follows, err := r.ReadUint32()
			if err != nil || follows == 0 {
				break
			}
			r.ReadUint64()
			name, _ := r.ReadString()
			r.ReadUint32()
			r.ReadOpaque()
			names = append(names, name)
		}

		assert.Contains(t, names, "alpha.txt")
		assert.Contains(t, names, "beta.txt")
		assert.Contains(t, names, "gamma.txt")
	})

	It("propagates requested READDIR attrs", func() {
		content := []byte("hello readdir attrs")
		c.writeTestFile("with-attrs.txt", content)

		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(ops)
		ops.WriteUint32(uint32(OpReadDir))
		ops.WriteUint64(0)
		ops.WriteUint64(0)
		ops.WriteUint32(4096)
		ops.WriteUint32(4096)
		ops.WriteUint32(2)
		ops.WriteUint32(1<<1 | 1<<4)
		ops.WriteUint32(0)

		reply := c.sendCompound(ops.Bytes(), 3)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()

		readDirOp, _ := r.ReadUint32()
		assert.Equal(t, uint32(OpReadDir), readDirOp)
		readDirStatus, _ := r.ReadUint32()
		require.Equal(t, uint32(NFS4_OK), readDirStatus)
		r.ReadUint64()

		for {
			follows, err := r.ReadUint32()
			require.NoError(t, err)
			if follows == 0 {
				break
			}
			r.ReadUint64()
			name, _ := r.ReadString()
			bitmapLen, _ := r.ReadUint32()
			var word0 uint32
			for i := uint32(0); i < bitmapLen; i++ {
				word, _ := r.ReadUint32()
				if i == 0 {
					word0 = word
				}
			}
			attrVals, err := r.ReadOpaque()
			require.NoError(t, err)
			if name != "with-attrs.txt" {
				continue
			}

			require.NotZero(t, word0&(1<<1), "READDIR entry should include requested TYPE attr")
			require.NotZero(t, word0&(1<<4), "READDIR entry should include requested SIZE attr")
			ar := NewXDRReader(attrVals)
			fileType, _ := ar.ReadUint32()
			fileSize, _ := ar.ReadUint64()
			require.Equal(t, uint32(NF4REG), fileType)
			require.Equal(t, uint64(len(content)), fileSize)
			return
		}
		require.Fail(t, "READDIR did not return with-attrs.txt")
	})

	It("returns file GETATTR type and size", func() {
		content := []byte("hello getattr test")
		c.writeTestFile("attrs.txt", content)

		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(ops)
		writeLookupFile(ops, "attrs.txt")
		ops.WriteUint32(uint32(OpGetAttr))
		ops.WriteUint32(2)
		ops.WriteUint32(0x12)
		ops.WriteUint32(0)

		reply := c.sendCompound(ops.Bytes(), 4)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()

		getAttrOp, _ := r.ReadUint32()
		assert.Equal(t, uint32(OpGetAttr), getAttrOp)
		getAttrStatus, _ := r.ReadUint32()
		require.Equal(t, uint32(NFS4_OK), getAttrStatus)

		bitmapLen, _ := r.ReadUint32()
		for range bitmapLen {
			r.ReadUint32()
		}
		attrVals, err := r.ReadOpaque()
		require.NoError(t, err)

		require.GreaterOrEqual(t, len(attrVals), 12)
		attrReader := NewXDRReader(attrVals)
		fileType, _ := attrReader.ReadUint32()
		fileSize, _ := attrReader.ReadUint64()

		assert.Equal(t, uint32(NF4REG), fileType, "should be a regular file")
		assert.Equal(t, uint64(len(content)), fileSize, "size should match content length")
	})

	It("reads from a non-zero offset", func() {
		c.writeTestFile("offset.txt", []byte("hello world"))

		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(ops)
		writeLookupFile(ops, "offset.txt")
		ops.WriteUint32(uint32(OpRead))
		ops.WriteUint32(0)
		ops.WriteUint64(0)
		ops.WriteUint32(0)
		ops.WriteUint64(6)
		ops.WriteUint32(1024)

		reply := c.sendCompound(ops.Bytes(), 4)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()

		readOp, _ := r.ReadUint32()
		assert.Equal(t, uint32(OpRead), readOp)
		readStatus, _ := r.ReadUint32()
		require.Equal(t, uint32(NFS4_OK), readStatus)

		r.ReadUint32()
		data, err := r.ReadOpaque()
		require.NoError(t, err)
		assert.Equal(t, []byte("world"), data)
	})

	It("returns eof for reads beyond EOF", func() {
		c.writeTestFile("short.txt", []byte("hi"))

		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		writeLookupLegacyExport(ops)
		writeLookupFile(ops, "short.txt")
		ops.WriteUint32(uint32(OpRead))
		ops.WriteUint32(0)
		ops.WriteUint64(0)
		ops.WriteUint32(0)
		ops.WriteUint64(0)
		ops.WriteUint32(1024)

		reply := c.sendCompound(ops.Bytes(), 4)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()

		eof, _ := r.ReadUint32()
		data, err := r.ReadOpaque()
		require.NoError(t, err)

		assert.Equal(t, uint32(1), eof, "should be EOF")
		assert.Equal(t, []byte("hi"), data)
	})

	It("echoes requested ACCESS permission bits", func() {
		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		ops.WriteUint32(uint32(OpAccess))
		ops.WriteUint32(0x1F)

		reply := c.sendCompound(ops.Bytes(), 2)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()

		accessOp, _ := r.ReadUint32()
		assert.Equal(t, uint32(OpAccess), accessOp)
		accessStatus, _ := r.ReadUint32()
		require.Equal(t, uint32(NFS4_OK), accessStatus)

		r.ReadUint32()
		access, _ := r.ReadUint32()
		assert.Equal(t, uint32(0x1F), access, "ACCESS should grant all requested bits")
	})

	It("confirms SETCLIENTID", func() {
		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpSetClientID))
		ops.WriteUint64(99999)
		ops.WriteString("confirm-test-client")
		ops.WriteUint32(0)
		ops.WriteString("tcp")
		ops.WriteString("127.0.0.1.0.0")
		ops.WriteUint32(0)

		reply := c.sendCompound(ops.Bytes(), 1)
		status, r := c.parseCompoundReply(reply)
		require.Equal(t, uint32(NFS4_OK), status)

		r.ReadUint32()
		r.ReadUint32()
		clientID, _ := r.ReadUint64()
		r.ReadUint64()

		ops2 := &XDRWriter{}
		ops2.WriteUint32(uint32(OpSetClientIDConfirm))
		ops2.WriteUint64(clientID)
		ops2.WriteUint64(clientID)

		reply2 := c.sendCompound(ops2.Bytes(), 1)
		status2, _ := c.parseCompoundReply(reply2)
		assert.Equal(t, uint32(NFS4_OK), status2)
	})

	It("reuses a retrieved filehandle via PUTFH", func() {
		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpPutRootFH))
		ops.WriteUint32(uint32(OpGetFH))

		reply := c.sendCompound(ops.Bytes(), 2)
		_, r := c.parseCompoundReply(reply)
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		rootFH, err := r.ReadOpaque()
		require.NoError(t, err)
		require.Len(t, rootFH, 16)

		ops2 := &XDRWriter{}
		ops2.WriteUint32(uint32(OpPutFH))
		ops2.WriteOpaque(rootFH)
		ops2.WriteUint32(uint32(OpGetFH))

		reply2 := c.sendCompound(ops2.Bytes(), 2)
		status2, r2 := c.parseCompoundReply(reply2)
		require.Equal(t, uint32(NFS4_OK), status2)

		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		r2.ReadUint32()
		returnedFH, err := r2.ReadOpaque()
		require.NoError(t, err)
		assert.Equal(t, rootFH, returnedFH, "PUTFH + GETFH should return same filehandle")
	})

	It("accepts RENEW lease renewal", func() {
		ops := &XDRWriter{}
		ops.WriteUint32(uint32(OpRenew))
		ops.WriteUint64(12345)

		reply := c.sendCompound(ops.Bytes(), 1)
		status, _ := c.parseCompoundReply(reply)
		assert.Equal(t, uint32(NFS4_OK), status)
	})
})
