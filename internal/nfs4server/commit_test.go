package nfs4server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommit_AfterWrite(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "commit.bin", []byte("hello"))

	commitOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(uint32(OpCommit))
		w.WriteUint64(0) // offset
		w.WriteUint32(0) // count (0 = all)
		return w.Bytes()
	}()
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp("commit.bin"),
		commitOp,
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32() // opCount
	r.ReadUint32()
	r.ReadUint32() // PUTROOTFH
	r.ReadUint32()
	r.ReadUint32() // LOOKUP
	r.ReadUint32()
	r.ReadUint32() // COMMIT opcode+status
	// writeverf4: 8 bytes (two uint32)
	hi, _ := r.ReadUint32()
	lo, _ := r.ReadUint32()
	verfer := uint64(hi)<<32 | uint64(lo)
	assert.NotZero(t, verfer, "writeverf4 must be non-zero after server init")
}

func TestCommit_VerferStable(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	createAndWriteFile(t, conn, 10, "stable.bin", []byte("data"))

	commitOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(uint32(OpCommit))
		w.WriteUint64(0)
		w.WriteUint32(0)
		return w.Bytes()
	}()

	getVerf := func(xid uint32) uint64 {
		compound := buildCompound40(buildPutRootFHOp(), buildLookupOp("stable.bin"), commitOp)
		require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
		reply, err := readRPCFrame(conn)
		require.NoError(t, err)
		_, r := parseCompoundReply(t, reply)
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		r.ReadUint32()
		hi, _ := r.ReadUint32()
		lo, _ := r.ReadUint32()
		return uint64(hi)<<32 | uint64(lo)
	}

	v1 := getVerf(20)
	v2 := getVerf(21)
	assert.Equal(t, v1, v2, "writeverf4 must be stable within a server instance")
}

func TestCommit_NoFH(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	commitOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(uint32(OpCommit))
		w.WriteUint64(0)
		w.WriteUint32(0)
		return w.Bytes()
	}()
	compound := buildCompound40(commitOp) // no PUTROOTFH
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(1, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4ERR_NOFILEHANDLE), status)
	r.ReadUint32()
	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpCommit), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4ERR_NOFILEHANDLE), opStatus)
}
