package nfs4server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildCompound40 builds a COMPOUND4args with minorversion=0.
func buildCompound40(ops ...[]byte) []byte {
	w := &XDRWriter{}
	w.WriteString("") // tag
	w.WriteUint32(0)  // minor version = 0
	w.WriteUint32(uint32(len(ops)))
	for _, op := range ops {
		w.buf.Write(op)
	}
	return w.Bytes()
}

func buildLookupOp(name string) []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpLookup))
	w.WriteString(name)
	return w.Bytes()
}

func buildOpenCreateOp(filename string) []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpOpen))
	w.WriteUint32(0)          // seqid
	w.WriteUint32(2)          // OPEN4_SHARE_ACCESS_WRITE
	w.WriteUint32(0)          // OPEN4_SHARE_DENY_NONE
	w.WriteUint64(1)          // owner clientid
	w.WriteString("owner-1") // owner
	w.WriteUint32(1)          // opentype = OPEN4_CREATE
	w.WriteUint32(0)          // createmode = UNCHECKED4
	w.WriteUint32(0)          // fattr bitmap len = 0
	w.WriteOpaque(nil)        // empty attrvals
	w.WriteUint32(0)          // claim = CLAIM_NULL
	w.WriteString(filename)
	return w.Bytes()
}

func buildGetFHOp() []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpGetFH))
	return w.Bytes()
}

func buildSaveFHOp() []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpSaveFH))
	return w.Bytes()
}

func buildRestoreFHOp() []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpRestoreFH))
	return w.Bytes()
}

func buildPutRootFHOp() []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpPutRootFH))
	return w.Bytes()
}

// sendCompound40 sends a v0 COMPOUND and returns the raw reply.
func sendCompound40(t *testing.T, conn net.Conn, xid uint32, ops ...[]byte) []byte {
	t.Helper()
	compound := buildCompound40(ops...)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	return reply
}

// skipOpenResult advances r past an OPEN4resok result.
// opOpen encodes: seqid(4)+stateID(8)+pad(4) + atomic(4)+before(8)+after(8) + rflags(4) + bitmapLen(4) + deleg_type(4).
// stateid is NOT XDR opaque — it is three separate writes with no length prefix.
func skipOpenResult(r *XDRReader) {
	r.ReadUint32()            // seqid
	r.ReadUint64()            // stateID
	r.ReadUint32()            // padding
	r.ReadUint32()            // atomic
	r.ReadUint64()            // before
	r.ReadUint64()            // after
	r.ReadUint32()            // rflags
	bLen, _ := r.ReadUint32() // bitmap_len
	for i := uint32(0); i < bLen; i++ {
		r.ReadUint32()
	}
	r.ReadUint32() // delegation_type
}

func TestSaveFH_RestoreFH(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Single COMPOUND: PUTROOTFH → OPEN("a.txt") → GETFH → SAVEFH → PUTROOTFH → OPEN("b.txt") → RESTOREFH → GETFH
	// savedFH is COMPOUND-scoped (Dispatcher reset on each COMPOUND), so SAVEFH and RESTOREFH must be in the same COMPOUND.
	reply := sendCompound40(t, conn, 1,
		buildPutRootFHOp(),
		buildOpenCreateOp("a.txt"),
		buildGetFHOp(),
		buildSaveFHOp(),
		buildPutRootFHOp(),
		buildOpenCreateOp("b.txt"),
		buildRestoreFHOp(),
		buildGetFHOp(),
	)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32()                 // opCount
	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32(); r.ReadUint32() // OPEN a.txt opcode+status
	skipOpenResult(r)              // OPEN a.txt result
	r.ReadUint32(); r.ReadUint32() // GETFH opcode+status
	fhA, _ := r.ReadOpaque()       // FH of a.txt
	r.ReadUint32(); r.ReadUint32() // SAVEFH
	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32(); r.ReadUint32() // OPEN b.txt opcode+status
	skipOpenResult(r)              // OPEN b.txt result
	r.ReadUint32(); r.ReadUint32() // RESTOREFH
	r.ReadUint32(); r.ReadUint32() // GETFH opcode+status
	fhAfterRestore, _ := r.ReadOpaque()

	assert.Equal(t, fhA, fhAfterRestore, "RESTOREFH should restore saved FH")
}

func TestRestoreFH_WithoutSave(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// PUTROOTFH → RESTOREFH (no SAVEFH before) → must return NFS4ERR_RESTOREFH
	reply := sendCompound40(t, conn, 1,
		buildPutRootFHOp(),
		buildRestoreFHOp(),
	)
	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4ERR_RESTOREFH), status)
	r.ReadUint32()                 // opCount
	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32()                 // RESTOREFH opcode
	restoreStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4ERR_RESTOREFH), restoreStatus)
}

func TestSaveFH_Overwrites(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Single COMPOUND: PUTROOTFH → OPEN("x.txt") → SAVEFH → PUTROOTFH → OPEN("y.txt") → GETFH → SAVEFH → RESTOREFH → GETFH
	// RFC §16.39: RESTOREFH returns the most-recently saved FH (y.txt).
	// SAVEFH and RESTOREFH must be in the same COMPOUND (Dispatcher is per-COMPOUND).
	reply := sendCompound40(t, conn, 1,
		buildPutRootFHOp(),
		buildOpenCreateOp("x.txt"),
		buildSaveFHOp(),
		buildPutRootFHOp(),
		buildOpenCreateOp("y.txt"),
		buildGetFHOp(), // y.txt FH
		buildSaveFHOp(),
		buildRestoreFHOp(),
		buildGetFHOp(), // FH after RESTOREFH
	)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status)

	r.ReadUint32()                 // opCount
	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32(); r.ReadUint32() // OPEN x.txt opcode+status
	skipOpenResult(r)              // OPEN x.txt result
	r.ReadUint32(); r.ReadUint32() // SAVEFH
	r.ReadUint32(); r.ReadUint32() // PUTROOTFH
	r.ReadUint32(); r.ReadUint32() // OPEN y.txt opcode+status
	skipOpenResult(r)              // OPEN y.txt result
	r.ReadUint32(); r.ReadUint32() // GETFH opcode+status
	fhY, _ := r.ReadOpaque()       // y.txt FH
	r.ReadUint32(); r.ReadUint32() // SAVEFH
	r.ReadUint32(); r.ReadUint32() // RESTOREFH
	r.ReadUint32(); r.ReadUint32() // GETFH opcode+status
	fhAfterRestore, _ := r.ReadOpaque()

	assert.Equal(t, fhY, fhAfterRestore, "SAVEFH should overwrite: RESTOREFH returns last saved FH")
}
