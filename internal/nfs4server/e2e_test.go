package nfs4server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func startTestNFS4Server(t *testing.T) (string, *Server) {
	t.Helper()

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	// Create the NFS4 bucket
	require.NoError(t, backend.CreateBucket(nfs4Bucket))

	srv := NewServer(backend)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv.mu.Lock()
	srv.listener = ln
	srv.mu.Unlock()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()

	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String(), srv
}

func TestE2E_NullProcedure(t *testing.T) {
	addr, _ := startTestNFS4Server(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Build NULL RPC call (procedure 0)
	w := &XDRWriter{}
	w.WriteUint32(1)          // XID
	w.WriteUint32(rpcMsgCall) // CALL
	w.WriteUint32(2)          // RPC version
	w.WriteUint32(rpcProgNFS) // program
	w.WriteUint32(rpcVersNFS4) // version
	w.WriteUint32(0)          // procedure = NULL
	w.WriteUint32(authNone)   // cred flavor
	w.WriteUint32(0)          // cred body len
	w.WriteUint32(authNone)   // verf flavor
	w.WriteUint32(0)          // verf body len

	require.NoError(t, writeRPCFrame(conn, w.Bytes()))

	// Read reply
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	assert.True(t, len(reply) >= 24, "reply should have RPC header")

	// Parse XID from reply
	r := NewXDRReader(reply)
	xid, _ := r.ReadUint32()
	assert.Equal(t, uint32(1), xid)
}

func TestE2E_CompoundPutRootFH_GetFH(t *testing.T) {
	addr, _ := startTestNFS4Server(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Build COMPOUND: PUTROOTFH + GETFH
	compound := &XDRWriter{}
	compound.WriteString("")  // tag
	compound.WriteUint32(0)   // minor version
	compound.WriteUint32(2)   // 2 ops
	compound.WriteUint32(uint32(OpPutRootFH))
	compound.WriteUint32(uint32(OpGetFH))

	// Wrap in RPC call
	rpc := &XDRWriter{}
	rpc.WriteUint32(2)           // XID
	rpc.WriteUint32(rpcMsgCall)
	rpc.WriteUint32(2)           // RPC version
	rpc.WriteUint32(rpcProgNFS)
	rpc.WriteUint32(rpcVersNFS4)
	rpc.WriteUint32(1)           // procedure = COMPOUND
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.buf.Write(compound.Bytes())

	require.NoError(t, writeRPCFrame(conn, rpc.Bytes()))

	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	// Parse reply: XID + reply header + COMPOUND4res
	r := NewXDRReader(reply)
	xid, _ := r.ReadUint32()
	assert.Equal(t, uint32(2), xid)

	// Skip RPC reply header (msg_type + reply_stat + verf + accept_stat)
	r.ReadUint32() // msg_type = REPLY
	r.ReadUint32() // reply_stat = MSG_ACCEPTED
	r.ReadUint32() // verf flavor
	r.ReadOpaque() // verf body
	r.ReadUint32() // accept_stat = SUCCESS

	// COMPOUND4res: status + tag + results
	status, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), status)

	tag, _ := r.ReadString()
	assert.Equal(t, "", tag)

	opCount, _ := r.ReadUint32()
	assert.Equal(t, uint32(2), opCount)

	// Result 1: PUTROOTFH
	op1Code, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpPutRootFH), op1Code)
	op1Status, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), op1Status)

	// Result 2: GETFH
	op2Code, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpGetFH), op2Code)
	op2Status, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), op2Status)

	// GETFH returns a filehandle opaque
	fh, _ := r.ReadOpaque()
	assert.Equal(t, 16, len(fh), "filehandle should be 16 bytes")
}

func TestE2E_CompoundSetClientID(t *testing.T) {
	addr, _ := startTestNFS4Server(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Build COMPOUND: SETCLIENTID
	compound := &XDRWriter{}
	compound.WriteString("")  // tag
	compound.WriteUint32(0)   // minor version
	compound.WriteUint32(1)   // 1 op
	compound.WriteUint32(uint32(OpSetClientID))
	// args: verifier(8) + id(opaque) + callback(program+netid+addr) + callback_ident
	compound.WriteUint64(12345) // verifier
	compound.WriteString("test-client") // id
	compound.WriteUint32(0)    // cb_program
	compound.WriteString("tcp") // netid
	compound.WriteString("127.0.0.1.0.0") // addr
	compound.WriteUint32(0)    // callback_ident

	rpc := &XDRWriter{}
	rpc.WriteUint32(3)
	rpc.WriteUint32(rpcMsgCall)
	rpc.WriteUint32(2)
	rpc.WriteUint32(rpcProgNFS)
	rpc.WriteUint32(rpcVersNFS4)
	rpc.WriteUint32(1) // COMPOUND
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.buf.Write(compound.Bytes())

	require.NoError(t, writeRPCFrame(conn, rpc.Bytes()))

	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	r := NewXDRReader(reply)
	xid, _ := r.ReadUint32()
	assert.Equal(t, uint32(3), xid)

	// Skip RPC header
	r.ReadUint32() // msg_type
	r.ReadUint32() // reply_stat
	r.ReadUint32() // verf flavor
	r.ReadOpaque() // verf body
	r.ReadUint32() // accept_stat

	// COMPOUND status
	status, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), status)
}

func TestE2E_WriteAndReadFile(t *testing.T) {
	addr, _ := startTestNFS4Server(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Step 1: PUTROOTFH + OPEN(CREATE) "test.txt" + WRITE + CLOSE
	compound := &XDRWriter{}
	compound.WriteString("")  // tag
	compound.WriteUint32(0)   // minor version
	compound.WriteUint32(4)   // 4 ops: PUTROOTFH, OPEN, WRITE, CLOSE

	// PUTROOTFH
	compound.WriteUint32(uint32(OpPutRootFH))

	// OPEN (CREATE)
	compound.WriteUint32(uint32(OpOpen))
	compound.WriteUint32(0)  // seqid
	compound.WriteUint32(2)  // OPEN4_SHARE_ACCESS_WRITE
	compound.WriteUint32(0)  // OPEN4_SHARE_DENY_NONE
	compound.WriteUint64(1)  // owner clientid
	compound.WriteString("owner-1") // owner
	compound.WriteUint32(1)  // opentype = OPEN4_CREATE
	compound.WriteUint32(0)  // createmode = UNCHECKED4
	compound.WriteUint32(0)  // fattr bitmap len = 0
	compound.WriteOpaque(nil) // empty attrvals
	compound.WriteUint32(0)  // claim = CLAIM_NULL
	compound.WriteString("test.txt") // filename

	// WRITE
	compound.WriteUint32(uint32(OpWrite))
	// stateid (16 bytes) — placeholder
	compound.WriteUint32(1) // seqid
	compound.WriteUint64(0)
	compound.WriteUint32(0)
	compound.WriteUint64(0) // offset
	compound.WriteUint32(2) // FILE_SYNC
	compound.WriteOpaque([]byte("hello nfs4!"))

	// CLOSE
	compound.WriteUint32(uint32(OpClose))
	compound.WriteUint32(0) // seqid
	compound.WriteUint32(0); compound.WriteUint64(0); compound.WriteUint32(0) // stateid

	rpc := buildRPCCallFrame(4, compound.Bytes())
	require.NoError(t, writeRPCFrame(conn, rpc))

	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	r := NewXDRReader(reply)
	r.ReadUint32() // XID
	r.ReadUint32(); r.ReadUint32(); r.ReadUint32(); r.ReadOpaque(); r.ReadUint32() // RPC header

	status, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), status, "write compound should succeed")

	// Step 2: PUTROOTFH + LOOKUP "test.txt" + READ
	compound2 := &XDRWriter{}
	compound2.WriteString("")
	compound2.WriteUint32(0)
	compound2.WriteUint32(3) // PUTROOTFH, LOOKUP, READ

	compound2.WriteUint32(uint32(OpPutRootFH))

	compound2.WriteUint32(uint32(OpLookup))
	compound2.WriteString("test.txt")

	compound2.WriteUint32(uint32(OpRead))
	compound2.WriteUint32(0); compound2.WriteUint64(0); compound2.WriteUint32(0) // stateid
	compound2.WriteUint64(0) // offset
	compound2.WriteUint32(1024) // count

	rpc2 := buildRPCCallFrame(5, compound2.Bytes())
	require.NoError(t, writeRPCFrame(conn, rpc2))

	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)

	r2 := NewXDRReader(reply2)
	r2.ReadUint32() // XID
	r2.ReadUint32(); r2.ReadUint32(); r2.ReadUint32(); r2.ReadOpaque(); r2.ReadUint32() // RPC header

	status2, _ := r2.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), status2, "read compound should succeed")

	r2.ReadString() // tag
	opCount, _ := r2.ReadUint32()
	assert.Equal(t, uint32(3), opCount)

	// Skip PUTROOTFH result
	r2.ReadUint32(); r2.ReadUint32()
	// Skip LOOKUP result
	r2.ReadUint32(); r2.ReadUint32()
	// READ result
	readOpCode, _ := r2.ReadUint32()
	assert.Equal(t, uint32(OpRead), readOpCode)
	readStatus, _ := r2.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), readStatus)

	// READ data: eof(4) + data(opaque)
	r2.ReadUint32() // eof
	readData, err := r2.ReadOpaque()
	require.NoError(t, err)
	assert.Equal(t, "hello nfs4!", string(readData))
}

func buildRPCCallFrame(xid uint32, compoundData []byte) []byte {
	rpc := &XDRWriter{}
	rpc.WriteUint32(xid)
	rpc.WriteUint32(rpcMsgCall)
	rpc.WriteUint32(2)
	rpc.WriteUint32(rpcProgNFS)
	rpc.WriteUint32(rpcVersNFS4)
	rpc.WriteUint32(1) // COMPOUND
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.WriteUint32(authNone)
	rpc.WriteUint32(0)
	rpc.buf.Write(compoundData)
	return rpc.Bytes()
}
