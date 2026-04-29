package nfs4server

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildCompound41 builds a COMPOUND4args with minorversion=1.
func buildCompound41(ops ...[]byte) []byte {
	w := &XDRWriter{}
	w.WriteString("") // tag
	w.WriteUint32(1)  // minor version = 1
	w.WriteUint32(uint32(len(ops)))
	for _, op := range ops {
		w.buf.Write(op)
	}
	return w.Bytes()
}

// buildExchangeIDOp builds the XDR for an EXCHANGE_ID op.
func buildExchangeIDOp(verifier [8]byte, ownerID string) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpExchangeID)
	w.buf.Write(verifier[:]) // verifier
	w.WriteString(ownerID)  // eia_clientowner.co_ownerid
	w.WriteUint32(0)        // eia_flags
	w.WriteUint32(0)        // eia_state_protect.spa_how = SP4_NONE
	w.WriteUint32(0)        // eia_client_impl_id count = 0
	return w.Bytes()
}

// buildCreateSessionOp builds the XDR for a CREATE_SESSION op.
func buildCreateSessionOp(clientID uint64, seq uint32) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpCreateSession)
	w.WriteUint64(clientID) // csa_clientid
	w.WriteUint32(seq)      // csa_sequence
	w.WriteUint32(0)        // csa_flags

	// fore channel attrs
	w.WriteUint32(0)       // ca_headerpadsize
	w.WriteUint32(262144)  // ca_maxrequestsize
	w.WriteUint32(262144)  // ca_maxresponsesize
	w.WriteUint32(4096)    // ca_maxresponsesize_cached
	w.WriteUint32(64)      // ca_maxoperations
	w.WriteUint32(64)      // ca_maxrequests
	w.WriteUint32(0)       // ca_rdma_ird count = 0

	// back channel attrs (minimal)
	w.WriteUint32(0)  // ca_headerpadsize
	w.WriteUint32(4096)
	w.WriteUint32(4096)
	w.WriteUint32(4096)
	w.WriteUint32(4)
	w.WriteUint32(4)
	w.WriteUint32(0) // ca_rdma_ird count = 0

	w.WriteUint32(0) // csa_cb_program
	w.WriteUint32(0) // csa_sec_parms count = 0
	return w.Bytes()
}

// buildSequenceOp builds the XDR for a SEQUENCE op.
func buildSequenceOp(sessionID SessionID, seqID, slotID, highSlot uint32, cacheThis bool) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpSequence)
	w.buf.Write(sessionID[:]) // sa_sessionid
	w.WriteUint32(seqID)      // sa_sequenceid
	w.WriteUint32(slotID)     // sa_slotid
	w.WriteUint32(highSlot)   // sa_highest_slotid
	if cacheThis {
		w.WriteUint32(1)
	} else {
		w.WriteUint32(0)
	}
	return w.Bytes()
}

// parseCompoundReply parses the COMPOUND response and returns (status, results[]byte).
// results contains the raw XDR bytes after the op count.
func parseCompoundReply(t *testing.T, reply []byte) (uint32, *XDRReader) {
	t.Helper()
	r := NewXDRReader(reply)
	r.ReadUint32() // XID
	r.ReadUint32() // msg_type
	r.ReadUint32() // reply_stat
	r.ReadUint32() // verf flavor
	r.ReadOpaque() // verf body
	r.ReadUint32() // accept_stat

	status, err := r.ReadUint32()
	require.NoError(t, err)
	r.ReadString() // tag
	return status, r
}

func TestE2E_ExchangeID(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	var verifier [8]byte
	binary.BigEndian.PutUint64(verifier[:], 0xdeadbeef12345678)

	compound := buildCompound41(buildExchangeIDOp(verifier, "test-client-v41"))
	rpc := buildRPCCallFrame(10, compound)
	require.NoError(t, writeRPCFrame(conn, rpc))

	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status, "EXCHANGE_ID should succeed")

	opCount, _ := r.ReadUint32()
	require.Equal(t, uint32(1), opCount)

	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpExchangeID), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), opStatus)

	// Read eir_clientid and eir_sequenceid
	clientID, err := r.ReadUint64()
	require.NoError(t, err)
	assert.NotZero(t, clientID, "server must assign a non-zero clientID")

	seqID, _ := r.ReadUint32()
	_ = seqID // first call returns 0

	// eir_flags
	r.ReadUint32()
	// eir_state_protect.spr_how (SP4_NONE = 0)
	spr, _ := r.ReadUint32()
	assert.Equal(t, uint32(0), spr, "server should respond with SP4_NONE")

	// eir_server_owner: minor_id(8) + major_id(opaque)
	r.ReadUint64()
	r.ReadOpaque()

	// eir_server_scope: opaque
	r.ReadOpaque()

	// eir_server_impl_id count (we send 0)
	implCount, _ := r.ReadUint32()
	assert.Equal(t, uint32(0), implCount)
}

func TestE2E_CreateSession(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// First: EXCHANGE_ID to get a clientID
	var verifier [8]byte
	binary.BigEndian.PutUint64(verifier[:], 0xabcdef0011223344)
	compound1 := buildCompound41(buildExchangeIDOp(verifier, "test-cs-client"))
	rpc1 := buildRPCCallFrame(11, compound1)
	require.NoError(t, writeRPCFrame(conn, rpc1))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)

	status1, r1 := parseCompoundReply(t, reply1)
	require.Equal(t, uint32(NFS4_OK), status1)
	r1.ReadUint32() // opCount
	r1.ReadUint32() // opCode
	r1.ReadUint32() // opStatus
	clientID, _ := r1.ReadUint64()
	seqID, _ := r1.ReadUint32()

	// Second: CREATE_SESSION
	compound2 := buildCompound41(buildCreateSessionOp(clientID, seqID))
	rpc2 := buildRPCCallFrame(12, compound2)
	require.NoError(t, writeRPCFrame(conn, rpc2))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)

	status2, r2 := parseCompoundReply(t, reply2)
	assert.Equal(t, uint32(NFS4_OK), status2, "CREATE_SESSION should succeed")

	opCount2, _ := r2.ReadUint32()
	require.Equal(t, uint32(1), opCount2)

	opCode2, _ := r2.ReadUint32()
	assert.Equal(t, uint32(OpCreateSession), opCode2)
	opStatus2, _ := r2.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), opStatus2)

	// csr_sessionid: fixed 16 bytes (no XDR length prefix)
	sidBytes, err := r2.ReadFixed(16)
	require.NoError(t, err)
	assert.Equal(t, 16, len(sidBytes), "session ID must be 16 bytes")

	// csr_sequence: uint32
	cseq, _ := r2.ReadUint32()
	_ = cseq
	// csr_flags: uint32
	r2.ReadUint32()
}

func TestE2E_Sequence(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// EXCHANGE_ID
	var verifier [8]byte
	binary.BigEndian.PutUint64(verifier[:], 0x1111222233334444)
	compound1 := buildCompound41(buildExchangeIDOp(verifier, "seq-test-client"))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(20, compound1)))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)
	_, r1 := parseCompoundReply(t, reply1)
	r1.ReadUint32() // opCount
	r1.ReadUint32() // opCode
	r1.ReadUint32() // opStatus
	clientID, _ := r1.ReadUint64()
	seqID, _ := r1.ReadUint32()

	// CREATE_SESSION
	compound2 := buildCompound41(buildCreateSessionOp(clientID, seqID))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(21, compound2)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	_, r2 := parseCompoundReply(t, reply2)
	r2.ReadUint32() // opCount
	r2.ReadUint32() // opCode
	r2.ReadUint32() // opStatus
	sidBytes, _ := r2.ReadFixed(16) // csr_sessionid: fixed 16 bytes
	var sessionID SessionID
	copy(sessionID[:], sidBytes)

	// SEQUENCE + PUTROOTFH + GETFH
	seqOp := buildSequenceOp(sessionID, 1, 0, 0, false)
	putrootfhOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpPutRootFH)
		return w.Bytes()
	}()
	getfhOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpGetFH)
		return w.Bytes()
	}()

	compound3 := buildCompound41(seqOp, putrootfhOp, getfhOp)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(22, compound3)))
	reply3, err := readRPCFrame(conn)
	require.NoError(t, err)

	status3, r3 := parseCompoundReply(t, reply3)
	assert.Equal(t, uint32(NFS4_OK), status3, "SEQUENCE+PUTROOTFH+GETFH should succeed")

	opCount, _ := r3.ReadUint32()
	assert.Equal(t, uint32(3), opCount)

	// SEQUENCE result
	opCode, _ := r3.ReadUint32()
	assert.Equal(t, uint32(OpSequence), opCode)
	opStatus, _ := r3.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), opStatus)

	// SEQUENCE4resok: sessionid(16 fixed) + sequenceid(4) + slotid(4) + highest_slotid(4) + target_highest_slotid(4) + status_flags(4)
	seqSID, _ := r3.ReadFixed(16) // sr_sessionid: fixed 16 bytes
	assert.Equal(t, 16, len(seqSID))
	r3.ReadUint32() // sequenceid
	r3.ReadUint32() // slotid
	r3.ReadUint32() // highest_slotid
	r3.ReadUint32() // target_highest_slotid
	r3.ReadUint32() // status_flags

	// PUTROOTFH result
	op2Code, _ := r3.ReadUint32()
	assert.Equal(t, uint32(OpPutRootFH), op2Code)
	op2Status, _ := r3.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), op2Status)

	// GETFH result
	op3Code, _ := r3.ReadUint32()
	assert.Equal(t, uint32(OpGetFH), op3Code)
	op3Status, _ := r3.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), op3Status)
	fh, _ := r3.ReadOpaque()
	assert.Equal(t, 16, len(fh))
}

func TestE2E_ReclaimComplete(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// EXCHANGE_ID + CREATE_SESSION + SEQUENCE + RECLAIM_COMPLETE
	var verifier [8]byte
	binary.BigEndian.PutUint64(verifier[:], 0xaabbccddeeff0011)
	compound1 := buildCompound41(buildExchangeIDOp(verifier, "reclaim-client"))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(30, compound1)))
	reply1, _ := readRPCFrame(conn)
	_, r1 := parseCompoundReply(t, reply1)
	r1.ReadUint32(); r1.ReadUint32(); r1.ReadUint32()
	clientID, _ := r1.ReadUint64()
	seqID, _ := r1.ReadUint32()

	compound2 := buildCompound41(buildCreateSessionOp(clientID, seqID))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(31, compound2)))
	reply2, _ := readRPCFrame(conn)
	_, r2 := parseCompoundReply(t, reply2)
	r2.ReadUint32(); r2.ReadUint32(); r2.ReadUint32()
	sidBytes, _ := r2.ReadFixed(16) // csr_sessionid: fixed 16 bytes
	var sessionID SessionID
	copy(sessionID[:], sidBytes)

	// SEQUENCE + RECLAIM_COMPLETE
	seqOp := buildSequenceOp(sessionID, 1, 0, 0, false)
	rcOp := func() []byte {
		w := &XDRWriter{}
		w.WriteUint32(OpReclaimComplete)
		w.WriteUint32(0) // rca_one_fs = false
		return w.Bytes()
	}()

	compound3 := buildCompound41(seqOp, rcOp)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(32, compound3)))
	reply3, err := readRPCFrame(conn)
	require.NoError(t, err)

	status3, r3 := parseCompoundReply(t, reply3)
	assert.Equal(t, uint32(NFS4_OK), status3)

	opCount, _ := r3.ReadUint32()
	assert.Equal(t, uint32(2), opCount)

	// SEQUENCE
	r3.ReadUint32(); r3.ReadUint32() // opCode + opStatus
	r3.ReadFixed(16)                 // sr_sessionid: fixed 16 bytes
	r3.ReadUint32(); r3.ReadUint32(); r3.ReadUint32(); r3.ReadUint32(); r3.ReadUint32()

	// RECLAIM_COMPLETE
	rcCode, _ := r3.ReadUint32()
	assert.Equal(t, uint32(OpReclaimComplete), rcCode)
	rcStatus, _ := r3.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), rcStatus)
}

// buildDestroySessionOp builds the XDR for a DESTROY_SESSION op.
func buildDestroySessionOp(sid SessionID) []byte {
	w := &XDRWriter{}
	w.WriteUint32(OpDestroySession)
	w.buf.Write(sid[:])
	return w.Bytes()
}

// exchangeIDAndCreateSession performs EXCHANGE_ID + CREATE_SESSION and returns the new SessionID.
func exchangeIDAndCreateSession(t *testing.T, conn net.Conn, xid uint32) SessionID {
	t.Helper()
	var verifier [8]byte
	binary.BigEndian.PutUint64(verifier[:], uint64(xid)^0xfeedface11223344)
	compound1 := buildCompound41(buildExchangeIDOp(verifier, fmt.Sprintf("ds-client-%d", xid)))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound1)))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)
	_, r1 := parseCompoundReply(t, reply1)
	r1.ReadUint32()
	r1.ReadUint32()
	r1.ReadUint32()
	clientID, _ := r1.ReadUint64()
	seqID, _ := r1.ReadUint32()

	compound2 := buildCompound41(buildCreateSessionOp(clientID, seqID))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid+1, compound2)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	_, r2 := parseCompoundReply(t, reply2)
	r2.ReadUint32()
	r2.ReadUint32()
	r2.ReadUint32()
	sidBytes, _ := r2.ReadFixed(16)
	var sid SessionID
	copy(sid[:], sidBytes)
	return sid
}

func TestDestroySession_Valid(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 100)

	compound := buildCompound41(buildDestroySessionOp(sid))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(102, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4_OK), status)
	r.ReadUint32()
	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpDestroySession), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4_OK), opStatus)
}

func TestDestroySession_Invalid(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	var fakeSID SessionID
	for i := range fakeSID {
		fakeSID[i] = 0xff
	}

	compound := buildCompound41(buildDestroySessionOp(fakeSID))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(200, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)

	status, r := parseCompoundReply(t, reply)
	assert.Equal(t, uint32(NFS4ERR_BADSESSION), status)
	r.ReadUint32()
	opCode, _ := r.ReadUint32()
	assert.Equal(t, uint32(OpDestroySession), opCode)
	opStatus, _ := r.ReadUint32()
	assert.Equal(t, uint32(NFS4ERR_BADSESSION), opStatus)
}

func TestDestroySession_Twice(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 300)

	// 첫 번째 → NFS4_OK
	compound1 := buildCompound41(buildDestroySessionOp(sid))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(302, compound1)))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)
	status1, _ := parseCompoundReply(t, reply1)
	assert.Equal(t, uint32(NFS4_OK), status1)

	// 두 번째 → NFS4ERR_BADSESSION
	compound2 := buildCompound41(buildDestroySessionOp(sid))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(303, compound2)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	status2, _ := parseCompoundReply(t, reply2)
	assert.Equal(t, uint32(NFS4ERR_BADSESSION), status2)
}

func TestDestroySession_SeqAfter(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 400)

	// DESTROY_SESSION
	compound1 := buildCompound41(buildDestroySessionOp(sid))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(402, compound1)))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)
	status1, _ := parseCompoundReply(t, reply1)
	require.Equal(t, uint32(NFS4_OK), status1)

	// SEQUENCE with destroyed sid → NFS4ERR_BADSESSION
	seqOp := buildSequenceOp(sid, 1, 0, 0, false)
	compound2 := buildCompound41(seqOp)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(403, compound2)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	status2, _ := parseCompoundReply(t, reply2)
	assert.Equal(t, uint32(NFS4ERR_BADSESSION), status2)
}
