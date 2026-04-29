package nfs4server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequenceReplayCache_HitReturnsCached(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 200)

	// 첫 번째 SEQUENCE: seqID=1, cacheThis=true
	compound1 := buildCompound41(buildSequenceOp(sid, 1, 0, 0, true))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(201, compound1)))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)
	status1, _ := parseCompoundReply(t, reply1)
	require.Equal(t, uint32(NFS4_OK), status1, "first SEQUENCE should succeed")

	// 동일 seqID=1, slotID=0 재전송 → 캐시된 응답 반환 (NFS4_OK)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(202, compound1)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	status2, _ := parseCompoundReply(t, reply2)
	assert.Equal(t, uint32(NFS4_OK), status2, "replay should return cached NFS4_OK")
}

func TestSequenceReplayCache_StaleSeqReturnsError(t *testing.T) {
	addr, _ := startTestNFS4Server(t)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	sid := exchangeIDAndCreateSession(t, conn, 210)

	// seqID=1 성공
	compound1 := buildCompound41(buildSequenceOp(sid, 1, 0, 0, false))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(211, compound1)))
	reply1, err := readRPCFrame(conn)
	require.NoError(t, err)
	status1, _ := parseCompoundReply(t, reply1)
	require.Equal(t, uint32(NFS4_OK), status1)

	// seqID=2 성공
	compound2 := buildCompound41(buildSequenceOp(sid, 2, 0, 0, false))
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(212, compound2)))
	reply2, err := readRPCFrame(conn)
	require.NoError(t, err)
	status2, _ := parseCompoundReply(t, reply2)
	require.Equal(t, uint32(NFS4_OK), status2)

	// seqID=1 재전송 (stale) → error
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(213, compound1)))
	reply3, err := readRPCFrame(conn)
	require.NoError(t, err)
	status3, _ := parseCompoundReply(t, reply3)
	assert.NotEqual(t, uint32(NFS4_OK), status3, "stale sequenceid must fail")
}
