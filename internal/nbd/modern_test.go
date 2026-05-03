package nbd

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/require"
)

type optionReply struct {
	opt     uint32
	typ     uint32
	payload []byte
}

type simpleReply struct {
	errCode uint32
	handle  [8]byte
}

type structuredChunk struct {
	flags   uint16
	typ     uint16
	handle  [8]byte
	payload []byte
}

func setupRawNBDConn(t *testing.T) (net.Conn, *Server) {
	t.Helper()

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	mgr := volume.NewManager(backend)
	_, err = mgr.Create("nbd-test", 1024*1024)
	require.NoError(t, err)

	srv := NewServer(mgr, "nbd-test")
	client, server := net.Pipe()
	go srv.handleConn(server)

	t.Cleanup(func() {
		client.Close()
	})

	return client, srv
}

func readServerHeader(t *testing.T, conn net.Conn) {
	t.Helper()

	hdr := readExact(t, conn, 18)
	require.Equal(t, nbdMagic, binary.BigEndian.Uint64(hdr[0:8]))
	require.Equal(t, nbdOptionMagic, binary.BigEndian.Uint64(hdr[8:16]))
	require.Equal(t, nbdFlagFixedNewstyle, binary.BigEndian.Uint16(hdr[16:18]))
}

func writeClientFlags(t *testing.T, conn net.Conn, flags uint32) {
	t.Helper()

	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], flags)
	_, err := conn.Write(buf[:])
	require.NoError(t, err)
}

func completeClientFlags(t *testing.T, conn net.Conn, flags uint32) {
	t.Helper()

	readServerHeader(t, conn)
	writeClientFlags(t, conn, flags)
}

func writeOptExportName(t *testing.T, conn net.Conn, name string) {
	t.Helper()
	writeOption(t, conn, nbdOptExportName, []byte(name))
}

func writeOptGo(t *testing.T, conn net.Conn, name string, info []uint16) {
	t.Helper()
	writeOption(t, conn, nbdOptGo, optionInfoPayload(name, info))
}

func writeOptInfo(t *testing.T, conn net.Conn, name string, info []uint16) {
	t.Helper()
	writeOption(t, conn, nbdOptInfo, optionInfoPayload(name, info))
}

func writeEmptyOption(t *testing.T, conn net.Conn, opt uint32) {
	t.Helper()
	writeOption(t, conn, opt, nil)
}

func writeOption(t *testing.T, conn net.Conn, opt uint32, payload []byte) {
	t.Helper()

	buf := make([]byte, 16+len(payload))
	binary.BigEndian.PutUint64(buf[0:8], nbdOptionMagic)
	binary.BigEndian.PutUint32(buf[8:12], opt)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(payload)))
	copy(buf[16:], payload)
	_, err := conn.Write(buf)
	require.NoError(t, err)
}

func optionInfoPayload(name string, info []uint16) []byte {
	payload := make([]byte, 4+len(name)+2+2*len(info))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(name)))
	copy(payload[4:], name)
	pos := 4 + len(name)
	binary.BigEndian.PutUint16(payload[pos:pos+2], uint16(len(info)))
	pos += 2
	for _, typ := range info {
		binary.BigEndian.PutUint16(payload[pos:pos+2], typ)
		pos += 2
	}
	return payload
}

func readExact(t *testing.T, conn net.Conn, n int) []byte {
	t.Helper()

	buf := make([]byte, n)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	return buf
}

func readOptionReplyHeader(conn net.Conn) (optionReply, error) {
	var hdr [20]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return optionReply{}, err
	}
	reply := optionReply{
		opt: binary.BigEndian.Uint32(hdr[8:12]),
		typ: binary.BigEndian.Uint32(hdr[12:16]),
	}
	length := binary.BigEndian.Uint32(hdr[16:20])
	if length > 0 {
		reply.payload = make([]byte, length)
		if _, err := io.ReadFull(conn, reply.payload); err != nil {
			return optionReply{}, err
		}
	}
	return reply, nil
}

func readOptionReply(t *testing.T, conn net.Conn) optionReply {
	t.Helper()

	reply, err := readOptionReplyHeader(conn)
	require.NoError(t, err)
	return reply
}

func readInfoReply(t *testing.T, conn net.Conn, infoType uint16) []byte {
	t.Helper()

	for {
		reply := readOptionReply(t, conn)
		require.Equal(t, nbdRepInfo, reply.typ)
		require.GreaterOrEqual(t, len(reply.payload), 2)
		if binary.BigEndian.Uint16(reply.payload[0:2]) == infoType {
			return reply.payload
		}
	}
}

func readSimpleReply(t *testing.T, conn net.Conn) simpleReply {
	t.Helper()

	buf := readExact(t, conn, 16)
	require.Equal(t, nbdReplyMagic, binary.BigEndian.Uint32(buf[0:4]))
	var reply simpleReply
	reply.errCode = binary.BigEndian.Uint32(buf[4:8])
	copy(reply.handle[:], buf[8:16])
	return reply
}

func readStructuredChunk(t *testing.T, conn net.Conn) structuredChunk {
	t.Helper()

	hdr := readExact(t, conn, 20)
	require.Equal(t, nbdStructuredReplyMagic, binary.BigEndian.Uint32(hdr[0:4]))
	chunk := structuredChunk{
		flags: binary.BigEndian.Uint16(hdr[4:6]),
		typ:   binary.BigEndian.Uint16(hdr[6:8]),
	}
	copy(chunk.handle[:], hdr[8:16])
	length := binary.BigEndian.Uint32(hdr[16:20])
	if length > 0 {
		chunk.payload = readExact(t, conn, int(length))
	}
	return chunk
}

func sendRawRequest(t *testing.T, conn net.Conn, typ uint32, offset uint64, length uint64, payload []byte, flags uint16) {
	t.Helper()

	req := make([]byte, 28+len(payload))
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], flags)
	binary.BigEndian.PutUint16(req[6:8], uint16(typ))
	binary.BigEndian.PutUint64(req[8:16], 1)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], uint32(length))
	copy(req[28:], payload)
	_, err := conn.Write(req)
	require.NoError(t, err)
}

func sendReadRequest(t *testing.T, conn net.Conn, offset uint64, length uint32) {
	t.Helper()
	sendRawRequest(t, conn, nbdCmdRead, offset, uint64(length), nil, 0)
}

func sendWriteZeroesConn(t *testing.T, conn net.Conn, offset uint64, length uint32, flags uint16) {
	t.Helper()
	sendRawRequest(t, conn, nbdCmdWriteZeroes, offset, uint64(length), nil, flags)
}

func sendTrimConn(t *testing.T, conn net.Conn, offset uint64, length uint32) {
	t.Helper()
	sendRawRequest(t, conn, nbdCmdTrim, offset, uint64(length), nil, 0)
}

func sendBlockStatusRequest(t *testing.T, conn net.Conn, offset uint64, length uint32) {
	t.Helper()
	sendRawRequest(t, conn, nbdCmdBlockStatus, offset, uint64(length), nil, nbdCmdFlagReqOne)
}

func setupStructuredNBD(t *testing.T) (net.Conn, *Server) {
	t.Helper()

	conn, srv := setupRawNBDConn(t)
	completeClientFlags(t, conn, nbdFlagClientFixedNewstyle)
	writeEmptyOption(t, conn, nbdOptStructuredReply)
	require.Equal(t, nbdRepAck, readOptionReply(t, conn).typ)
	writeOptExportName(t, conn, "nbd-test")
	readExact(t, conn, 134)
	return conn, srv
}

func setupBlockStatusNBD(t *testing.T) (net.Conn, *Server) {
	t.Helper()

	conn, srv := setupRawNBDConn(t)
	completeClientFlags(t, conn, nbdFlagClientFixedNewstyle)
	writeEmptyOption(t, conn, nbdOptStructuredReply)
	require.Equal(t, nbdRepAck, readOptionReply(t, conn).typ)
	writeMetaContextOption(t, conn, nbdOptSetMetaContext, "nbd-test", []string{"base:allocation"})
	meta := readOptionReply(t, conn)
	require.Equal(t, nbdRepMetaContext, meta.typ)
	require.Equal(t, nbdMetaContextBaseAllocID, binary.BigEndian.Uint32(meta.payload[0:4]))
	require.Equal(t, "base:allocation", string(meta.payload[4:]))
	require.Equal(t, nbdRepAck, readOptionReply(t, conn).typ)
	writeOptExportName(t, conn, "nbd-test")
	readExact(t, conn, 134)
	return conn, srv
}

func writeMetaContextOption(t *testing.T, conn net.Conn, opt uint32, name string, contexts []string) {
	t.Helper()

	payloadLen := 4 + len(name) + 4
	for _, contextName := range contexts {
		payloadLen += 4 + len(contextName)
	}
	payload := make([]byte, payloadLen)
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(name)))
	copy(payload[4:], name)
	pos := 4 + len(name)
	binary.BigEndian.PutUint32(payload[pos:pos+4], uint32(len(contexts)))
	pos += 4
	for _, contextName := range contexts {
		binary.BigEndian.PutUint32(payload[pos:pos+4], uint32(len(contextName)))
		pos += 4
		copy(payload[pos:], contextName)
		pos += len(contextName)
	}
	writeOption(t, conn, opt, payload)
}

func TestNBDHandshakeRejectsUnknownClientFlags(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	readServerHeader(t, client)
	writeClientFlags(t, client, 1<<31)
	_, err := readOptionReplyHeader(client)
	require.Error(t, err)
}

func TestNBDExportNameHonorsNoZeroes(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	readServerHeader(t, client)
	writeClientFlags(t, client, nbdFlagClientFixedNewstyle|nbdFlagClientNoZeroes)
	writeOptExportName(t, client, "nbd-test")
	reply := readExact(t, client, 10)
	require.Equal(t, uint64(1024*1024), binary.BigEndian.Uint64(reply[0:8]))
	require.Equal(t, nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendTrim|nbdFlagSendWriteZeroes, binary.BigEndian.Uint16(reply[8:10]))
}

func TestNBDOptGoValidatesExportName(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
	writeOptGo(t, client, "missing", []uint16{nbdInfoExport})
	rep := readOptionReply(t, client)
	require.Equal(t, nbdRepErrUnknown, rep.typ)
}

func TestNBDOptInfoBlockSize(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
	writeOptInfo(t, client, "nbd-test", []uint16{nbdInfoBlockSize})
	info := readInfoReply(t, client, nbdInfoBlockSize)
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(info[2:6]))
	require.Equal(t, uint32(4096), binary.BigEndian.Uint32(info[6:10]))
	require.Equal(t, uint32(nbdMaxPayloadSize), binary.BigEndian.Uint32(info[10:14]))
	ack := readOptionReply(t, client)
	require.Equal(t, nbdRepAck, ack.typ)
}

func TestNBDOptionRejectsOversizeBeforeAllocation(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	completeClientFlags(t, client, nbdFlagClientFixedNewstyle)

	hdr := make([]byte, 16)
	binary.BigEndian.PutUint64(hdr[0:8], nbdOptionMagic)
	binary.BigEndian.PutUint32(hdr[8:12], nbdOptInfo)
	binary.BigEndian.PutUint32(hdr[12:16], nbdMaxOptionPayloadSize+1)
	_, err := client.Write(hdr)
	require.NoError(t, err)

	rep := readOptionReply(t, client)
	require.Equal(t, nbdRepErrInvalid, rep.typ)
}

func TestNBDReadRejectsOversizeBeforeAllocation(t *testing.T) {
	_, conn := setupNBD(t)
	sendRawRequest(t, conn, nbdCmdRead, 0, uint64(nbdMaxPayloadSize)+1, nil, 0)
	reply := readSimpleReply(t, conn)
	require.Equal(t, nbdErrEINVAL, reply.errCode)
}

func TestNBDWriteRejectsOversizeAndCloses(t *testing.T) {
	_, conn := setupNBD(t)
	sendRawRequest(t, conn, nbdCmdWrite, 0, uint64(nbdMaxPayloadSize)+1, nil, 0)
	reply := readSimpleReply(t, conn)
	require.Equal(t, nbdErrEINVAL, reply.errCode)

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := io.ReadFull(conn, make([]byte, 1))
	require.Error(t, err)
}

func TestNBDFlushOrdersWriteZeroesAndTrim(t *testing.T) {
	_, conn := setupNBD(t)
	sendWriteConn(t, conn, 0, bytes.Repeat([]byte{0xaa}, 8192))

	sendWriteZeroesConn(t, conn, 0, 4096, 0)
	require.Equal(t, uint32(0), readSimpleReply(t, conn).errCode)

	sendTrimConn(t, conn, 4096, 4096)
	require.Equal(t, uint32(0), readSimpleReply(t, conn).errCode)

	sendFlushConn(t, conn)
	got := sendReadConn(t, conn, 0, 8192)
	require.Equal(t, make([]byte, 8192), got)
}

func TestNBDWriteZeroesReadBack(t *testing.T) {
	_, conn := setupNBD(t)
	sendWriteConn(t, conn, 0, bytes.Repeat([]byte{0xcc}, 8192))

	sendWriteZeroesConn(t, conn, 4096, 4096, 0)
	require.Equal(t, uint32(0), readSimpleReply(t, conn).errCode)
	sendFlushConn(t, conn)

	got := sendReadConn(t, conn, 0, 8192)
	require.Equal(t, bytes.Repeat([]byte{0xcc}, 4096), got[:4096])
	require.Equal(t, make([]byte, 4096), got[4096:])
}

func TestNBDWriteZeroesRejectsFastZero(t *testing.T) {
	_, conn := setupNBD(t)
	sendWriteZeroesConn(t, conn, 0, 4096, nbdCmdFlagFastZero)
	reply := readSimpleReply(t, conn)
	require.NotEqual(t, uint32(0), reply.errCode)
}

func TestNBDStructuredReadReply(t *testing.T) {
	client, _ := setupStructuredNBD(t)
	sendWriteConn(t, client, 0, []byte("abcd"))
	sendReadRequest(t, client, 0, 4)
	chunk := readStructuredChunk(t, client)
	require.Equal(t, nbdReplyTypeOffsetData, chunk.typ)
	require.Equal(t, nbdReplyFlagDone, chunk.flags)
	require.Equal(t, []byte("abcd"), chunk.payload[8:])
}

func TestNBDBlockStatusConservativeAllocated(t *testing.T) {
	client, _ := setupBlockStatusNBD(t)
	sendBlockStatusRequest(t, client, 0, 4096)
	chunk := readStructuredChunk(t, client)
	require.Equal(t, nbdReplyTypeBlockStatus, chunk.typ)
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(chunk.payload[0:4]))
	require.Equal(t, uint32(4096), binary.BigEndian.Uint32(chunk.payload[4:8]))
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(chunk.payload[8:12]))
}

func TestNBDSetMetaContextRequiresStructuredReply(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
	writeMetaContextOption(t, client, nbdOptSetMetaContext, "nbd-test", []string{"base:allocation"})
	rep := readOptionReply(t, client)
	require.Equal(t, nbdRepErrInvalid, rep.typ)
}

func TestNBDListMetaContextWithZeroQueriesReturnsBaseAllocation(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
	writeMetaContextOption(t, client, nbdOptListMetaContext, "nbd-test", nil)
	meta := readOptionReply(t, client)
	require.Equal(t, nbdRepMetaContext, meta.typ)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(meta.payload[0:4]))
	require.Equal(t, "base:allocation", string(meta.payload[4:]))
	require.Equal(t, nbdRepAck, readOptionReply(t, client).typ)
}

func TestNBDExtendedHeadersUnsupportedByDefault(t *testing.T) {
	client, _ := setupRawNBDConn(t)
	completeClientFlags(t, client, nbdFlagClientFixedNewstyle)
	writeEmptyOption(t, client, nbdOptExtendedHeaders)
	rep := readOptionReply(t, client)
	require.Equal(t, nbdRepErrUnsup, rep.typ)
}

func TestNBDExtendedRequestParserRejectsOversizeEffect(t *testing.T) {
	req := make([]byte, 32)
	binary.BigEndian.PutUint32(req[0:4], nbdExtendedRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdWriteZeroes))
	binary.BigEndian.PutUint64(req[24:32], uint64(nbdMaxPayloadSize)+1)
	_, err := parseExtendedRequest(req)
	require.Error(t, err)
}
