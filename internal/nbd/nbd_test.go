//go:build linux

package nbd

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNBD(t *testing.T) (*Server, net.Conn) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	mgr := volume.NewManager(backend)
	_, err = mgr.Create("nbd-test", 1024*1024)
	require.NoError(t, err)

	srv := NewServer(mgr, "nbd-test")

	// Use a pipe for testing
	client, server := net.Pipe()
	go srv.handleConn(server)

	// Read handshake
	hdr := make([]byte, 152)
	_, err = client.Read(hdr)
	require.NoError(t, err)

	magic := binary.BigEndian.Uint64(hdr[0:8])
	assert.Equal(t, nbdMagic, magic)

	t.Cleanup(func() {
		client.Close()
	})

	return srv, client
}

func TestNBDHandshake(t *testing.T) {
	_, _ = setupNBD(t)
	// If we get here, handshake succeeded
}

func TestNBDWriteRead(t *testing.T) {
	_, conn := setupNBD(t)

	// Write request
	data := []byte("Hello NBD!")
	writeReq := make([]byte, 28+len(data))
	binary.BigEndian.PutUint32(writeReq[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(writeReq[4:6], 0)             // flags
	binary.BigEndian.PutUint16(writeReq[6:8], uint16(nbdCmdWrite))
	binary.BigEndian.PutUint64(writeReq[8:16], 1)            // handle
	binary.BigEndian.PutUint64(writeReq[16:24], 0)           // offset
	binary.BigEndian.PutUint32(writeReq[24:28], uint32(len(data)))
	copy(writeReq[28:], data)

	_, err := conn.Write(writeReq)
	require.NoError(t, err)

	// Read write reply
	reply := make([]byte, 16)
	_, err = conn.Read(reply)
	require.NoError(t, err)
	assert.Equal(t, nbdReplyMagic, binary.BigEndian.Uint32(reply[0:4]))
	assert.Equal(t, uint32(0), binary.BigEndian.Uint32(reply[4:8]))

	// Read request
	readReq := make([]byte, 28)
	binary.BigEndian.PutUint32(readReq[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(readReq[4:6], 0)
	binary.BigEndian.PutUint16(readReq[6:8], uint16(nbdCmdRead))
	binary.BigEndian.PutUint64(readReq[8:16], 2)            // handle
	binary.BigEndian.PutUint64(readReq[16:24], 0)           // offset
	binary.BigEndian.PutUint32(readReq[24:28], uint32(len(data)))

	_, err = conn.Write(readReq)
	require.NoError(t, err)

	// Read reply header + data
	replyBuf := make([]byte, 16+len(data))
	_, err = conn.Read(replyBuf)
	require.NoError(t, err)
	assert.Equal(t, nbdReplyMagic, binary.BigEndian.Uint32(replyBuf[0:4]))
	assert.Equal(t, data, replyBuf[16:])
}
