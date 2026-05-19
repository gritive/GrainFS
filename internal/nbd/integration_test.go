//go:build linux

package nbd

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// nbdClient is a minimal Go NBD client for E2E testing.
type nbdClient struct {
	conn   net.Conn
	handle uint64
}

func dialNBD(t *testing.T, addr, export string) *nbdClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	// Receive server header (18 bytes)
	hdr := make([]byte, 18)
	_, err = io.ReadFull(conn, hdr)
	require.NoError(t, err)
	require.Equal(t, nbdMagic, binary.BigEndian.Uint64(hdr[0:8]))

	// Send client flags
	clientFlags := make([]byte, 4)
	binary.BigEndian.PutUint32(clientFlags, 1)
	_, err = conn.Write(clientFlags)
	require.NoError(t, err)

	// Send OPT_EXPORT_NAME
	name := []byte(export)
	opt := make([]byte, 16+len(name))
	binary.BigEndian.PutUint64(opt[0:8], nbdOptionMagic)
	binary.BigEndian.PutUint32(opt[8:12], nbdOptExportName)
	binary.BigEndian.PutUint32(opt[12:16], uint32(len(name)))
	copy(opt[16:], name)
	_, err = conn.Write(opt)
	require.NoError(t, err)

	// Receive export data (134 bytes)
	exportData := make([]byte, 134)
	_, err = io.ReadFull(conn, exportData)
	require.NoError(t, err)

	t.Cleanup(func() { conn.Close() })
	return &nbdClient{conn: conn}
}

func (c *nbdClient) write(t *testing.T, offset uint64, data []byte) {
	t.Helper()
	c.handle++
	req := make([]byte, 28+len(data))
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdWrite))
	binary.BigEndian.PutUint64(req[8:16], c.handle)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], uint32(len(data)))
	copy(req[28:], data)
	_, err := c.conn.Write(req)
	require.NoError(t, err)

	reply := make([]byte, 16)
	_, err = io.ReadFull(c.conn, reply)
	require.NoError(t, err)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(reply[4:8]), "write error")
}

func (c *nbdClient) read(t *testing.T, offset uint64, length uint32) []byte {
	t.Helper()
	c.handle++
	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdRead))
	binary.BigEndian.PutUint64(req[8:16], c.handle)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], length)
	_, err := c.conn.Write(req)
	require.NoError(t, err)

	buf := make([]byte, 16+length)
	_, err = io.ReadFull(c.conn, buf)
	require.NoError(t, err)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(buf[4:8]), "read error")
	return buf[16:]
}

func setupTCPNBD(t *testing.T) (string, *volume.Manager) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	mgr := volume.NewManager(backend)
	_, err = mgr.Create("vol", 4*1024*1024) // 4MB volume
	require.NoError(t, err)

	srv := NewServer(mgr, "vol")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()

	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() { srv.Close() })

	return addr, mgr
}

// TestNBD_E2E_TCP_LargeBlock verifies that large block reads/writes work over TCP.
func TestNBD_E2E_TCP_LargeBlock(t *testing.T) {
	addr, _ := setupTCPNBD(t)
	c := dialNBD(t, addr, "vol")

	// Write 1MB of data
	data := bytes.Repeat([]byte("NBD large block test "), 50*1024)
	data = data[:1024*1024] // exactly 1MB
	c.write(t, 0, data)

	// Read it back
	got := c.read(t, 0, uint32(len(data)))
	require.Equal(t, data, got)
}

// TestNBD_E2E_TCP_BlockSizeAligned tests reads/writes aligned to block size.
func TestNBD_E2E_TCP_BlockSizeAligned(t *testing.T) {
	addr, _ := setupTCPNBD(t)
	c := dialNBD(t, addr, "vol")

	const blockSize = 4096
	data := bytes.Repeat([]byte{0xAB}, blockSize)

	// Write block 0
	c.write(t, 0, data)

	// Write block 1 (different pattern)
	data2 := bytes.Repeat([]byte{0xCD}, blockSize)
	c.write(t, blockSize, data2)

	// Read both back
	got0 := c.read(t, 0, blockSize)
	require.Equal(t, data, got0)

	got1 := c.read(t, blockSize, blockSize)
	require.Equal(t, data2, got1)
}

// TestNBD_E2E_TCP_UnalignedOffset tests partial block reads/writes.
func TestNBD_E2E_TCP_UnalignedOffset(t *testing.T) {
	addr, _ := setupTCPNBD(t)
	c := dialNBD(t, addr, "vol")

	const offset = 100
	payload := []byte("unaligned write at offset 100")
	c.write(t, offset, payload)

	got := c.read(t, offset, uint32(len(payload)))
	require.Equal(t, payload, got)
}

// TestNBD_E2E_TCP_MultipleClients tests concurrent clients on the same volume.
func TestNBD_E2E_TCP_MultipleClients(t *testing.T) {
	addr, _ := setupTCPNBD(t)

	// Two clients write to non-overlapping regions
	c1 := dialNBD(t, addr, "vol")
	c2 := dialNBD(t, addr, "vol")

	data1 := bytes.Repeat([]byte{0x11}, 4096)
	data2 := bytes.Repeat([]byte{0x22}, 4096)

	c1.write(t, 0, data1)
	c2.write(t, 4096, data2)

	// Each client reads the other's data
	got2 := c1.read(t, 4096, 4096)
	require.Equal(t, data2, got2)

	got1 := c2.read(t, 0, 4096)
	require.Equal(t, data1, got1)
}
