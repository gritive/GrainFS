//go:build linux

package nbd

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// nbdClient is a minimal Go NBD client for E2E testing.
type nbdClient struct {
	conn   net.Conn
	handle uint64
}

type nbdTestTB interface {
	Helper()
	Cleanup(func())
	TempDir() string
	Errorf(format string, args ...interface{})
	FailNow()
}

func dialNBD(t nbdTestTB, addr, export string) *nbdClient {
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

	DeferCleanup(conn.Close)
	return &nbdClient{conn: conn}
}

func (c *nbdClient) write(t nbdTestTB, offset uint64, data []byte) {
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

func (c *nbdClient) read(t nbdTestTB, offset uint64, length uint32) []byte {
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

func setupTCPNBD(t nbdTestTB) (string, *volume.Manager) {
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
	DeferCleanup(srv.Close)

	return addr, mgr
}

var _ = Describe("NBD TCP integration", func() {
	var t nbdTestTB

	BeforeEach(func() {
		t = GinkgoT()
	})

	It("round-trips large blocks", func() {
		addr, _ := setupTCPNBD(t)
		c := dialNBD(t, addr, "vol")

		data := bytes.Repeat([]byte("NBD large block test "), 50*1024)
		data = data[:1024*1024]
		c.write(t, 0, data)

		got := c.read(t, 0, uint32(len(data)))
		Expect(got).To(Equal(data))
	})

	It("round-trips block-aligned reads and writes", func() {
		addr, _ := setupTCPNBD(t)
		c := dialNBD(t, addr, "vol")

		const blockSize = 4096
		data := bytes.Repeat([]byte{0xAB}, blockSize)
		c.write(t, 0, data)

		data2 := bytes.Repeat([]byte{0xCD}, blockSize)
		c.write(t, blockSize, data2)

		Expect(c.read(t, 0, blockSize)).To(Equal(data))
		Expect(c.read(t, blockSize, blockSize)).To(Equal(data2))
	})

	It("round-trips unaligned writes", func() {
		addr, _ := setupTCPNBD(t)
		c := dialNBD(t, addr, "vol")

		const offset = 100
		payload := []byte("unaligned write at offset 100")
		c.write(t, offset, payload)

		got := c.read(t, offset, uint32(len(payload)))
		Expect(got).To(Equal(payload))
	})

	It("serves multiple clients on one volume", func() {
		addr, _ := setupTCPNBD(t)
		c1 := dialNBD(t, addr, "vol")
		c2 := dialNBD(t, addr, "vol")

		data1 := bytes.Repeat([]byte{0x11}, 4096)
		data2 := bytes.Repeat([]byte{0x22}, 4096)

		c1.write(t, 0, data1)
		c2.write(t, 4096, data2)

		Expect(c1.read(t, 4096, 4096)).To(Equal(data2))
		Expect(c2.read(t, 0, 4096)).To(Equal(data1))
	})
})
