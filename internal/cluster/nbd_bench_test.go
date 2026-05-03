package cluster

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/volume"
)

const (
	benchNBDMagic        = uint64(0x4e42444d41474943)
	benchNBDOptionMagic  = uint64(0x49484156454f5054)
	benchNBDRequestMagic = uint32(0x25609513)
	benchNBDReplyMagic   = uint32(0x67446698)
	benchNBDOptExport    = uint32(1)
	benchNBDCmdRead      = uint16(0)
	benchNBDCmdWrite     = uint16(1)
	benchNBDCmdFlush     = uint16(3)
)

type clusterNBDConn struct {
	conn net.Conn
	req  []byte
	resp []byte
}

func setupClusterNBD(b *testing.B) *clusterNBDConn {
	b.Helper()

	backend := newTestDistributedBackend(b)
	mgr := volume.NewManager(backend)
	_, err := mgr.Create("vol", 64*1024*1024)
	require.NoError(b, err)

	srv := nbd.NewServer(mgr, "vol")
	srv.SetReadIndexer(backend)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	go func() { _ = srv.Serve(ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(b, err)
	require.NoError(b, clusterNBDHandshake(conn, "vol"))

	b.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
		_ = ln.Close()
	})
	return &clusterNBDConn{
		conn: conn,
		req:  make([]byte, 28+64*1024),
		resp: make([]byte, 16+64*1024),
	}
}

func clusterNBDHandshake(conn net.Conn, export string) error {
	hdr := make([]byte, 18)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return err
	}
	if binary.BigEndian.Uint64(hdr[0:8]) != benchNBDMagic {
		return io.ErrUnexpectedEOF
	}
	var flags [4]byte
	binary.BigEndian.PutUint32(flags[:], 1)
	if _, err := conn.Write(flags[:]); err != nil {
		return err
	}

	name := []byte(export)
	opt := make([]byte, 16+len(name))
	binary.BigEndian.PutUint64(opt[0:8], benchNBDOptionMagic)
	binary.BigEndian.PutUint32(opt[8:12], benchNBDOptExport)
	binary.BigEndian.PutUint32(opt[12:16], uint32(len(name)))
	copy(opt[16:], name)
	if _, err := conn.Write(opt); err != nil {
		return err
	}
	_, err := io.ReadFull(conn, make([]byte, 134))
	return err
}

func (c *clusterNBDConn) writeAt(offset uint64, data []byte) error {
	req := c.req[:28+len(data)]
	binary.BigEndian.PutUint32(req[0:4], benchNBDRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], benchNBDCmdWrite)
	binary.BigEndian.PutUint64(req[8:16], 1)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], uint32(len(data)))
	copy(req[28:], data)
	if _, err := c.conn.Write(req); err != nil {
		return err
	}
	if _, err := io.ReadFull(c.conn, c.resp[:16]); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(c.resp[0:4]) != benchNBDReplyMagic {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (c *clusterNBDConn) flush() error {
	req := c.req[:28]
	binary.BigEndian.PutUint32(req[0:4], benchNBDRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], benchNBDCmdFlush)
	binary.BigEndian.PutUint64(req[8:16], 1)
	if _, err := c.conn.Write(req); err != nil {
		return err
	}
	_, err := io.ReadFull(c.conn, c.resp[:16])
	return err
}

func (c *clusterNBDConn) readAt(offset uint64, size uint32) error {
	req := c.req[:28]
	binary.BigEndian.PutUint32(req[0:4], benchNBDRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], benchNBDCmdRead)
	binary.BigEndian.PutUint64(req[8:16], 1)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], size)
	if _, err := c.conn.Write(req); err != nil {
		return err
	}
	_, err := io.ReadFull(c.conn, c.resp[:16+int(size)])
	return err
}

func BenchmarkClusterNBD_Write4K(b *testing.B) {
	c := setupClusterNBD(b)
	data := make([]byte, 4*1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.writeAt(0, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterNBD_Write64K(b *testing.B) {
	c := setupClusterNBD(b)
	data := make([]byte, 64*1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.writeAt(0, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterNBD_FlushAfter16Writes4K(b *testing.B) {
	c := setupClusterNBD(b)
	data := make([]byte, 4*1024)
	b.SetBytes(int64(16 * len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 16; j++ {
			if err := c.writeAt(uint64(j*len(data)), data); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterNBD_Read4K(b *testing.B) {
	c := setupClusterNBD(b)
	data := make([]byte, 4*1024)
	require.NoError(b, c.writeAt(0, data))
	require.NoError(b, c.flush())
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.readAt(0, uint32(len(data))); err != nil {
			b.Fatal(err)
		}
	}
}
