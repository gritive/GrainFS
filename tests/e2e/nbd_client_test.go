package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/volumeadmin"
)

const (
	e2eNBDMagic        = uint64(0x4e42444d41474943)
	e2eNBDOptionMagic  = uint64(0x49484156454F5054)
	e2eNBDRequestMagic = uint32(0x25609513)
	e2eNBDOptExport    = uint32(1)
	e2eNBDCmdRead      = uint32(0)
	e2eNBDCmdWrite     = uint32(1)
	e2eNBDCmdFlush     = uint32(3)
)

type e2eNBDClient struct {
	conn   net.Conn
	handle uint64
}

func ensureE2ENBDVolume(t testing.TB, ctx context.Context, c *e2eCluster, name string, size int64) {
	t.Helper()
	leaderIdx := c.leaderIdx
	if leaderIdx < 0 {
		leaderIdx = 0
	}
	cli, err := volumeadmin.NewClient(filepath.Join(c.dataDirs[leaderIdx], "admin.sock"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = cli.CreateVolume(ctx, volumeadmin.CreateVolumeReq{Name: name, Size: size})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func dialE2ENBD(t testing.TB, addr string, export string) *e2eNBDClient {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("tcp", addr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(conn.SetDeadline(time.Now().Add(5 * time.Second))).To(gomega.Succeed())
	t.Cleanup(func() { _ = conn.SetDeadline(time.Time{}) })

	hdr := make([]byte, 18)
	_, err = io.ReadFull(conn, hdr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(binary.BigEndian.Uint64(hdr[0:8])).To(gomega.Equal(e2eNBDMagic))

	clientFlags := make([]byte, 4)
	binary.BigEndian.PutUint32(clientFlags, 1)
	_, err = conn.Write(clientFlags)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	name := []byte(export)
	opt := make([]byte, 16+len(name))
	binary.BigEndian.PutUint64(opt[0:8], e2eNBDOptionMagic)
	binary.BigEndian.PutUint32(opt[8:12], e2eNBDOptExport)
	binary.BigEndian.PutUint32(opt[12:16], uint32(len(name)))
	copy(opt[16:], name)
	_, err = conn.Write(opt)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	exportData := make([]byte, 134)
	_, err = io.ReadFull(conn, exportData)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(conn.SetDeadline(time.Time{})).To(gomega.Succeed())

	return &e2eNBDClient{conn: conn}
}

func (c *e2eNBDClient) Close() error {
	return c.conn.Close()
}

func (c *e2eNBDClient) WriteAt(t testing.TB, off uint64, data []byte) {
	t.Helper()
	c.handle++
	req := make([]byte, 28+len(data))
	binary.BigEndian.PutUint32(req[0:4], e2eNBDRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(e2eNBDCmdWrite))
	binary.BigEndian.PutUint64(req[8:16], c.handle)
	binary.BigEndian.PutUint64(req[16:24], off)
	binary.BigEndian.PutUint32(req[24:28], uint32(len(data)))
	copy(req[28:], data)
	_, err := c.conn.Write(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	reply := make([]byte, 16)
	_, err = io.ReadFull(c.conn, reply)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(binary.BigEndian.Uint32(reply[4:8])).To(gomega.Equal(uint32(0)), "write error")
}

func (c *e2eNBDClient) Flush(t testing.TB) {
	t.Helper()
	c.handle++
	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:4], e2eNBDRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(e2eNBDCmdFlush))
	binary.BigEndian.PutUint64(req[8:16], c.handle)
	_, err := c.conn.Write(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	reply := make([]byte, 16)
	_, err = io.ReadFull(c.conn, reply)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(binary.BigEndian.Uint32(reply[4:8])).To(gomega.Equal(uint32(0)), "flush error")
}

func requireNBDReadEventually(t testing.TB, client *e2eNBDClient, off uint64, want []byte) {
	t.Helper()
	var got []byte
	var lastErr error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		got, err = client.tryReadAt(off, uint32(len(want)))
		lastErr = err
		if err == nil && bytes.Equal(got, want) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	gomega.Expect(got).To(gomega.Equal(want),
		"NBD read did not return committed bytes: offset=%d got=%x want=%x err=%v", off, got, want, lastErr)
}

func (c *e2eNBDClient) ReadAt(t testing.TB, off uint64, size uint32) []byte {
	t.Helper()
	got, err := c.tryReadAt(off, size)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return got
}

func (c *e2eNBDClient) tryReadAt(off uint64, size uint32) ([]byte, error) {
	_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = c.conn.SetDeadline(time.Time{}) }()

	c.handle++
	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:4], e2eNBDRequestMagic)
	binary.BigEndian.PutUint16(req[6:8], uint16(e2eNBDCmdRead))
	binary.BigEndian.PutUint64(req[8:16], c.handle)
	binary.BigEndian.PutUint64(req[16:24], off)
	binary.BigEndian.PutUint32(req[24:28], size)
	if _, err := c.conn.Write(req); err != nil {
		return nil, err
	}

	buf := make([]byte, 16+size)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	}
	if errno := binary.BigEndian.Uint32(buf[4:8]); errno != 0 {
		return nil, fmt.Errorf("read error errno=%d", errno)
	}
	return buf[16:], nil
}
