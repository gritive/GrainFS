package nbd

import (
	"net"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// setupBenchNBD returns a connected (client, server) pair after completing the NBD handshake.
// Uses net.Pipe() for in-process benchmarking with no network overhead.
func setupBenchNBD(b *testing.B) net.Conn {
	b.Helper()
	return setupBenchNBDWithHandshake(b, func(conn net.Conn) error {
		return doHandshake(conn, "vol")
	})
}

func setupBenchStructuredNBD(b *testing.B) net.Conn {
	b.Helper()
	return setupBenchNBDWithHandshake(b, func(conn net.Conn) error {
		return doStructuredHandshake(conn, "vol", false)
	})
}

func setupBenchBlockStatusNBD(b *testing.B) net.Conn {
	b.Helper()
	return setupBenchNBDWithHandshake(b, func(conn net.Conn) error {
		return doStructuredHandshake(conn, "vol", true)
	})
}

func setupBenchNBDWithHandshake(b *testing.B, handshake func(net.Conn) error) net.Conn {
	b.Helper()
	dir := b.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	mgr := volume.NewManager(backend)
	if _, err := mgr.Create("vol", 64*1024*1024); err != nil { // 64MB
		b.Fatal(err)
	}
	srv := NewServer(mgr, "vol")

	client, server := net.Pipe()
	go srv.handleConn(server) //nolint:errcheck

	if err := handshake(client); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { client.Close() })
	return client
}

func doHandshake(conn net.Conn, export string) error {
	hdr := make([]byte, 18)
	if _, err := readFull(conn, hdr); err != nil {
		return err
	}
	// client flags
	cf := [4]byte{}
	cf[3] = 1 // NBD_FLAG_C_FIXED_NEWSTYLE
	if _, err := conn.Write(cf[:]); err != nil {
		return err
	}
	// OPT_EXPORT_NAME
	name := []byte(export)
	opt := make([]byte, 16+len(name))
	putU64(opt[0:], nbdOptionMagic)
	putU32(opt[8:], nbdOptExportName)
	putU32(opt[12:], uint32(len(name)))
	copy(opt[16:], name)
	if _, err := conn.Write(opt); err != nil {
		return err
	}
	// export data (134 bytes)
	resp := make([]byte, 134)
	_, err := readFull(conn, resp)
	return err
}

func doStructuredHandshake(conn net.Conn, export string, meta bool) error {
	hdr := make([]byte, 18)
	if _, err := readFull(conn, hdr); err != nil {
		return err
	}
	cf := [4]byte{}
	cf[3] = 1
	if _, err := conn.Write(cf[:]); err != nil {
		return err
	}
	opt := make([]byte, 16)
	putU64(opt[0:], nbdOptionMagic)
	putU32(opt[8:], nbdOptStructuredReply)
	if _, err := conn.Write(opt); err != nil {
		return err
	}
	if err := drainOptionReply(conn); err != nil {
		return err
	}
	if meta {
		payload := benchMetaContextPayload(export, "base:allocation")
		opt = make([]byte, 16+len(payload))
		putU64(opt[0:], nbdOptionMagic)
		putU32(opt[8:], nbdOptSetMetaContext)
		putU32(opt[12:], uint32(len(payload)))
		copy(opt[16:], payload)
		if _, err := conn.Write(opt); err != nil {
			return err
		}
		if err := drainOptionRepliesUntilAck(conn); err != nil {
			return err
		}
	}
	name := []byte(export)
	opt = make([]byte, 16+len(name))
	putU64(opt[0:], nbdOptionMagic)
	putU32(opt[8:], nbdOptExportName)
	putU32(opt[12:], uint32(len(name)))
	copy(opt[16:], name)
	if _, err := conn.Write(opt); err != nil {
		return err
	}
	resp := make([]byte, 134)
	_, err := readFull(conn, resp)
	return err
}

func benchMetaContextPayload(export, context string) []byte {
	payload := make([]byte, 4+len(export)+4+4+len(context))
	putU32(payload[0:], uint32(len(export)))
	copy(payload[4:], export)
	pos := 4 + len(export)
	putU32(payload[pos:], 1)
	pos += 4
	putU32(payload[pos:], uint32(len(context)))
	pos += 4
	copy(payload[pos:], context)
	return payload
}

func drainOptionReply(conn net.Conn) error {
	hdr := make([]byte, 20)
	if _, err := readFull(conn, hdr); err != nil {
		return err
	}
	length := uint32(hdr[16])<<24 | uint32(hdr[17])<<16 | uint32(hdr[18])<<8 | uint32(hdr[19])
	if length > 0 {
		_, err := readFull(conn, make([]byte, length))
		return err
	}
	return nil
}

func drainOptionRepliesUntilAck(conn net.Conn) error {
	for {
		hdr := make([]byte, 20)
		if _, err := readFull(conn, hdr); err != nil {
			return err
		}
		replyType := uint32(hdr[12])<<24 | uint32(hdr[13])<<16 | uint32(hdr[14])<<8 | uint32(hdr[15])
		length := uint32(hdr[16])<<24 | uint32(hdr[17])<<16 | uint32(hdr[18])<<8 | uint32(hdr[19])
		if length > 0 {
			if _, err := readFull(conn, make([]byte, length)); err != nil {
				return err
			}
		}
		if replyType == nbdRepAck {
			return nil
		}
	}
}

// sendRead sends an NBD read request and drains the reply (header + data).
func sendRead(conn net.Conn, offset uint64, length uint32, req, reply []byte) error {
	putU32(req[0:], nbdRequestMagic)
	putU16(req[4:], 0)
	putU16(req[6:], uint16(nbdCmdRead))
	putU64(req[8:], 1)
	putU64(req[16:], offset)
	putU32(req[24:], length)
	if _, err := conn.Write(req[:28]); err != nil {
		return err
	}
	_, err := readFull(conn, reply[:16+int(length)])
	return err
}

// sendStructuredRead sends an NBD read request and drains one OFFSET_DATA chunk.
func sendStructuredRead(conn net.Conn, offset uint64, length uint32, req, reply []byte) error {
	putU32(req[0:], nbdRequestMagic)
	putU16(req[4:], 0)
	putU16(req[6:], uint16(nbdCmdRead))
	putU64(req[8:], 1)
	putU64(req[16:], offset)
	putU32(req[24:], length)
	if _, err := conn.Write(req[:28]); err != nil {
		return err
	}
	_, err := readFull(conn, reply[:20+8+int(length)])
	return err
}

func sendBlockStatus(conn net.Conn, offset uint64, length uint32, req, reply []byte) error {
	putU32(req[0:], nbdRequestMagic)
	putU16(req[4:], nbdCmdFlagReqOne)
	putU16(req[6:], uint16(nbdCmdBlockStatus))
	putU64(req[8:], 1)
	putU64(req[16:], offset)
	putU32(req[24:], length)
	if _, err := conn.Write(req[:28]); err != nil {
		return err
	}
	_, err := readFull(conn, reply[:32])
	return err
}

// sendWrite sends an NBD write request with data and drains the reply header.
func sendWrite(conn net.Conn, offset uint64, data, req, reply []byte) error {
	putU32(req[0:], nbdRequestMagic)
	putU16(req[4:], 0)
	putU16(req[6:], uint16(nbdCmdWrite))
	putU64(req[8:], 1)
	putU64(req[16:], offset)
	putU32(req[24:], uint32(len(data)))
	copy(req[28:], data)
	if _, err := conn.Write(req[:28+len(data)]); err != nil {
		return err
	}
	_, err := readFull(conn, reply[:16])
	return err
}

// sendWriteZeroes sends an NBD write-zeroes request and drains the reply header.
func sendWriteZeroes(conn net.Conn, offset uint64, length uint32, req, reply []byte) error {
	putU32(req[0:], nbdRequestMagic)
	putU16(req[4:], 0)
	putU16(req[6:], uint16(nbdCmdWriteZeroes))
	putU64(req[8:], 1)
	putU64(req[16:], offset)
	putU32(req[24:], length)
	if _, err := conn.Write(req[:28]); err != nil {
		return err
	}
	_, err := readFull(conn, reply[:16])
	return err
}

func BenchmarkNBD_Read4K(b *testing.B) {
	conn := setupBenchNBD(b)
	req := make([]byte, 28)
	reply := make([]byte, 16+4096)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendRead(conn, 0, 4096, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNBD_Read64K(b *testing.B) {
	conn := setupBenchNBD(b)
	req := make([]byte, 28)
	reply := make([]byte, 16+65536)
	b.SetBytes(65536)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendRead(conn, 0, 65536, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNBD_Write4K(b *testing.B) {
	conn := setupBenchNBD(b)
	data := make([]byte, 4096)
	req := make([]byte, 28+4096)
	reply := make([]byte, 16)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendWrite(conn, 0, data, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNBD_Write64K(b *testing.B) {
	conn := setupBenchNBD(b)
	data := make([]byte, 65536)
	req := make([]byte, 28+65536)
	reply := make([]byte, 16)
	b.SetBytes(65536)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendWrite(conn, 0, data, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNBD_WriteZeroes4K(b *testing.B) {
	conn := setupBenchNBD(b)
	req := make([]byte, 28)
	reply := make([]byte, 16)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendWriteZeroes(conn, 0, 4096, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNBD_StructuredRead4K(b *testing.B) {
	conn := setupBenchStructuredNBD(b)
	req := make([]byte, 28)
	reply := make([]byte, 20+8+4096)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendStructuredRead(conn, 0, 4096, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNBD_BlockStatus4K(b *testing.B) {
	conn := setupBenchBlockStatusNBD(b)
	req := make([]byte, 28)
	reply := make([]byte, 32)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendBlockStatus(conn, 0, 4096, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

// helpers (encoding/binary-free to avoid import cycle in bench)
func putU64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}
func putU32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
func putU16(b []byte, v uint16) { b[0] = byte(v >> 8); b[1] = byte(v) }
func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
