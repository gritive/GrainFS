// Package nbd implements an NBD (Network Block Device) server backed by a volume.Manager.
// NBD protocol spec: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
package nbd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/volume"
)

const (
	nbdMagic        = uint64(0x4e42444d41474943) // "NBDMAGIC"
	nbdOptionMagic  = uint64(0x49484156454F5054) // "IHAVEOPT"
	nbdReplyMagic   = uint32(0x67446698)
	nbdRequestMagic = uint32(0x25609513)

	// Newstyle handshake flags (server)
	nbdFlagFixedNewstyle = uint16(1 << 0)

	// Transmission flags
	nbdFlagHasFlags  = uint16(1 << 0)
	nbdFlagSendFlush = uint16(1 << 2)
	nbdFlagSendTrim  = uint16(1 << 5) // NBD spec: bit 5

	// Option types
	nbdOptExportName = uint32(1)
	nbdOptAbort      = uint32(2)
	nbdOptList       = uint32(3)
	nbdOptGo         = uint32(7)

	// Option reply types
	nbdRepAck        = uint32(1)
	nbdRepServer     = uint32(2)
	nbdRepInfo       = uint32(3)
	nbdRepErrUnsup   = uint32(1 | (1 << 31))
	nbdOptReplyMagic = uint64(0x3e889045565a9)

	// Info types
	nbdInfoExport = uint16(0)

	// Commands
	nbdCmdRead  = uint32(0)
	nbdCmdWrite = uint32(1)
	nbdCmdDisc  = uint32(2)
	nbdCmdFlush = uint32(3)
	nbdCmdTrim  = uint32(4)
)

// nbdPoolBufSize is the buffer size that the pool recycles. Matches the
// default NBD block size (4 KiB) and the most common fio workload size.
const nbdPoolBufSize = 4096

// Server serves a single volume over NBD protocol.
type Server struct {
	mgr      *volume.Manager
	volName  string
	listener atomic.Pointer[net.Listener]
	closed   atomic.Bool
	bufPool  *pool.Pool[[]byte]
}

// NewServer creates a new NBD server for the named volume.
func NewServer(mgr *volume.Manager, volName string) *Server {
	return &Server{
		mgr:     mgr,
		volName: volName,
		bufPool: pool.New(func() []byte { return make([]byte, nbdPoolBufSize) }),
	}
}

// ListenAndServe starts the NBD server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nbd listen: %w", err)
	}
	log.Info().Str("component", "nbd").Str("addr", addr).Str("volume", s.volName).Msg("nbd server started")
	return s.Serve(ln)
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.listener.Store(&ln)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			log.Error().Err(err).Msg("nbd accept error")
			continue
		}
		go s.handleConn(conn)
	}
}

// Close stops the NBD server.
func (s *Server) Close() error {
	s.closed.Store(true)
	if ln := s.listener.Load(); ln != nil {
		return (*ln).Close()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("panic", fmt.Sprintf("%v", r)).Msg("nbd: recovered panic in connection handler")
		}
	}()

	vol, err := s.mgr.Get(s.volName)
	if err != nil {
		log.Error().Str("volume", s.volName).Err(err).Msg("nbd: volume not found")
		return
	}

	// Newstyle handshake
	if err := s.newstyleHandshake(conn, vol); err != nil {
		log.Error().Err(err).Msg("nbd: handshake failed")
		return
	}

	// Command loop
	for {
		if err := s.handleRequest(conn); err != nil {
			if err == io.EOF {
				return
			}
			log.Error().Err(err).Msg("nbd: request failed")
			return
		}
	}
}

func (s *Server) newstyleHandshake(conn net.Conn, vol *volume.Volume) error {
	// Step 1: Server sends initial newstyle header
	// NBDMAGIC (8) + IHAVEOPT (8) + handshake flags (2) = 18 bytes
	hdr := make([]byte, 18)
	binary.BigEndian.PutUint64(hdr[0:8], nbdMagic)
	binary.BigEndian.PutUint64(hdr[8:16], nbdOptionMagic)
	binary.BigEndian.PutUint16(hdr[16:18], nbdFlagFixedNewstyle)
	if _, err := conn.Write(hdr); err != nil {
		return fmt.Errorf("write server header: %w", err)
	}

	// Step 2: Client sends client flags (4 bytes)
	var clientFlags [4]byte
	if _, err := io.ReadFull(conn, clientFlags[:]); err != nil {
		return fmt.Errorf("read client flags: %w", err)
	}

	// Step 3: Option haggling loop
	for {
		// Read option header: IHAVEOPT(8) + option(4) + length(4) = 16
		var optHdr [16]byte
		if _, err := io.ReadFull(conn, optHdr[:]); err != nil {
			return fmt.Errorf("read option header: %w", err)
		}

		magic := binary.BigEndian.Uint64(optHdr[0:8])
		if magic != nbdOptionMagic {
			return fmt.Errorf("bad option magic: %x", magic)
		}

		optType := binary.BigEndian.Uint32(optHdr[8:12])
		optLen := binary.BigEndian.Uint32(optHdr[12:16])

		// Read option data
		var optData []byte
		if optLen > 0 {
			optData = make([]byte, optLen)
			if _, err := io.ReadFull(conn, optData); err != nil {
				return fmt.Errorf("read option data: %w", err)
			}
		}

		switch optType {
		case nbdOptExportName:
			// Client selected an export — send export info and enter transmission
			return s.sendExportData(conn, vol)

		case nbdOptGo:
			// NBD_OPT_GO: send info replies then enter transmission
			return s.handleOptGo(conn, vol, optType)

		case nbdOptList:
			// List exports — we only have one
			if err := s.handleOptList(conn, optType); err != nil {
				return err
			}

		case nbdOptAbort:
			s.sendOptReply(conn, optType, nbdRepAck, nil)
			return fmt.Errorf("client aborted")

		default:
			// Unknown option — reply unsupported
			s.sendOptReply(conn, optType, nbdRepErrUnsup, nil)
		}
	}
}

func (s *Server) sendExportData(conn net.Conn, vol *volume.Volume) error {
	// For NBD_OPT_EXPORT_NAME: send size(8) + transmission flags(2) + zeros(124)
	buf := make([]byte, 134)
	binary.BigEndian.PutUint64(buf[0:8], uint64(vol.Size))
	binary.BigEndian.PutUint16(buf[8:10], nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendTrim)
	// rest is zeros
	_, err := conn.Write(buf)
	return err
}

func (s *Server) handleOptGo(conn net.Conn, vol *volume.Volume, optType uint32) error {
	// Send NBD_REP_INFO with NBD_INFO_EXPORT
	info := make([]byte, 12)
	binary.BigEndian.PutUint16(info[0:2], nbdInfoExport)
	binary.BigEndian.PutUint64(info[2:10], uint64(vol.Size))
	binary.BigEndian.PutUint16(info[10:12], nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendTrim)
	if err := s.sendOptReply(conn, optType, nbdRepInfo, info); err != nil {
		return err
	}

	// Send NBD_REP_ACK to finish
	return s.sendOptReply(conn, optType, nbdRepAck, nil)
}

func (s *Server) handleOptList(conn net.Conn, optType uint32) error {
	// Send one export: our volume name
	nameBytes := []byte(s.volName)
	data := make([]byte, 4+len(nameBytes))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(nameBytes)))
	copy(data[4:], nameBytes)
	if err := s.sendOptReply(conn, optType, nbdRepServer, data); err != nil {
		return err
	}
	return s.sendOptReply(conn, optType, nbdRepAck, nil)
}

func (s *Server) sendOptReply(conn net.Conn, optType, replyType uint32, data []byte) error {
	// Option reply: magic(8) + option(4) + reply_type(4) + length(4) + data
	hdr := make([]byte, 20)
	binary.BigEndian.PutUint64(hdr[0:8], nbdOptReplyMagic)
	binary.BigEndian.PutUint32(hdr[8:12], optType)
	binary.BigEndian.PutUint32(hdr[12:16], replyType)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(data)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// getBuf returns a buffer of exactly length bytes. Pooled for nbdPoolBufSize.
func (s *Server) getBuf(length uint32) []byte {
	if length == nbdPoolBufSize {
		return s.bufPool.Get()
	}
	return make([]byte, length)
}

// putBuf returns buf to the pool if it was pool-allocated.
func (s *Server) putBuf(buf []byte) {
	if len(buf) == nbdPoolBufSize {
		s.bufPool.Put(buf)
	}
}

func (s *Server) handleRequest(conn net.Conn) error {
	var hdr [28]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return err
	}

	magic := binary.BigEndian.Uint32(hdr[0:4])
	if magic != nbdRequestMagic {
		return fmt.Errorf("bad request magic: %x", magic)
	}

	// Standard NBD request: magic(4) + flags(2) + type(2) + handle(8) + offset(8) + length(4)
	cmdType := uint32(binary.BigEndian.Uint16(hdr[6:8]))
	handle := hdr[8:16]
	offset := binary.BigEndian.Uint64(hdr[16:24])
	length := binary.BigEndian.Uint32(hdr[24:28])

	switch cmdType {
	case nbdCmdRead:
		buf := s.getBuf(length)
		_, _ = s.mgr.ReadAt(s.volName, buf, int64(offset))
		err := s.sendReply(conn, handle, 0, buf)
		s.putBuf(buf)
		return err

	case nbdCmdWrite:
		buf := s.getBuf(length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			s.putBuf(buf)
			return fmt.Errorf("read write data: %w", err)
		}
		_, _ = s.mgr.WriteAt(s.volName, buf, int64(offset))
		s.putBuf(buf)
		return s.sendReply(conn, handle, 0, nil)

	case nbdCmdDisc:
		return io.EOF

	case nbdCmdFlush:
		return s.sendReply(conn, handle, 0, nil)

	case nbdCmdTrim:
		if err := s.mgr.Discard(s.volName, int64(offset), int64(length)); err != nil {
			return s.sendReply(conn, handle, 5, nil) // EIO
		}
		return s.sendReply(conn, handle, 0, nil)

	default:
		return s.sendReply(conn, handle, 22, nil) // EINVAL
	}
}

func (s *Server) sendReply(conn net.Conn, handle []byte, errCode uint32, data []byte) error {
	hdr := make([]byte, 16)
	binary.BigEndian.PutUint32(hdr[0:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(hdr[4:8], errCode)
	copy(hdr[8:16], handle)

	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	if data != nil {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}
	return nil
}
