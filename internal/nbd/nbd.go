//go:build linux

// Package nbd implements an NBD (Network Block Device) server backed by a volume.Manager.
// NBD protocol spec: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
package nbd

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/gritive/GrainFS/internal/volume"
)

const (
	nbdMagic       = uint64(0x4e42444d41474943) // "NBDMAGIC"
	nbdOptionMagic = uint64(0x49484156454F5054) // "IHAVEOPT"
	nbdReplyMagic  = uint32(0x67446698)
	nbdRequestMagic = uint32(0x25609513)

	// Newstyle handshake flags (server)
	nbdFlagFixedNewstyle = uint16(1 << 0)

	// Transmission flags
	nbdFlagHasFlags  = uint16(1 << 0)
	nbdFlagSendFlush = uint16(1 << 2)

	// Option types
	nbdOptExportName = uint32(1)
	nbdOptAbort      = uint32(2)
	nbdOptList       = uint32(3)
	nbdOptGo         = uint32(7)

	// Option reply types
	nbdRepAck           = uint32(1)
	nbdRepServer        = uint32(2)
	nbdRepInfo          = uint32(3)
	nbdRepErrUnsup      = uint32(1 | (1 << 31))
	nbdOptReplyMagic    = uint64(0x3e889045565a9)

	// Info types
	nbdInfoExport = uint16(0)

	// Commands
	nbdCmdRead  = uint32(0)
	nbdCmdWrite = uint32(1)
	nbdCmdDisc  = uint32(2)
	nbdCmdFlush = uint32(3)
)

// Server serves a single volume over NBD protocol.
type Server struct {
	mgr      *volume.Manager
	volName  string
	listener net.Listener
	mu       sync.Mutex
	closed   bool
}

// NewServer creates a new NBD server for the named volume.
func NewServer(mgr *volume.Manager, volName string) *Server {
	return &Server{mgr: mgr, volName: volName}
}

// ListenAndServe starts the NBD server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nbd listen: %w", err)
	}
	slog.Info("nbd server started", "component", "nbd", "addr", addr, "volume", s.volName)
	return s.Serve(ln)
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			slog.Error("nbd accept error", "error", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// Close stops the NBD server.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("nbd: recovered panic in connection handler", "panic", r)
		}
	}()

	vol, err := s.mgr.Get(s.volName)
	if err != nil {
		slog.Error("nbd: volume not found", "volume", s.volName, "error", err)
		return
	}

	// Newstyle handshake
	if err := s.newstyleHandshake(conn, vol); err != nil {
		slog.Error("nbd: handshake failed", "error", err)
		return
	}

	// Command loop
	for {
		if err := s.handleRequest(conn); err != nil {
			if err == io.EOF {
				return
			}
			slog.Error("nbd: request failed", "error", err)
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
	binary.BigEndian.PutUint16(buf[8:10], nbdFlagHasFlags|nbdFlagSendFlush)
	// rest is zeros
	_, err := conn.Write(buf)
	return err
}

func (s *Server) handleOptGo(conn net.Conn, vol *volume.Volume, optType uint32) error {
	// Send NBD_REP_INFO with NBD_INFO_EXPORT
	info := make([]byte, 12)
	binary.BigEndian.PutUint16(info[0:2], nbdInfoExport)
	binary.BigEndian.PutUint64(info[2:10], uint64(vol.Size))
	binary.BigEndian.PutUint16(info[10:12], nbdFlagHasFlags|nbdFlagSendFlush)
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
		buf := make([]byte, length)
		_, _ = s.mgr.ReadAt(s.volName, buf, int64(offset))
		return s.sendReply(conn, handle, 0, buf)

	case nbdCmdWrite:
		buf := make([]byte, length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return fmt.Errorf("read write data: %w", err)
		}
		_, _ = s.mgr.WriteAt(s.volName, buf, int64(offset))
		return s.sendReply(conn, handle, 0, nil)

	case nbdCmdDisc:
		return io.EOF

	case nbdCmdFlush:
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
