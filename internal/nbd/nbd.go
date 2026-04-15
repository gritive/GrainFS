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
	nbdMagic         = uint64(0x4e42444d41474943)   // "NBDMAGIC"
	nbdOptionMagic   = uint64(0x49484156454F5054)   // "IHAVEOPT"
	nbdReplyMagic    = uint32(0x67446698)
	nbdRequestMagic  = uint32(0x25609513)
	nbdFlagHasFlags  = uint16(1 << 0)
	nbdFlagSendFlush = uint16(1 << 2)

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
	s.listener = ln
	slog.Info("nbd server started", "component", "nbd", "addr", addr, "volume", s.volName)

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

	vol, err := s.mgr.Get(s.volName)
	if err != nil {
		slog.Error("nbd: volume not found", "volume", s.volName, "error", err)
		return
	}

	// Oldstyle handshake (simpler, single export)
	if err := s.sendHandshake(conn, vol); err != nil {
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

func (s *Server) sendHandshake(conn net.Conn, vol *volume.Volume) error {
	// Oldstyle negotiation
	buf := make([]byte, 152)
	binary.BigEndian.PutUint64(buf[0:8], nbdMagic)
	binary.BigEndian.PutUint64(buf[8:16], 0x00420281861253)   // cliserv magic
	binary.BigEndian.PutUint64(buf[16:24], uint64(vol.Size))
	binary.BigEndian.PutUint32(buf[24:28], uint32(nbdFlagHasFlags|nbdFlagSendFlush))
	// rest is zeros (reserved + padding)
	_, err := conn.Write(buf)
	return err
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

	// flags := binary.BigEndian.Uint16(hdr[4:6]) // unused currently
	cmdType := binary.BigEndian.Uint32(hdr[4:8]) >> 16 // type is in bits 16-31 of word
	// Actually: type is at offset 6-7 as uint16 in newer spec, but let's use the standard layout
	// Standard NBD request: magic(4) + flags(2) + type(2) + handle(8) + offset(8) + length(4)
	cmdType = uint32(binary.BigEndian.Uint16(hdr[6:8]))
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
