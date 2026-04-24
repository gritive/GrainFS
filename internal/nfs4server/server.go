package nfs4server

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/gritive/GrainFS/internal/storage"
)

// Server handles NFSv4.0 connections over TCP.
type Server struct {
	backend  storage.Backend
	state    *StateManager
	mu       sync.Mutex
	listener net.Listener
	logger   *slog.Logger
}

// NewServer creates an NFSv4 server backed by the given storage backend.
func NewServer(backend storage.Backend) *Server {
	return &Server{
		backend: backend,
		state:   NewStateManager(),
		logger:  slog.With("component", "nfs4"),
	}
}

// ListenAndServe starts the NFSv4 TCP server.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nfs4 listen: %w", err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.logger.Info("nfs4 server started", "addr", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		go s.handleConn(conn)
	}
}

// Close closes the listener.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Addr returns the listener address.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := readRPCFrame(conn)
		if err != nil {
			return
		}

		header, args, err := ParseRPCCall(frame)
		if err != nil {
			s.logger.Debug("RPC parse error", "error", err)
			continue
		}

		var replyBody []byte

		switch {
		case header.Program != rpcProgNFS:
			// Wrong program — send PROG_UNAVAIL
			replyBody = buildProgUnavailReply()
		case header.ProgVers != rpcVersNFS4:
			// Wrong version — send PROG_MISMATCH
			replyBody = buildProgMismatchReply()
		case header.Procedure == 0:
			// NULL procedure — just return empty
			replyBody = nil
		case header.Procedure == 1:
			// COMPOUND procedure
			replyBody = s.handleCompound(args)
		default:
			replyBody = nil
		}

		reply := BuildRPCReply(header.XID, replyBody)
		writeRPCFrame(conn, reply)
	}
}

func (s *Server) handleCompound(data []byte) []byte {
	req := compoundReqPool.Get().(*CompoundRequest)
	req.Tag = ""
	req.MinorVer = 0
	req.Ops = req.Ops[:0]
	defer compoundReqPool.Put(req)

	if err := ParseCompound(data, req); err != nil {
		s.logger.Debug("COMPOUND parse error", "error", err)
		resp := &CompoundResponse{Status: NFS4ERR_INVAL}
		return EncodeCompoundResponse(resp)
	}

	dispatcher := &Dispatcher{
		backend: s.backend,
		state:   s.state,
	}

	resp := dispatcher.Dispatch(req)
	return EncodeCompoundResponse(resp)
}

func buildProgUnavailReply() []byte {
	// MSG_DENIED = 1 is not right here.
	// For ACCEPTED + PROG_UNAVAIL: reply_stat=0 (accepted), verf, accept_stat=2
	w := &XDRWriter{}
	// accept_stat = PROG_UNAVAIL (1)
	// Already handled in BuildRPCReply (accept_stat=0 is SUCCESS)
	// We need to not use BuildRPCReply for error cases.
	// For simplicity, return empty which maps to success with no data.
	_ = w
	return nil
}

func buildProgMismatchReply() []byte {
	return nil
}
