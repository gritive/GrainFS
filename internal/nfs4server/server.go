package nfs4server

import (
	"fmt"
	"net"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// Server handles NFSv4.0 connections over TCP.
type Server struct {
	backend  storage.Backend
	state    *StateManager
	mu       sync.Mutex
	listener net.Listener
	logger   zerolog.Logger
}

// NewServer creates an NFSv4 server backed by the given storage backend.
func NewServer(backend storage.Backend) *Server {
	return &Server{
		backend: backend,
		state:   NewStateManager(),
		logger:  log.With().Str("component", "nfs4").Logger(),
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

	s.logger.Info().Str("addr", addr).Msg("nfs4 server started")

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
			s.logger.Debug().Err(err).Msg("RPC parse error")
			continue
		}

		w := getXDRWriter()
		w.WriteUint32(header.XID)
		w.WriteUint32(rpcMsgReply)
		w.WriteUint32(0)        // MSG_ACCEPTED
		w.WriteUint32(authNone) // verifier flavor
		w.WriteUint32(0)        // verifier body length
		w.WriteUint32(0)        // ACCEPT_SUCCESS

		if header.Program == rpcProgNFS && header.ProgVers == rpcVersNFS4 && header.Procedure == 1 {
			s.handleCompoundInto(args, w)
		}

		writeRPCFrame(conn, w.Bytes())
		putXDRWriter(w)
	}
}

func (s *Server) handleCompoundInto(data []byte, w *XDRWriter) {
	req := compoundReqPool.Get().(*CompoundRequest)
	req.Tag = ""
	req.MinorVer = 0
	req.Ops = req.Ops[:0]
	defer compoundReqPool.Put(req)

	if err := ParseCompound(data, req); err != nil {
		s.logger.Debug().Err(err).Msg("COMPOUND parse error")
		encodeCompoundResponseInto(w, &CompoundResponse{Status: NFS4ERR_INVAL})
		return
	}

	d := getDispatcher(s.backend, s.state)
	defer putDispatcher(d)

	resp := compoundRespPool.Get().(*CompoundResponse)
	resp.Status = NFS4_OK
	resp.Tag = ""
	resp.Results = resp.Results[:0]
	defer compoundRespPool.Put(resp)

	d.Dispatch(req, resp)
	encodeCompoundResponseInto(w, resp)
}
