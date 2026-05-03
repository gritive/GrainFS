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
// It ensures the NFS storage bucket exists.
func NewServer(backend storage.Backend) *Server {
	s := &Server{
		backend: backend,
		state:   NewStateManager(),
		logger:  log.With().Str("component", "nfs4").Logger(),
	}
	if err := backend.HeadBucket(nfs4Bucket); err != nil {
		if err := backend.CreateBucket(nfs4Bucket); err != nil {
			s.logger.Warn().Err(err).Str("bucket", nfs4Bucket).Msg("nfs4: could not create storage bucket")
		}
	}
	return s
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

	s.logger.Debug().Str("remote", conn.RemoteAddr().String()).Msg("nfs4: new connection")

	var frameBuf []byte
	for {
		frame, err := readRPCFrameInto(conn, frameBuf)
		if err != nil {
			return
		}
		frameBuf = frame

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
		} else {
			s.logger.Debug().
				Uint32("prog", header.Program).
				Uint32("vers", header.ProgVers).
				Uint32("proc", header.Procedure).
				Msg("non-compound RPC call")
		}

		writeRPCFrame(conn, w.Bytes())
		putXDRWriter(w)
	}
}

func (s *Server) handleCompoundInto(data []byte, w *XDRWriter) {
	req := compoundReqPool.Get()
	req.Tag = ""
	req.MinorVer = 0
	req.Ops = req.Ops[:0]
	defer compoundReqPool.Put(req)

	if err := ParseCompound(data, req); err != nil {
		s.logger.Debug().Err(err).Msg("COMPOUND parse error")
		encodeCompoundResponseInto(w, &CompoundResponse{Status: NFS4ERR_INVAL})
		return
	}

	if e := s.logger.Debug(); e.Enabled() {
		ops := make([]int, len(req.Ops))
		for i, op := range req.Ops {
			ops[i] = op.OpCode
		}
		e.Uint32("minorver", req.MinorVer).Ints("ops", ops).Msg("nfs4: COMPOUND")
	}

	d := getDispatcher(s.backend, s.state)
	defer putDispatcher(d)

	resp := compoundRespPool.Get()
	resp.Status = NFS4_OK
	resp.Tag = ""
	resp.Results = resp.Results[:0]
	defer compoundRespPool.Put(resp)

	d.Dispatch(req, resp)

	if d.replayFull != nil {
		// Full cached COMPOUND response from SEQUENCE replay cache; write directly.
		w.buf.Write(d.replayFull)
		return
	}

	if d.pendingCacheSlot != nil {
		// cacheThis=1: encode to []byte so we can cache and write in one pass.
		respBytes := EncodeCompoundResponse(resp)
		w.buf.Write(respBytes)
		d.storePendingCache(respBytes)
	} else {
		encodeCompoundResponseInto(w, resp)
	}
}
