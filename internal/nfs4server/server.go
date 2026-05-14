package nfs4server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

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
	exports  atomic.Pointer[exportSnap]
	hinter   *unknownExportHinter
	lookups  *LookupRing

	exportSource exportSource
}

// NewServer creates an NFSv4 server backed by the given storage backend.
//
// Phase 0b (D6): boot-time auto-creation of "__grainfs_nfs4" has been removed.
// Operators must explicitly create buckets and register them as NFS exports
// (Phase 1+: grainfs nfs export add <bucket>). Legacy data recovery:
//
//	grainfs bucket create __grainfs_nfs4
//	grainfs nfs export add __grainfs_nfs4
func NewServer(backend storage.Backend) *Server {
	s := &Server{
		backend: backend,
		state:   NewStateManager(),
		logger:  log.With().Str("component", "nfs4").Logger(),
		hinter:  newUnknownExportHinter(hinterTTL),
		lookups: NewLookupRing(1024),
	}
	s.exports.Store(emptySnap)
	if err := s.RefreshExports(context.Background()); err != nil {
		s.logger.Warn().Err(err).Msg("nfs4: initial RefreshExports failed; running with empty export set")
	}
	return s
}

// ListenAndServe starts the NFSv4 TCP server.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nfs4 listen: %w", err)
	}
	return s.Serve(ln)
}

// Serve accepts NFSv4 connections from an already-open listener.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.logger.Info().Str("addr", ln.Addr().String()).Msg("nfs4 server started")

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
	if s.hinter != nil {
		s.hinter.Close()
	}
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

// Invalidate clears NFS metadata caches for an object that was mutated
// out-of-band (e.g. an S3 PUT replicated via Raft from another cluster
// node, dispatched here through cluster.Registry.InvalidateAll).
//
// Implements the cluster.CacheInvalidator interface shape via duck typing
// so internal/nfs4server does not import internal/cluster. The Server is
// registered in serveruntime.Run after StartNodeServices returns.
//
// Phase 0b (D6): the legacy dedicated-bucket filter was removed. Phase 3 will
// restore export-aware filtering using the active export registry.
func (s *Server) Invalidate(bucket string, key string) {
	if !s.isExportRegistered(bucket) {
		return
	}
	s.state.InvalidateObject(bucket, key)
}

// connRPCConcurrency caps the number of NFSv4 COMPOUND RPCs processed
// concurrently per TCP connection. NFSv4 clients (Linux) typically use a
// single connection per mount and rely on XID-based matching for out-of-order
// replies, so per-connection parallelism is the dominant throughput axis for
// fio-style multi-threaded workloads. The cap protects against runaway
// goroutine fan-out from a single misbehaving client.
const connRPCConcurrency = 64

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	s.logger.Debug().Str("remote", conn.RemoteAddr().String()).Msg("nfs4: new connection")

	var writeMu sync.Mutex
	sem := make(chan struct{}, connRPCConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		// Frame buffers come from a pool: per-RPC concurrent processing means
		// the worker needs to read its own bytes, so reusing one connection-
		// local buffer would alias. The pool keeps alloc churn bounded.
		frame, err := readRPCFrameInto(conn, getFrameBuf())
		if err != nil {
			return
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(f []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			defer putFrameBuf(f)

			header, args, err := ParseRPCCall(f)
			if err != nil {
				s.logger.Debug().Err(err).Msg("RPC parse error")
				return
			}

			w := getXDRWriter()
			defer putXDRWriter(w)

			w.WriteUint32(header.XID)
			w.WriteUint32(rpcMsgReply)
			w.WriteUint32(0)        // MSG_ACCEPTED
			w.WriteUint32(authNone) // verifier flavor
			w.WriteUint32(0)        // verifier body length
			w.WriteUint32(0)        // ACCEPT_SUCCESS

			if header.Program == rpcProgNFS && header.ProgVers == rpcVersNFS4 && header.Procedure == 1 {
				s.handleCompoundIntoFrom(args, w, conn.RemoteAddr().String())
			} else {
				s.logger.Debug().
					Uint32("prog", header.Program).
					Uint32("vers", header.ProgVers).
					Uint32("proc", header.Procedure).
					Msg("non-compound RPC call")
			}

			writeMu.Lock()
			werr := writeRPCFrame(conn, w.Bytes())
			writeMu.Unlock()
			if werr != nil {
				s.logger.Debug().Err(werr).Msg("nfs: write rpc frame")
				_ = conn.Close()
			}
		}(frame)
	}
}

func (s *Server) handleCompoundIntoFrom(data []byte, w *XDRWriter, clientAddr string) {
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

	d := getDispatcherWithClient(s.backend, s.state, s, clientAddr, s.hinter)
	defer putDispatcher(d)

	resp := compoundRespPool.Get()
	resp.Status = NFS4_OK
	resp.Tag = ""
	resp.Results = resp.Results[:0]
	defer putCompoundResponse(resp)

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

func (s *Server) isExportReadOnly(bucket string) bool {
	if bucket == "" {
		return false
	}
	cfg, ok := s.loadExports().byBucket[bucket]
	return ok && cfg.readOnly
}

func (s *Server) isExportRegistered(bucket string) bool {
	if bucket == "" {
		return false
	}
	_, ok := s.loadExports().byBucket[bucket]
	return ok
}

func (s *Server) exportGeneration(bucket string) uint64 {
	cfg, ok := s.loadExports().byBucket[bucket]
	if !ok {
		return 0
	}
	return cfg.generation
}

func (s *Server) exportFSID(bucket string) (uint64, uint64) {
	cfg := s.loadExports().byBucket[bucket]
	return cfg.fsidMajor, cfg.fsidMinor
}

func (s *Server) RecentLookups(bucket string, window time.Duration) []LookupRecord {
	if s == nil {
		return nil
	}
	return s.lookups.Snapshot(bucket, window)
}

func (s *Server) ActiveMountClients(bucket string) []string {
	return nil
}
