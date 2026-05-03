// Package nbd implements an NBD (Network Block Device) server backed by a volume.Manager.
// NBD protocol spec: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
package nbd

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/volume"
)

// ReadIndexer provides linearizable read fencing. Implemented by cluster.DistributedBackend.
type ReadIndexer interface {
	ReadIndex(ctx context.Context) (uint64, error)
	WaitApplied(ctx context.Context, index uint64) error
}

// pendingMutation pairs a block key with its deferred Raft commit function.
// key = offset (exact write-start address), used to group writes to the same block.
type pendingMutation struct {
	key uint64
	fn  func() error
}

// flushPending runs deferred Raft commits concurrently across distinct block keys
// but sequentially within each key, preserving per-block write ordering.
func flushPending(pending []pendingMutation) error {
	groups := make(map[uint64][]func() error, len(pending))
	for _, pw := range pending {
		groups[pw.key] = append(groups[pw.key], pw.fn)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(groups))
	for _, fns := range groups {
		wg.Add(1)
		go func(fns []func() error) {
			defer wg.Done()
			for _, fn := range fns {
				if err := fn(); err != nil {
					errCh <- err
					return
				}
			}
		}(fns)
	}
	wg.Wait()
	close(errCh)
	return <-errCh
}

const (
	nbdMagic        = uint64(0x4e42444d41474943) // "NBDMAGIC"
	nbdOptionMagic  = uint64(0x49484156454F5054) // "IHAVEOPT"
	nbdReplyMagic   = uint32(0x67446698)
	nbdRequestMagic = uint32(0x25609513)

	// Newstyle handshake flags (server)
	nbdFlagFixedNewstyle = uint16(1 << 0)

	// Newstyle handshake flags (client)
	nbdFlagClientFixedNewstyle = uint32(1 << 0)
	nbdFlagClientNoZeroes      = uint32(1 << 1)
	nbdKnownClientFlags        = nbdFlagClientFixedNewstyle | nbdFlagClientNoZeroes

	// Transmission flags
	nbdFlagHasFlags        = uint16(1 << 0)
	nbdFlagSendFlush       = uint16(1 << 2)
	nbdFlagSendTrim        = uint16(1 << 5)
	nbdFlagSendWriteZeroes = uint16(1 << 6)
	nbdFlagSendDF          = uint16(1 << 7)

	// Option types
	nbdOptExportName      = uint32(1)
	nbdOptAbort           = uint32(2)
	nbdOptList            = uint32(3)
	nbdOptInfo            = uint32(6)
	nbdOptGo              = uint32(7)
	nbdOptStructuredReply = uint32(8)
	nbdOptListMetaContext = uint32(9)
	nbdOptSetMetaContext  = uint32(10)
	nbdOptExtendedHeaders = uint32(11)

	// Option reply types
	nbdRepAck        = uint32(1)
	nbdRepServer     = uint32(2)
	nbdRepInfo       = uint32(3)
	nbdRepErrUnsup   = uint32(1 | (1 << 31))
	nbdRepErrInvalid = uint32(3 | (1 << 31))
	nbdRepErrUnknown = uint32(6 | (1 << 31))
	nbdOptReplyMagic = uint64(0x3e889045565a9)

	// Info types
	nbdInfoExport    = uint16(0)
	nbdInfoBlockSize = uint16(3)

	// Commands
	nbdCmdRead        = uint32(0)
	nbdCmdWrite       = uint32(1)
	nbdCmdDisc        = uint32(2)
	nbdCmdFlush       = uint32(3)
	nbdCmdTrim        = uint32(4)
	nbdCmdWriteZeroes = uint32(6)
	nbdCmdBlockStatus = uint32(7)

	// Command flags
	nbdCmdFlagNoHole   = uint16(1 << 1)
	nbdCmdFlagDF       = uint16(1 << 2)
	nbdCmdFlagReqOne   = uint16(1 << 3)
	nbdCmdFlagFastZero = uint16(1 << 4)

	// Size constraints
	nbdMinBlockSize       = uint32(1)
	nbdPreferredBlockSize = uint32(volume.DefaultBlockSize)
	nbdMaxPayloadSize     = uint32(32 * 1024 * 1024)

	// Structured and extended protocol values.
	nbdStructuredReplyMagic   = uint32(0x668e33ef)
	nbdExtendedRequestMagic   = uint32(0x21e41c71)
	nbdReplyFlagDone          = uint16(1 << 0)
	nbdReplyTypeOffsetData    = uint16(1)
	nbdReplyTypeBlockStatus   = uint16(5)
	nbdReplyTypeError         = uint16(32769)
	nbdMetaContextBaseAllocID = uint32(1)

	// NBD errno values.
	nbdErrEIO    = uint32(5)
	nbdErrEINVAL = uint32(22)
)

// nbdPoolBufSize is the buffer size that the pool recycles. Matches the
// default NBD block size (4 KiB) and the most common fio workload size.
const nbdPoolBufSize = 4096

// Server serves a single volume over NBD protocol.
type Server struct {
	mgr         *volume.Manager
	volName     string
	listener    atomic.Pointer[net.Listener]
	closed      atomic.Bool
	bufPool     *pool.Pool[[]byte]
	readIndexer ReadIndexer // nil = no gate (single-node / non-distributed)
}

// NewServer creates a new NBD server for the named volume.
func NewServer(mgr *volume.Manager, volName string) *Server {
	return &Server{
		mgr:     mgr,
		volName: volName,
		bufPool: pool.New(func() []byte { return make([]byte, nbdPoolBufSize) }),
	}
}

// SetReadIndexer wires a ReadIndexer so NBD reads are linearizable.
func (s *Server) SetReadIndexer(ri ReadIndexer) {
	s.readIndexer = ri
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
	if _, err := s.newstyleHandshake(conn, vol); err != nil {
		log.Error().Err(err).Msg("nbd: handshake failed")
		return
	}

	// Command loop — pending accumulates deferred Raft commits for write-back.
	var pending []pendingMutation
	for {
		if err := s.handleRequest(conn, &pending); err != nil {
			// Drain remaining deferred commits on disconnect so write-back
			// data reaches Raft even without an explicit NBD_CMD_FLUSH.
			for _, pw := range pending {
				pw.fn() //nolint:errcheck
			}
			if err == io.EOF {
				return
			}
			log.Error().Err(err).Msg("nbd: request failed")
			return
		}
	}
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

func (s *Server) handleRequest(conn net.Conn, pending *[]pendingMutation) error {
	req, err := readNBDRequest(conn)
	if err != nil {
		return err
	}

	if err := validateRequestSize(req); err != nil {
		return s.sendReply(conn, req.handle[:], nbdErrEINVAL, nil)
	}

	switch req.typ {
	case nbdCmdRead:
		if ri := s.readIndexer; ri != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			idx, err := ri.ReadIndex(ctx)
			cancel()
			if err == nil {
				ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
				err = ri.WaitApplied(ctx2, idx)
				cancel2()
			}
			if err != nil {
				return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
			}
		}
		buf := s.getBuf(uint32(req.length))
		if _, err := s.mgr.ReadAt(s.volName, buf, int64(req.offset)); err != nil && err != io.EOF {
			s.putBuf(buf)
			return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
		}
		err := s.sendReply(conn, req.handle[:], 0, buf)
		s.putBuf(buf)
		return err

	case nbdCmdWrite:
		buf := s.getBuf(uint32(req.length))
		if _, err := io.ReadFull(conn, buf); err != nil {
			s.putBuf(buf)
			return fmt.Errorf("read write data: %w", err)
		}
		// Write-back: ack after local disk write; Raft commit deferred to flush.
		commits, _, err := s.mgr.WriteAtDeferred(s.volName, buf, int64(req.offset))
		s.putBuf(buf)
		if err != nil {
			return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
		}
		for _, fn := range commits {
			*pending = append(*pending, pendingMutation{key: req.offset, fn: fn})
		}
		return s.sendReply(conn, req.handle[:], 0, nil)

	case nbdCmdDisc:
		return io.EOF

	case nbdCmdFlush:
		if err := flushPending(*pending); err != nil {
			*pending = (*pending)[:0]
			return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
		}
		*pending = (*pending)[:0]
		return s.sendReply(conn, req.handle[:], 0, nil)

	case nbdCmdTrim:
		if err := flushPending(*pending); err != nil {
			*pending = (*pending)[:0]
			return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
		}
		*pending = (*pending)[:0]
		if err := s.mgr.Discard(s.volName, int64(req.offset), int64(req.length)); err != nil {
			return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
		}
		return s.sendReply(conn, req.handle[:], 0, nil)

	case nbdCmdWriteZeroes:
		if req.flags&nbdCmdFlagFastZero != 0 || req.flags&^nbdCmdFlagNoHole != 0 {
			return s.sendReply(conn, req.handle[:], nbdErrEINVAL, nil)
		}
		if err := s.writeZeroes(req.offset, uint32(req.length), pending); err != nil {
			return s.sendReply(conn, req.handle[:], nbdErrEIO, nil)
		}
		return s.sendReply(conn, req.handle[:], 0, nil)

	default:
		return s.sendReply(conn, req.handle[:], nbdErrEINVAL, nil)
	}
}

func (s *Server) writeZeroes(offset uint64, length uint32, pending *[]pendingMutation) error {
	if length == 0 {
		return nil
	}
	zeroes := s.getBuf(nbdPoolBufSize)
	clear(zeroes)
	defer s.putBuf(zeroes)

	remaining := length
	pos := offset
	for remaining > 0 {
		n := min(remaining, uint32(len(zeroes)))
		commits, _, err := s.mgr.WriteAtDeferred(s.volName, zeroes[:n], int64(pos))
		if err != nil {
			return err
		}
		for _, fn := range commits {
			*pending = append(*pending, pendingMutation{key: pos, fn: fn})
		}
		remaining -= n
		pos += uint64(n)
	}
	return nil
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
