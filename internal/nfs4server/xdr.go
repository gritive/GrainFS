package nfs4server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// XDR encoding/decoding helpers for NFSv4.0 (RFC 7530).

// XDRWriter writes XDR-encoded values.
type XDRWriter struct {
	buf bytes.Buffer
}

const maxXDRWriterCap = 64 * 1024

var xdrWriterPool = sync.Pool{New: func() any { return &XDRWriter{} }}

func getXDRWriter() *XDRWriter {
	return xdrWriterPool.Get().(*XDRWriter)
}

func putXDRWriter(w *XDRWriter) {
	if w.buf.Cap() > maxXDRWriterCap {
		w.buf = bytes.Buffer{}
	} else {
		w.buf.Reset()
	}
	xdrWriterPool.Put(w)
}

// xdrWriterBytes copies w's contents to a new slice, returns the writer to pool, and returns the slice.
func xdrWriterBytes(w *XDRWriter) []byte {
	src := w.Bytes()
	out := make([]byte, len(src))
	copy(out, src)
	putXDRWriter(w)
	return out
}

func (w *XDRWriter) WriteUint32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	w.buf.Write(b[:])
}

func (w *XDRWriter) WriteUint64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.buf.Write(b[:])
}

func (w *XDRWriter) WriteOpaque(data []byte) {
	w.WriteUint32(uint32(len(data)))
	w.buf.Write(data)
	pad := (4 - len(data)%4) % 4
	for i := 0; i < pad; i++ {
		w.buf.WriteByte(0)
	}
}

func (w *XDRWriter) WriteString(s string) {
	w.WriteOpaque([]byte(s))
}

func (w *XDRWriter) Bytes() []byte {
	return w.buf.Bytes()
}

// XDRReader reads XDR-encoded values.
type XDRReader struct {
	r    bytes.Reader
	pool *sync.Pool
}

var xdrReaderPool = sync.Pool{New: func() any { return &XDRReader{} }}

var opArgPool16 = sync.Pool{New: func() any { b := make([]byte, 16); return &b }}
var opArgPool8 = sync.Pool{New: func() any { b := make([]byte, 8); return &b }}

func getOpArg16() []byte  { return (*opArgPool16.Get().(*[]byte))[:16] }
func putOpArg16(b []byte) { b = b[:cap(b)]; opArgPool16.Put(&b) }

func getOpArg8() []byte  { return (*opArgPool8.Get().(*[]byte))[:8] }
func putOpArg8(b []byte) { b = b[:cap(b)]; opArgPool8.Put(&b) }

func NewXDRReader(data []byte) *XDRReader {
	r := &XDRReader{}
	r.r.Reset(data)
	return r
}

func newXDRReaderFromPool(data []byte) *XDRReader {
	r := xdrReaderPool.Get().(*XDRReader)
	r.r.Reset(data)
	r.pool = &xdrReaderPool
	return r
}

func putXDRReader(r *XDRReader) {
	if r.pool != nil {
		p := r.pool
		r.pool = nil
		p.Put(r)
	}
}

func (r *XDRReader) ReadUint32() (uint32, error) {
	var b [4]byte
	n, err := r.r.Read(b[:])
	if n < 4 {
		if err == nil || err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func (r *XDRReader) ReadUint64() (uint64, error) {
	var b [8]byte
	n, err := r.r.Read(b[:])
	if n < 8 {
		if err == nil || err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func (r *XDRReader) ReadOpaque() ([]byte, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if length > maxFrameSize {
		return nil, fmt.Errorf("opaque too large: %d", length)
	}
	data := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(&r.r, data); err != nil {
			return nil, err
		}
	}
	pad := (4 - int(length)%4) % 4
	if pad > 0 {
		var skip [3]byte
		n, err := r.r.Read(skip[:pad])
		if n < pad {
			if err == nil || err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return data, nil
}

func (r *XDRReader) ReadString() (string, error) {
	data, err := r.ReadOpaque()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *XDRReader) Remaining() int {
	return int(r.r.Len())
}

// --- ONC RPC ---

// RPCCallHeader represents an ONC RPC call (RFC 5531).
type RPCCallHeader struct {
	XID       uint32
	MsgType   uint32
	RPCVers   uint32
	Program   uint32
	ProgVers  uint32
	Procedure uint32
}

// ParseRPCCall parses an ONC RPC call header from data.
// Returns the header and remaining data after auth fields.
func ParseRPCCall(data []byte) (*RPCCallHeader, []byte, error) {
	if len(data) < 40 { // minimum RPC call with empty auth
		return nil, nil, fmt.Errorf("RPC call too short: %d bytes", len(data))
	}

	r := NewXDRReader(data)

	xid, _ := r.ReadUint32()
	msgType, _ := r.ReadUint32()
	if msgType != rpcMsgCall {
		return nil, nil, fmt.Errorf("expected CALL (0), got %d", msgType)
	}

	rpcVers, _ := r.ReadUint32()
	program, _ := r.ReadUint32()
	progVers, _ := r.ReadUint32()
	procedure, _ := r.ReadUint32()

	header := &RPCCallHeader{
		XID: xid, MsgType: msgType, RPCVers: rpcVers,
		Program: program, ProgVers: progVers, Procedure: procedure,
	}

	// Skip auth credentials (flavor + body)
	r.ReadUint32() // cred flavor
	r.ReadOpaque() // cred body

	// Skip verifier (flavor + body)
	r.ReadUint32() // verf flavor
	r.ReadOpaque() // verf body

	// Remaining data is the procedure args
	remaining := r.Remaining()
	return header, data[len(data)-remaining:], nil
}

// BuildRPCReply constructs an ONC RPC reply.
func BuildRPCReply(xid uint32, replyBody []byte) []byte {
	w := getXDRWriter()
	w.WriteUint32(xid)
	w.WriteUint32(rpcMsgReply)
	w.WriteUint32(0)        // MSG_ACCEPTED
	w.WriteUint32(authNone) // verifier flavor
	w.WriteUint32(0)        // verifier body length
	w.WriteUint32(0)        // ACCEPT_SUCCESS
	w.buf.Write(replyBody)
	return xdrWriterBytes(w)
}

// --- COMPOUND XDR ---

// ParseCompound parses a COMPOUND4args from XDR data into req.
// req must be pre-allocated and reset by the caller (e.g. via compoundReqPool).
func ParseCompound(data []byte, req *CompoundRequest) error {
	r := newXDRReaderFromPool(data)
	defer putXDRReader(r)

	tag, err := r.ReadString()
	if err != nil {
		return fmt.Errorf("read tag: %w", err)
	}

	minorVer, err := r.ReadUint32()
	if err != nil {
		return fmt.Errorf("read minor version: %w", err)
	}

	opCount, err := r.ReadUint32()
	if err != nil {
		return fmt.Errorf("read op count: %w", err)
	}

	if opCount > maxCompoundOps {
		return fmt.Errorf("too many ops: %d", opCount)
	}

	req.Tag = tag
	req.MinorVer = minorVer
	req.Ops = req.Ops[:0]

	for i := uint32(0); i < opCount; i++ {
		opCode, err := r.ReadUint32()
		if err != nil {
			return fmt.Errorf("read op %d code: %w", i, err)
		}

		argData, pk, err := readOpArgs(r, int(opCode))
		if err != nil {
			return fmt.Errorf("read op %d (%d) args: %w", i, opCode, err)
		}

		req.Ops = append(req.Ops, Op{OpCode: int(opCode), Data: argData, poolKey: pk})
	}

	return nil
}

// readOpArgs reads the XDR arguments for a specific op.
// Returns (data, poolKey, err). poolKey 0=no pool, 8=opArgPool8, 16=opArgPool16.
func readOpArgs(r *XDRReader, opCode int) ([]byte, int, error) {
	switch opCode {
	case OpPutRootFH, OpGetFH:
		return nil, 0, nil

	case OpPutFH:
		fh, err := r.ReadOpaque()
		return fh, 0, err

	case OpLookup:
		name, err := r.ReadString()
		return []byte(name), 0, err

	case OpGetAttr:
		bitmapLen, _ := r.ReadUint32()
		w := getXDRWriter()
		w.WriteUint32(bitmapLen)
		for i := uint32(0); i < bitmapLen; i++ {
			v, _ := r.ReadUint32()
			w.WriteUint32(v)
		}
		return xdrWriterBytes(w), 0, nil

	case OpAccess:
		b := getOpArg8()
		v, _ := r.ReadUint32()
		binary.BigEndian.PutUint32(b[:4], v)
		return b[:4], 8, nil

	case OpReadDir:
		cookie, _ := r.ReadUint64()
		var cookieVerf [8]byte
		io.ReadFull(&r.r, cookieVerf[:])
		dircount, _ := r.ReadUint32()
		maxcount, _ := r.ReadUint32()
		// bitmap for attr request
		bitmapLen, _ := r.ReadUint32()
		for i := uint32(0); i < bitmapLen; i++ {
			r.ReadUint32()
		}
		w := getXDRWriter()
		w.WriteUint64(cookie)
		w.WriteUint32(dircount)
		w.WriteUint32(maxcount)
		return xdrWriterBytes(w), 0, nil

	case OpRead:
		// stateid (seqid:4 + other:12) + offset:8 + count:4
		var buf [16]byte
		io.ReadFull(&r.r, buf[:]) // stateid
		offset, _ := r.ReadUint64()
		count, _ := r.ReadUint32()
		w := getXDRWriter()
		w.buf.Write(buf[:])
		w.WriteUint64(offset)
		w.WriteUint32(count)
		return xdrWriterBytes(w), 0, nil

	case OpWrite:
		var buf [16]byte
		io.ReadFull(&r.r, buf[:]) // stateid
		offset, _ := r.ReadUint64()
		stable, _ := r.ReadUint32()
		data, _ := r.ReadOpaque()
		w := getXDRWriter()
		w.buf.Write(buf[:])
		w.WriteUint64(offset)
		w.WriteUint32(stable)
		w.WriteOpaque(data)
		return xdrWriterBytes(w), 0, nil

	case OpOpen:
		seqid, _ := r.ReadUint32()
		shareAccess, _ := r.ReadUint32()
		shareDeny, _ := r.ReadUint32()
		clientID, _ := r.ReadUint64()
		r.ReadOpaque() // owner
		openType, _ := r.ReadUint32()
		if openType == 1 { // CREATE
			createMode, _ := r.ReadUint32()
			if createMode == 0 { // UNCHECKED
				bitmapLen, _ := r.ReadUint32()
				for i := uint32(0); i < bitmapLen; i++ {
					r.ReadUint32()
				}
				r.ReadOpaque() // attrvals
			}
		}
		claimType, _ := r.ReadUint32()
		var fileName string
		if claimType == 0 {
			fileName, _ = r.ReadString()
		}
		_ = seqid
		_ = shareDeny
		_ = clientID
		w := getXDRWriter()
		w.WriteUint32(shareAccess)
		w.WriteUint32(openType)
		w.WriteString(fileName)
		return xdrWriterBytes(w), 0, nil

	case OpClose:
		buf := getOpArg16()
		r.ReadUint32() // seqid
		io.ReadFull(&r.r, buf)
		return buf, 16, nil

	case OpSetClientID:
		var verf [8]byte
		io.ReadFull(&r.r, verf[:])
		id, _ := r.ReadOpaque()
		r.ReadUint32() // cb_program
		r.ReadString() // netid
		r.ReadString() // addr
		r.ReadUint32() // callback_ident
		return append(verf[:], id...), 0, nil

	case OpSetClientIDConfirm:
		buf := getOpArg16()
		io.ReadFull(&r.r, buf)
		return buf, 16, nil

	case OpSetAttr:
		buf := getOpArg16()
		io.ReadFull(&r.r, buf) // stateid
		bitmapLen, _ := r.ReadUint32()
		for i := uint32(0); i < bitmapLen; i++ {
			r.ReadUint32()
		}
		r.ReadOpaque() // attrvals
		return buf, 16, nil

	case OpOpenConfirm:
		buf := getOpArg16()
		io.ReadFull(&r.r, buf) // stateid (open_stateid)
		r.ReadUint32()         // seqid
		return buf, 16, nil

	case OpRenew:
		b := getOpArg8()
		clientID, _ := r.ReadUint64()
		binary.BigEndian.PutUint64(b, clientID)
		return b, 8, nil

	default:
		return nil, 0, nil
	}
}

// OpRenew is the RENEW operation code.
const OpRenew = 30

// encodeCompoundResponseInto writes a COMPOUND4res directly into w (zero extra allocation).
func encodeCompoundResponseInto(w *XDRWriter, resp *CompoundResponse) {
	w.WriteUint32(uint32(resp.Status))
	w.WriteString(resp.Tag)
	w.WriteUint32(uint32(len(resp.Results)))
	for _, result := range resp.Results {
		w.WriteUint32(uint32(result.OpCode))
		w.WriteUint32(uint32(result.Status))
		if result.Data != nil {
			w.buf.Write(result.Data)
		}
	}
}

// EncodeCompoundResponse encodes a COMPOUND4res to XDR.
func EncodeCompoundResponse(resp *CompoundResponse) []byte {
	w := getXDRWriter()
	encodeCompoundResponseInto(w, resp)
	return xdrWriterBytes(w)
}
