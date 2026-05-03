package nfs4server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/gritive/GrainFS/internal/pool"
)

// XDR encoding/decoding helpers for NFSv4.0 (RFC 7530).

// XDRWriter writes XDR-encoded values.
type XDRWriter struct {
	buf bytes.Buffer
}

const maxXDRWriterCap = 64 * 1024

var xdrWriterPool = pool.New(func() *XDRWriter { return &XDRWriter{} })

func getXDRWriter() *XDRWriter {
	return xdrWriterPool.Get()
}

func putXDRWriter(w *XDRWriter) {
	if w.buf.Cap() > maxXDRWriterCap {
		w.buf = bytes.Buffer{}
	} else {
		w.buf.Reset()
	}
	xdrWriterPool.Put(w)
}

// xdrWriterBytes transfers ownership of w's backing buffer to the caller and
// returns the now-empty writer to the pool. The caller must treat the returned
// slice as immutable.
func xdrWriterBytes(w *XDRWriter) []byte {
	out := w.Bytes()
	w.buf = bytes.Buffer{}
	xdrWriterPool.Put(w)
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
	r      bytes.Reader
	data   []byte
	pooled bool
}

var xdrReaderPool = pool.New(func() *XDRReader { return &XDRReader{} })

// opArgPool16/opArgPool8: raw sync.Pool 사용 — generic pool.Pool[*[N]byte]는 Go 1.26.2
// 컴파일러 ICE를 트리거한다 (`internal compiler error: bad ptr to array in slice
// go.shape.*uint8`). `make test`처럼 다수 패키지 + `-cover` 조합으로 병렬 빌드할 때만
// 재현되며 단독 빌드는 정상. 원인은 fixed-size array pointer (`*[N]byte`)를 generic
// type parameter로 instantiate한 후 결과를 슬라이싱하는 패턴에서 shape 분석 버그.
// 같은 generic Pool[T]는 *XDRWriter/*XDRReader 등 struct pointer에서는 정상 동작하므로
// 이 두 케이스만 raw sync.Pool로 우회한다. Go upstream 수정 시 generic으로 복원 가능.
var opArgPool16 = sync.Pool{New: func() any { return new([16]byte) }}
var opArgPool8 = sync.Pool{New: func() any { return new([8]byte) }}

func getOpArg16() []byte  { return opArgPool16.Get().(*[16]byte)[:] }
func putOpArg16(b []byte) { opArgPool16.Put((*[16]byte)(b[:16])) }

func getOpArg8() []byte  { return opArgPool8.Get().(*[8]byte)[:] }
func putOpArg8(b []byte) { opArgPool8.Put((*[8]byte)(b[:8])) }

func NewXDRReader(data []byte) *XDRReader {
	r := &XDRReader{data: data}
	r.r.Reset(data)
	return r
}

func newXDRReaderFromPool(data []byte) *XDRReader {
	r := xdrReaderPool.Get()
	r.data = data
	r.r.Reset(data)
	r.pooled = true
	return r
}

func putXDRReader(r *XDRReader) {
	if r.pooled {
		r.pooled = false
		r.data = nil
		xdrReaderPool.Put(r)
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
	data, err := r.ReadOpaqueView()
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (r *XDRReader) ReadOpaqueView() ([]byte, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if length > maxFrameSize {
		return nil, fmt.Errorf("opaque too large: %d", length)
	}
	if int(length) > r.r.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	start := r.Offset()
	end := start + int(length)
	if _, err := r.r.Seek(int64(length), io.SeekCurrent); err != nil {
		return nil, err
	}
	pad := (4 - int(length)%4) % 4
	if pad > r.r.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	if pad > 0 {
		if _, err := r.r.Seek(int64(pad), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
	return r.data[start:end], nil
}

func (r *XDRReader) ReadString() (string, error) {
	data, err := r.ReadOpaque()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadFixed reads exactly n bytes (fixed-length opaque, no XDR length prefix).
func (r *XDRReader) ReadFixed(n int) ([]byte, error) {
	data := make([]byte, n)
	if _, err := io.ReadFull(&r.r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (r *XDRReader) Remaining() int {
	return int(r.r.Len())
}

func (r *XDRReader) Offset() int {
	return len(r.data) - r.r.Len()
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

	case OpCreate:
		// CREATE4args: createtype4 objtype + component4 objname + fattr4 createattrs
		objType, _ := r.ReadUint32() // nfs_ftype4
		if objType == 5 {            // NF4LNK: skip linktext4 (string)
			r.ReadOpaque()
		}
		// For NF4BLK/NF4CHR skip specdata4 (2 uint32s); others have no extra data.
		if objType == 3 || objType == 4 {
			r.ReadUint32()
			r.ReadUint32()
		}
		name, _ := r.ReadString()
		// fattr4 createattrs: bitmap4 + opaque attrlist
		bitmapLen, _ := r.ReadUint32()
		for i := uint32(0); i < bitmapLen; i++ {
			r.ReadUint32()
		}
		r.ReadOpaque()
		w := getXDRWriter()
		w.WriteUint32(objType)
		w.WriteString(name)
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
		argStart := r.Offset()
		var buf [16]byte
		io.ReadFull(&r.r, buf[:]) // stateid
		r.ReadUint64()            // offset
		r.ReadUint32()            // stable
		if _, err := r.ReadOpaqueView(); err != nil {
			return nil, 0, err
		}
		return r.data[argStart:r.Offset()], 0, nil

	case OpOpen:
		seqid, _ := r.ReadUint32()
		shareAccess, _ := r.ReadUint32()
		shareDeny, _ := r.ReadUint32()
		clientID, _ := r.ReadUint64()
		r.ReadOpaque() // owner
		openType, _ := r.ReadUint32()
		if openType == 1 { // CREATE
			createMode, _ := r.ReadUint32()
			switch createMode {
			case 0, 1: // UNCHECKED4, GUARDED4: both carry fattr4
				bitmapLen, _ := r.ReadUint32()
				for i := uint32(0); i < bitmapLen; i++ {
					r.ReadUint32()
				}
				r.ReadOpaque() // attrvals
			case 2: // EXCLUSIVE4: 8-byte verifier
				r.ReadFixed(8)
			case 3: // EXCLUSIVE4_1: verifier + fattr4
				r.ReadFixed(8)
				bitmapLen, _ := r.ReadUint32()
				for i := uint32(0); i < bitmapLen; i++ {
					r.ReadUint32()
				}
				r.ReadOpaque()
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

	case OpCommit:
		r.ReadUint64() // offset
		r.ReadUint32() // count
		return nil, 0, nil

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

	case OpRemove:
		name, err := r.ReadString()
		return []byte(name), 0, err

	case OpRename:
		oldName, err := r.ReadString()
		if err != nil {
			return nil, 0, err
		}
		newName, err := r.ReadString()
		if err != nil {
			return nil, 0, err
		}
		w := getXDRWriter()
		w.WriteString(oldName)
		w.WriteString(newName)
		return xdrWriterBytes(w), 0, nil

	case OpSetAttr:
		var stateid [16]byte
		io.ReadFull(&r.r, stateid[:])
		bitmapLen, _ := r.ReadUint32()
		var bm [2]uint32
		if bitmapLen >= 1 {
			bm[0], _ = r.ReadUint32()
		}
		if bitmapLen >= 2 {
			bm[1], _ = r.ReadUint32()
		}
		for i := uint32(2); i < bitmapLen; i++ {
			r.ReadUint32()
		}
		attrVals, _ := r.ReadOpaque()
		// encode: stateid(16) + bm[0](4) + bm[1](4) + attrVals(opaque)
		w := getXDRWriter()
		w.buf.Write(stateid[:])
		w.WriteUint32(bm[0])
		w.WriteUint32(bm[1])
		w.WriteOpaque(attrVals)
		return xdrWriterBytes(w), 0, nil

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

	case OpExchangeID:
		// client_owner4: verifier(8) + co_ownerid(opaque)
		var verf [8]byte
		io.ReadFull(&r.r, verf[:])
		ownerID, _ := r.ReadOpaque()
		// eia_flags
		flags, _ := r.ReadUint32()
		// eia_state_protect: spa_how + optional params
		spaHow, _ := r.ReadUint32()
		// SP4_NONE=0, SP4_MACH_CRED=1, SP4_SSV=2
		// For SP4_MACH_CRED skip two bitmaps; we only support SP4_NONE
		if spaHow == 1 {
			for i := 0; i < 2; i++ {
				blen, _ := r.ReadUint32()
				for j := uint32(0); j < blen; j++ {
					r.ReadUint32()
				}
			}
		}
		// eia_client_impl_id: nfs_impl_id4<1>
		implCount, _ := r.ReadUint32()
		if implCount > 16 {
			implCount = 0
		}
		for i := uint32(0); i < implCount; i++ {
			r.ReadOpaque() // nii_domain
			r.ReadOpaque() // nii_name
			r.ReadUint64() // nii_date (nfstime4: seconds)
			r.ReadUint32() // nii_date: nseconds
		}
		_ = flags
		_ = spaHow
		w := getXDRWriter()
		w.buf.Write(verf[:])
		w.WriteOpaque(ownerID)
		return xdrWriterBytes(w), 0, nil

	case OpCreateSession:
		clientID, _ := r.ReadUint64()
		seq, _ := r.ReadUint32()
		flags, _ := r.ReadUint32()
		_ = flags
		// fore channel attrs
		fore := readChannelAttrs(r)
		// back channel attrs (skip)
		readChannelAttrs(r)
		r.ReadUint32() // csa_cb_program
		// csa_sec_parms count
		secCount, _ := r.ReadUint32()
		if secCount > 64 {
			secCount = 0
		}
		for i := uint32(0); i < secCount; i++ {
			r.ReadUint32() // cb_secflavor; we only expect AUTH_NONE=0
		}
		w := getXDRWriter()
		w.WriteUint64(clientID)
		w.WriteUint32(seq)
		w.WriteUint32(fore.HeaderPadSize)
		w.WriteUint32(fore.MaxRequestSize)
		w.WriteUint32(fore.MaxResponseSize)
		w.WriteUint32(fore.MaxResponseSizeCached)
		w.WriteUint32(fore.MaxOperations)
		w.WriteUint32(fore.MaxRequests)
		return xdrWriterBytes(w), 0, nil

	case OpDestroySession:
		var sid [16]byte
		io.ReadFull(&r.r, sid[:])
		return sid[:], 0, nil

	case OpSequence:
		// sessionid(16) + sequenceid(4) + slotid(4) + highest_slotid(4) + cachethis(4)
		buf := make([]byte, 32)
		io.ReadFull(&r.r, buf[:16]) // sessionid
		seq, _ := r.ReadUint32()
		slotID, _ := r.ReadUint32()
		highSlot, _ := r.ReadUint32()
		cacheThis, _ := r.ReadUint32()
		w := getXDRWriter()
		w.buf.Write(buf[:16])
		w.WriteUint32(seq)
		w.WriteUint32(slotID)
		w.WriteUint32(highSlot)
		w.WriteUint32(cacheThis)
		return xdrWriterBytes(w), 0, nil

	case OpReclaimComplete:
		r.ReadUint32() // rca_one_fs (bool)
		return nil, 0, nil

	case OpSeek:
		// stateid(16) + offset(8) + whence(4)
		w := getXDRWriter()
		sid, _ := r.ReadFixed(16)
		w.buf.Write(sid)
		offset, _ := r.ReadUint64()
		w.WriteUint64(offset)
		whence, _ := r.ReadUint32()
		w.WriteUint32(whence)
		return xdrWriterBytes(w), 0, nil

	case OpAllocate:
		// stateid(16) + offset(8) + length(8)
		w := getXDRWriter()
		sid, _ := r.ReadFixed(16)
		w.buf.Write(sid)
		offset, _ := r.ReadUint64()
		w.WriteUint64(offset)
		length, _ := r.ReadUint64()
		w.WriteUint64(length)
		return xdrWriterBytes(w), 0, nil

	case OpDeallocate:
		// stateid(16) + offset(8) + length(8)
		w := getXDRWriter()
		sid, _ := r.ReadFixed(16)
		w.buf.Write(sid)
		offset, _ := r.ReadUint64()
		w.WriteUint64(offset)
		length, _ := r.ReadUint64()
		w.WriteUint64(length)
		return xdrWriterBytes(w), 0, nil

	case OpCopy:
		// src stateid(16) + dst stateid(16) + src_offset(8) + dst_offset(8) + count(8)
		// + ca_consecutive(4) + ca_synchronous(4) + source_server count(4)
		w := getXDRWriter()
		srcSid, _ := r.ReadFixed(16)
		w.buf.Write(srcSid)
		dstSid, _ := r.ReadFixed(16)
		w.buf.Write(dstSid)
		srcOffset, _ := r.ReadUint64()
		w.WriteUint64(srcOffset)
		dstOffset, _ := r.ReadUint64()
		w.WriteUint64(dstOffset)
		count, _ := r.ReadUint64()
		w.WriteUint64(count)
		r.ReadUint32() // ca_consecutive
		r.ReadUint32() // ca_synchronous
		srcCount, _ := r.ReadUint32()
		for i := uint32(0); i < srcCount; i++ {
			r.ReadOpaque() //nolint:errcheck // na_netid
			r.ReadOpaque() //nolint:errcheck // na_uaddr
		}
		return xdrWriterBytes(w), 0, nil

	case OpIOAdvise:
		// stateid(16) + offset(8) + count(8) + hints bitmap
		r.ReadFixed(16) //nolint:errcheck // stateid
		r.ReadUint64()  // offset
		r.ReadUint64()  // count
		blen, _ := r.ReadUint32()
		for i := uint32(0); i < blen; i++ {
			r.ReadUint32()
		}
		return nil, 0, nil

	case OpDestroyClientID:
		b := getOpArg8()
		clientID, _ := r.ReadUint64()
		binary.BigEndian.PutUint64(b, clientID)
		return b, 8, nil

	case OpFreeStateID:
		// stateid4: seqid(4) + other(12)
		var buf [16]byte
		io.ReadFull(&r.r, buf[:])
		data := make([]byte, 16)
		copy(data, buf[:])
		return data, 0, nil

	case OpTestStateID:
		// tsria_stateids: count(4) + N×stateid4(16)
		count, _ := r.ReadUint32()
		w := getXDRWriter()
		w.WriteUint32(count)
		for i := uint32(0); i < count; i++ {
			var sid [16]byte
			io.ReadFull(&r.r, sid[:])
			w.buf.Write(sid[:])
		}
		return xdrWriterBytes(w), 0, nil

	default:
		return nil, 0, nil
	}
}

// readChannelAttrs reads a channel_attrs4 struct.
func readChannelAttrs(r *XDRReader) ChannelAttrs {
	ca := ChannelAttrs{}
	ca.HeaderPadSize, _ = r.ReadUint32()
	ca.MaxRequestSize, _ = r.ReadUint32()
	ca.MaxResponseSize, _ = r.ReadUint32()
	ca.MaxResponseSizeCached, _ = r.ReadUint32()
	ca.MaxOperations, _ = r.ReadUint32()
	ca.MaxRequests, _ = r.ReadUint32()
	rdmaCount, _ := r.ReadUint32()
	if rdmaCount > 64 {
		rdmaCount = 0
	}
	for i := uint32(0); i < rdmaCount; i++ {
		r.ReadUint32()
	}
	return ca
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
