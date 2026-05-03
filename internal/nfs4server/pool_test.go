package nfs4server

import (
	"encoding/binary"
	"github.com/rs/zerolog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildCompoundXDR builds raw XDR bytes for a COMPOUND request.
func buildCompoundXDR(tag string, ops ...func(*XDRWriter)) []byte {
	w := &XDRWriter{}
	w.WriteString(tag)
	w.WriteUint32(0) // minor version
	w.WriteUint32(uint32(len(ops)))
	for _, op := range ops {
		op(w)
	}
	return w.Bytes()
}

func opXDRPutRootFH(w *XDRWriter) {
	w.WriteUint32(uint32(OpPutRootFH))
}

func opXDRGetAttr(w *XDRWriter) {
	w.WriteUint32(uint32(OpGetAttr))
	w.WriteUint32(2)   // bitmap len
	w.WriteUint32(0x3) // attr word0
	w.WriteUint32(0)   // attr word1
}

func opXDRReadDir(w *XDRWriter) {
	w.WriteUint32(uint32(OpReadDir))
	w.WriteUint64(0) // cookie
	for i := 0; i < 8; i++ {
		w.buf.WriteByte(0) // cookieverf (8 bytes)
	}
	w.WriteUint32(512)  // dircount
	w.WriteUint32(4096) // maxcount
	w.WriteUint32(0)    // bitmap len
}

func TestXDRReaderPool_ZeroAllocs(t *testing.T) {
	data := []byte{0, 1, 2, 3}
	allocs := testing.AllocsPerRun(100, func() {
		r := newXDRReaderFromPool(data)
		putXDRReader(r)
	})
	assert.Equal(t, 0.0, allocs, "newXDRReaderFromPool + putXDRReader should allocate 0")
}

func TestXDRWriterPool_ZeroAllocs(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		w := getXDRWriter()
		w.WriteUint32(42)
		putXDRWriter(w)
	})
	assert.Equal(t, 0.0, allocs, "getXDRWriter + putXDRWriter should allocate 0")
}

func TestParseCompound_ErrorPath_NoLeak(t *testing.T) {
	// Truncated data should return error and req should be returned to pool.
	req := compoundReqPool.Get()
	req.Tag = ""
	req.MinorVer = 0
	req.Ops = req.Ops[:0]

	err := ParseCompound([]byte{0}, req)
	assert.Error(t, err)
	// Return to pool — should not panic or corrupt pool.
	compoundReqPool.Put(req)
}

func TestDispatch_EarlyBreak_OpArgReturned(t *testing.T) {
	// OpAccess uses opArgPool8 (poolKey=8). Dispatch should return it even on error break.
	state := NewStateManager()
	d := &Dispatcher{state: state}
	resp := &CompoundResponse{}

	req := &CompoundRequest{
		Ops: []Op{
			// OpAccess with poolKey=8 data — dispatcher should put it back.
			{OpCode: OpAccess, Data: getOpArg8()[:4], poolKey: 8},
			// This op would fail (no current FH for OpGetFH), but we test after OpAccess.
			{OpCode: OpGetFH, Data: nil, poolKey: 0},
		},
	}
	// Should not panic or leak.
	d.Dispatch(req, resp)
	// We get some result (either NFS4_OK or error), just verify no panic.
	assert.NotNil(t, resp)
}

// --- Coverage gap tests ---

func TestXDRWriterPool_LargeCapEviction(t *testing.T) {
	w := getXDRWriter()
	// Grow buf beyond eviction threshold (64KB)
	large := make([]byte, maxXDRWriterCap+1)
	w.buf.Write(large)
	require.Greater(t, w.buf.Cap(), maxXDRWriterCap)

	// putXDRWriter must evict without panic
	putXDRWriter(w)

	// Pool should still be usable after eviction
	w2 := getXDRWriter()
	w2.WriteUint32(42)
	assert.Equal(t, 4, len(w2.Bytes()))
	putXDRWriter(w2)
}

func TestOpArgPool16_ZeroAllocs(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		b := getOpArg16()
		putOpArg16(b)
	})
	assert.Equal(t, 0.0, allocs, "getOpArg16/putOpArg16 should allocate 0")
}

func TestDispatch_PoolKey16_Returned(t *testing.T) {
	state := NewStateManager()
	d := &Dispatcher{state: state}
	resp := &CompoundResponse{}

	// OpClose uses poolKey=16; Dispatch must return the buf to pool16 without panic.
	buf := getOpArg16()
	req := &CompoundRequest{
		Ops: []Op{
			{OpCode: OpClose, Data: buf, poolKey: 16},
		},
	}
	d.Dispatch(req, resp)
	assert.NotNil(t, resp)
}

func TestHandleCompoundInto_ParseError(t *testing.T) {
	srv := &Server{state: NewStateManager(), logger: zerolog.Nop()}
	w := getXDRWriter()
	defer putXDRWriter(w)

	// Truncated data forces ParseCompound to fail → NFS4ERR_INVAL response.
	srv.handleCompoundInto([]byte{0}, w)

	b := w.Bytes()
	require.GreaterOrEqual(t, len(b), 4, "response must contain at least a status uint32")
	status := binary.BigEndian.Uint32(b[:4])
	assert.Equal(t, uint32(NFS4ERR_INVAL), status)
}

func TestReadOpArgs_PutFH(t *testing.T) {
	// OpPutFH: ReadOpaque → poolKey=0
	w := getXDRWriter()
	fh := []byte{1, 2, 3, 4}
	w.WriteOpaque(fh)
	r := NewXDRReader(w.Bytes())
	putXDRWriter(w)

	data, pk, err := readOpArgs(r, OpPutFH)
	assert.NoError(t, err)
	assert.Equal(t, 0, pk)
	assert.Equal(t, fh, data)
}

func TestReadOpArgs_SetClientIDConfirm(t *testing.T) {
	// OpSetClientIDConfirm: reads exactly 16 bytes into pool16 buf.
	raw := make([]byte, 16)
	for i := range raw {
		raw[i] = byte(i)
	}
	r := NewXDRReader(raw)

	data, pk, err := readOpArgs(r, OpSetClientIDConfirm)
	assert.NoError(t, err)
	assert.Equal(t, 16, pk)
	assert.Equal(t, raw, data)
	putOpArg16(data)
}

func TestReadOpArgs_OpenConfirm(t *testing.T) {
	// OpOpenConfirm: stateid (16 bytes) + seqid (4 bytes); returns stateid only.
	raw := make([]byte, 20)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	r := NewXDRReader(raw)

	data, pk, err := readOpArgs(r, OpOpenConfirm)
	assert.NoError(t, err)
	assert.Equal(t, 16, pk)
	assert.Equal(t, 16, len(data))
	assert.Equal(t, raw[:16], data)
	putOpArg16(data)
}

func TestReadOpArgs_Renew(t *testing.T) {
	// OpRenew: 8-byte clientID into pool8 buf.
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], 0x1234567890abcdef)
	r := NewXDRReader(raw[:])

	data, pk, err := readOpArgs(r, OpRenew)
	assert.NoError(t, err)
	assert.Equal(t, 8, pk)
	assert.Equal(t, raw[:], data)
	putOpArg8(data)
}

func TestReadOpArgs_ReadUsesPooledFixedBuffer_NoAllocs(t *testing.T) {
	w := getXDRWriter()
	var stateid [16]byte
	for i := range stateid {
		stateid[i] = byte(i + 1)
	}
	w.buf.Write(stateid[:])
	w.WriteUint64(0x1020304050607080)
	w.WriteUint32(4096)
	raw := append([]byte(nil), w.Bytes()...)
	putXDRWriter(w)

	r := NewXDRReader(raw)
	data, pk, err := readOpArgs(r, OpRead)
	require.NoError(t, err)
	require.Equal(t, 32, pk)
	require.Len(t, data, 28)
	require.Equal(t, raw, data)
	putOpArg32(data)

	allocs := testing.AllocsPerRun(100, func() {
		r := newXDRReaderFromPool(raw)
		data, pk, err := readOpArgs(r, OpRead)
		putXDRReader(r)
		if err != nil {
			t.Fatalf("readOpArgs returned error: %v", err)
		}
		if pk != 32 || len(data) != 28 {
			t.Fatalf("unexpected read arg pool result: pk=%d len=%d", pk, len(data))
		}
		putOpArg32(data)
	})
	assert.Equal(t, 0.0, allocs, "OpRead args should reuse a fixed buffer")
}

func TestReadOpArgs_SetAttr(t *testing.T) {
	// OpSetAttr: stateid(16) + bitmap[2] + attrVals encoded as:
	// output = stateid(16) + bm0(4) + bm1(4) + attrVals(opaque)
	w := getXDRWriter()
	var stateid [16]byte
	for i := range stateid {
		stateid[i] = byte(i + 10)
	}
	w.buf.Write(stateid[:])
	w.WriteUint32(2)    // bitmap len = 2
	w.WriteUint32(0xAA) // bm0
	w.WriteUint32(0xBB) // bm1
	attrVals := []byte{1, 2, 3, 4}
	w.WriteOpaque(attrVals)
	r := NewXDRReader(w.Bytes())
	putXDRWriter(w)

	data, pk, err := readOpArgs(r, OpSetAttr)
	assert.NoError(t, err)
	assert.Equal(t, 0, pk) // pk=0 means caller should NOT call putOpArg16

	// data layout: stateid(16) + bm0(4) + bm1(4) + attrVals(opaque)
	assert.Equal(t, stateid[:], data[:16])
	// bm0 at bytes 16-19 (big-endian uint32 0xAA = 170)
	assert.Equal(t, byte(0), data[16])
	assert.Equal(t, byte(0), data[17])
	assert.Equal(t, byte(0), data[18])
	assert.Equal(t, byte(0xAA), data[19])
	_ = data // data returned from pool-less allocation; no putOpArg16
}

func TestReadOpArgs_WriteReturnsOriginalArgSlice(t *testing.T) {
	w := getXDRWriter()
	var stateid [16]byte
	w.buf.Write(stateid[:])
	w.WriteUint64(123)
	w.WriteUint32(2)
	w.WriteOpaque([]byte("payload"))
	raw := append([]byte(nil), w.Bytes()...)
	putXDRWriter(w)

	r := NewXDRReader(raw)
	data, pk, err := readOpArgs(r, OpWrite)
	assert.NoError(t, err)
	assert.Equal(t, 0, pk)
	assert.Equal(t, raw, data)

	raw[32] = 'P'
	assert.Equal(t, byte('P'), data[32])
}

func TestReadOpArgs_UnknownOp(t *testing.T) {
	// Unknown op code → default case, returns nil/0/nil.
	r := NewXDRReader(nil)
	data, pk, err := readOpArgs(r, 9999)
	assert.NoError(t, err)
	assert.Equal(t, 0, pk)
	assert.Nil(t, data)
}
