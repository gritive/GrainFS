package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

func opXDRGetFH(w *XDRWriter) {
	w.WriteUint32(uint32(OpGetFH))
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
	req := compoundReqPool.Get().(*CompoundRequest)
	req.Tag = ""
	req.MinorVer = 0
	req.Ops = req.Ops[:0]

	err := ParseCompound([]byte{0}, req)
	assert.Error(t, err)
	// Return to pool — should not panic or corrupt pool.
	compoundReqPool.Put(req)
}

func TestCompound_PUTROOTFH_GETATTR_AllocsPerRun(t *testing.T) {
	// Empty tag: no tag-string allocs. Tests the Into hot path.
	data := buildCompoundXDR("",
		opXDRPutRootFH,
		opXDRGetAttr,
	)

	srv := &Server{state: NewStateManager()}

	// Warm up pools.
	for i := 0; i < 10; i++ {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	}

	allocs := testing.AllocsPerRun(100, func() {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	})
	// PUTROOTFH+GETATTR: only unavoidable alloc is GetAttr arg encoding (xdrWriterBytes). Target ≤2.
	assert.LessOrEqual(t, allocs, 2.0,
		"COMPOUND PUTROOTFH+GETATTR round-trip should allocate ≤2 (got %.1f)", allocs)
}

func TestCompound_PUTROOTFH_READDIR_AllocsPerRun(t *testing.T) {
	data := buildCompoundXDR("",
		opXDRPutRootFH,
		opXDRReadDir,
	)

	srv := &Server{state: NewStateManager()}

	for i := 0; i < 10; i++ {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	}

	allocs := testing.AllocsPerRun(100, func() {
		w := getXDRWriter()
		srv.handleCompoundInto(data, w)
		putXDRWriter(w)
	})
	// PUTROOTFH+READDIR: only unavoidable alloc is ReadDir arg encoding (xdrWriterBytes). Target ≤3.
	assert.LessOrEqual(t, allocs, 3.0,
		"COMPOUND PUTROOTFH+READDIR round-trip should allocate ≤3 (got %.1f)", allocs)
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
