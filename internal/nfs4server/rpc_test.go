package nfs4server

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCFrame_EncodeAndDecode(t *testing.T) {
	payload := []byte("test rpc payload")

	// Encode
	var buf bytes.Buffer
	err := writeRPCFrame(&buf, payload)
	require.NoError(t, err)

	// Frame should be: 4 bytes length (with last-fragment bit) + payload
	assert.Equal(t, 4+len(payload), buf.Len())

	// Decode
	got, err := readRPCFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestRPCFrame_MaxSizeEnforced(t *testing.T) {
	// Craft a frame claiming to be larger than maxFrameSize
	var buf bytes.Buffer
	header := uint32(maxFrameSize+1) | 0x80000000 // last-fragment + oversized
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, header)
	buf.Write(b)

	_, err := readRPCFrame(&buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max")
}

func TestCompoundRequest_ParseMinimal(t *testing.T) {
	// A minimal COMPOUND with just a PUTROOTFH op
	req := &CompoundRequest{
		Tag:      "",
		MinorVer: 0,
		Ops:      []Op{{OpCode: OpPutRootFH}},
	}

	assert.Len(t, req.Ops, 1)
	assert.Equal(t, OpPutRootFH, req.Ops[0].OpCode)
}

func TestCompoundDispatcher_HandlesPutRootFH(t *testing.T) {
	d := NewDispatcher(nil) // nil backend for unit test

	req := &CompoundRequest{
		Ops: []Op{{OpCode: OpPutRootFH}},
	}
	resp := &CompoundResponse{}

	d.Dispatch(req, resp)
	require.Len(t, resp.Results, 1)
	assert.Equal(t, NFS4_OK, resp.Results[0].Status)
}

func TestCompoundDispatcher_RejectsOverMaxOps(t *testing.T) {
	d := NewDispatcher(nil)

	ops := make([]Op, maxCompoundOps+1)
	for i := range ops {
		ops[i] = Op{OpCode: OpPutRootFH}
	}

	req := &CompoundRequest{Ops: ops}
	resp := &CompoundResponse{}
	d.Dispatch(req, resp)
	assert.Equal(t, NFS4ERR_RESOURCE, resp.Status)
}

func TestReadRPCFrame_AllocsBounded(t *testing.T) {
	payload := []byte("hello rpc payload for alloc test")
	var encoded bytes.Buffer
	err := writeRPCFrame(&encoded, payload)
	require.NoError(t, err)
	encodedBytes := encoded.Bytes()

	allocs := testing.AllocsPerRun(100, func() {
		r := bytes.NewReader(encodedBytes)
		_, _ = readRPCFrame(r)
	})
	// 단일 fragment: bytes.NewReader(1) + hdr escape via io.ReadAtLeast(1) + result(1) = 3 alloc 이하
	assert.LessOrEqual(t, allocs, 3.0, "readRPCFrame single fragment should allocate ≤3")
}

func TestReadRPCFrameInto_ReusesBuffer(t *testing.T) {
	payload := []byte("hello rpc payload for buffer reuse")
	var encoded bytes.Buffer
	require.NoError(t, writeRPCFrame(&encoded, payload))
	encodedBytes := encoded.Bytes()
	reuse := make([]byte, len(payload))
	var r bytes.Reader

	allocs := testing.AllocsPerRun(100, func() {
		r.Reset(encodedBytes)
		got, err := readRPCFrameInto(&r, reuse)
		if err != nil {
			t.Fatalf("readRPCFrameInto returned error: %v", err)
		}
		if !bytes.Equal(payload, got) {
			t.Fatalf("payload mismatch: got %q want %q", got, payload)
		}
		if &reuse[0] != &got[0] {
			t.Fatal("readRPCFrameInto did not return the caller-provided buffer")
		}
	})
	assert.LessOrEqual(t, allocs, 1.0, "readRPCFrameInto should reuse caller buffer")
}

func TestRPCFrame_MultiFragment(t *testing.T) {
	frag1 := []byte("hello")
	frag2 := []byte(" world")

	var buf bytes.Buffer
	// first fragment: no last-fragment bit
	var h1 [4]byte
	binary.BigEndian.PutUint32(h1[:], uint32(len(frag1)))
	buf.Write(h1[:])
	buf.Write(frag1)
	// second fragment: last-fragment bit set
	var h2 [4]byte
	binary.BigEndian.PutUint32(h2[:], uint32(len(frag2))|0x80000000)
	buf.Write(h2[:])
	buf.Write(frag2)

	got, err := readRPCFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, append(frag1, frag2...), got)
}

func TestRPCFrame_MultiFragment_MaxSizeExceeded(t *testing.T) {
	frag1 := []byte("hello")

	var buf bytes.Buffer
	var h1 [4]byte
	binary.BigEndian.PutUint32(h1[:], uint32(len(frag1)))
	buf.Write(h1[:])
	buf.Write(frag1)
	// second fragment: oversized + last-fragment bit
	var h2 [4]byte
	binary.BigEndian.PutUint32(h2[:], uint32(maxFrameSize+1)|0x80000000)
	buf.Write(h2[:])

	_, err := readRPCFrame(&buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max")
}
