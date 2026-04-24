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

	resp := d.Dispatch(req)
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
	resp := d.Dispatch(req)
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
