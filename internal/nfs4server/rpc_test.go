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
		Tag:       "",
		MinorVer:  0,
		Ops:       []Op{{OpCode: OpPutRootFH}},
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
