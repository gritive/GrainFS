package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXDRWriter_WriteUint32_ZeroAllocs(t *testing.T) {
	var w XDRWriter
	allocs := testing.AllocsPerRun(100, func() {
		w.buf.Reset()
		w.WriteUint32(0xDEADBEEF)
	})
	assert.Equal(t, 0.0, allocs, "WriteUint32 should allocate 0 (stack array)")
}

func TestXDRWriter_WriteUint64_ZeroAllocs(t *testing.T) {
	var w XDRWriter
	allocs := testing.AllocsPerRun(100, func() {
		w.buf.Reset()
		w.WriteUint64(0xDEADBEEFCAFEBABE)
	})
	assert.Equal(t, 0.0, allocs, "WriteUint64 should allocate 0 (stack array)")
}

func TestXDRReader_ReadUint32_ZeroAllocs(t *testing.T) {
	var w XDRWriter
	w.WriteUint32(42)
	encoded := w.Bytes()

	r := NewXDRReader(encoded)
	allocs := testing.AllocsPerRun(100, func() {
		r.r.Seek(0, 0) //nolint:errcheck
		_, _ = r.ReadUint32()
	})
	assert.Equal(t, 0.0, allocs, "ReadUint32 should allocate 0 (stack array, bytes.Reader.Read)")
}

func TestXDRReader_ReadUint64_ZeroAllocs(t *testing.T) {
	var w XDRWriter
	w.WriteUint64(0xCAFEBABE12345678)
	encoded := w.Bytes()

	r := NewXDRReader(encoded)
	allocs := testing.AllocsPerRun(100, func() {
		r.r.Seek(0, 0) //nolint:errcheck
		_, _ = r.ReadUint64()
	})
	assert.Equal(t, 0.0, allocs, "ReadUint64 should allocate 0 (stack array, bytes.Reader.Read)")
}

func TestXDRRoundTrip_Uint32(t *testing.T) {
	var w XDRWriter
	w.WriteUint32(0x12345678)

	r := NewXDRReader(w.Bytes())
	v, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(0x12345678), v)
}

func TestXDRRoundTrip_Uint64(t *testing.T) {
	var w XDRWriter
	w.WriteUint64(0x123456789ABCDEF0)

	r := NewXDRReader(w.Bytes())
	v, err := r.ReadUint64()
	require.NoError(t, err)
	assert.Equal(t, uint64(0x123456789ABCDEF0), v)
}
