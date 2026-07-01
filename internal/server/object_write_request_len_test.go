package server

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// A1: a buffered PUT body reaches the storage SegmentWriter as *putObjectBodyReader
// (not *bytes.Reader). For firstChunkBufferSize to right-size the first chunk to
// the object size — instead of a full 16 MiB default — this reader must expose
// its authoritative remaining length via `interface{ Len() int }`.
func TestPutObjectBodyReader_ReportsRemainingLen(t *testing.T) {
	r := newPutObjectBodyReader([]byte("hello"))

	lr, ok := io.Reader(r).(interface{ Len() int })
	require.True(t, ok, "putObjectBodyReader must implement Len() int")
	require.Equal(t, 5, lr.Len())

	buf := make([]byte, 2)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 3, lr.Len(), "Len must reflect the unread remainder")
}
