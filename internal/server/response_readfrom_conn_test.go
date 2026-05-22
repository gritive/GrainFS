package server

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResponseReadFromConnUsesLargeBuffer(t *testing.T) {
	payload := bytes.Repeat([]byte{0x42}, 5<<20)
	raw := &recordingConn{}
	conn, ok := newResponseReadFromConn(raw).(io.ReaderFrom)
	require.True(t, ok)

	n, err := conn.ReadFrom(io.LimitReader(bytes.NewReader(payload), int64(len(payload))))
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.False(t, raw.readFromCalled)
	require.Equal(t, hertzResponseReadFromBufferSize, raw.maxWrite)
	require.Equal(t, 20, raw.writes)
}

func BenchmarkResponseReadFromConn5MiB(b *testing.B) {
	payload := bytes.Repeat([]byte{0x42}, 5<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()

	for b.Loop() {
		raw := &recordingConn{}
		conn := newResponseReadFromConn(raw).(io.ReaderFrom)
		n, err := conn.ReadFrom(io.LimitReader(bytes.NewReader(payload), int64(len(payload))))
		require.NoError(b, err)
		require.Equal(b, int64(len(payload)), n)
	}
}

type recordingConn struct {
	readFromCalled bool
	writes         int
	maxWrite       int
}

func (c *recordingConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *recordingConn) Write(p []byte) (int, error) {
	c.writes++
	if len(p) > c.maxWrite {
		c.maxWrite = len(p)
	}
	return len(p), nil
}

func (c *recordingConn) ReadFrom(io.Reader) (int64, error) {
	c.readFromCalled = true
	return 0, nil
}

func (c *recordingConn) Close() error                     { return nil }
func (c *recordingConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (c *recordingConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }
