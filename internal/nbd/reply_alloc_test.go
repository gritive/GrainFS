//go:build !race

package nbd

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type discardConn struct{}

func (discardConn) Read([]byte) (int, error)         { return 0, nil }
func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return nil }
func (discardConn) RemoteAddr() net.Addr             { return nil }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

func TestSendReplyAllocBudget(t *testing.T) {
	s := &Server{}
	var handle [8]byte
	conn := discardConn{}
	var err error

	allocs := testing.AllocsPerRun(100, func() {
		err = s.sendReply(conn, handle[:], 0, nil)
	})

	require.NoError(t, err)
	require.Zero(t, allocs, "fixed-size NBD reply headers should not allocate")
}

func TestWriteStructuredHeaderAllocBudget(t *testing.T) {
	var handle [8]byte
	conn := discardConn{}
	var err error

	allocs := testing.AllocsPerRun(100, func() {
		err = writeStructuredHeader(conn, handle[:], nbdReplyFlagDone, nbdReplyTypeOffsetData, 8)
	})

	require.NoError(t, err)
	require.Zero(t, allocs, "fixed-size NBD structured reply headers should not allocate")
}
