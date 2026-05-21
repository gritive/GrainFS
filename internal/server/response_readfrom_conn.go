package server

import (
	"io"
	"net"
	"sync"
)

const hertzResponseReadFromBufferSize = 256 * 1024

var hertzResponseReadFromBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, hertzResponseReadFromBufferSize)
		return &buf
	},
}

type responseReadFromConn struct {
	net.Conn
}

func newResponseReadFromConn(conn net.Conn) net.Conn {
	return &responseReadFromConn{Conn: conn}
}

func (c *responseReadFromConn) ReadFrom(r io.Reader) (int64, error) {
	bufp := hertzResponseReadFromBufferPool.Get().(*[]byte)
	buf := (*bufp)[:hertzResponseReadFromBufferSize]
	defer func() {
		*bufp = buf
		hertzResponseReadFromBufferPool.Put(bufp)
	}()
	return io.CopyBuffer(connWithoutReadFrom{Conn: c.Conn}, r, buf)
}

type connWithoutReadFrom struct {
	net.Conn
}
