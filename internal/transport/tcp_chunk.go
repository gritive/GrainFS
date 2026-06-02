package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// tcpChunkSize is the data-plane body chunk size — matched to the 1 MiB copy
	// buffer so per-chunk framing overhead is negligible.
	tcpChunkSize = 1 << 20
	// tcpMaxChunkSize bounds a single inbound chunk allocation/echo before the
	// length is trusted (defensive against a malformed/hostile length prefix).
	tcpMaxChunkSize = 16 << 20
)

// writeChunkedBody streams r to w as [4B BE len][len bytes] chunks terminated by
// a zero-length chunk, so the peer learns the body boundary WITHOUT a conn close
// — the property that makes the conn reusable (replaces S1's CloseWrite delimiter).
func writeChunkedBody(w io.Writer, r io.Reader) error {
	buf := make([]byte, tcpChunkSize)
	var hdr [4]byte
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			binary.BigEndian.PutUint32(hdr[:], uint32(n))
			if _, werr := w.Write(hdr[:]); werr != nil {
				return werr
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	binary.BigEndian.PutUint32(hdr[:], 0) // terminator
	_, err := w.Write(hdr[:])
	return err
}

// chunkedBodyReader reads a chunked body written by writeChunkedBody, presenting
// it as an io.Reader that returns io.EOF at the terminator and leaves the
// underlying r positioned at the next frame. `done` reports whether the
// terminator was reached — the pool-return path checks it to decide clean vs dirty.
type chunkedBodyReader struct {
	r         io.Reader
	remaining uint32 // bytes left in the current chunk
	done      bool
}

func (c *chunkedBodyReader) Read(p []byte) (int, error) {
	if c.done {
		return 0, io.EOF
	}
	if c.remaining == 0 {
		var hdr [4]byte
		if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 {
			c.done = true
			return 0, io.EOF
		}
		if n > tcpMaxChunkSize {
			return 0, fmt.Errorf("transport: inbound chunk %d exceeds max %d", n, tcpMaxChunkSize)
		}
		c.remaining = n
	}
	toRead := len(p)
	if uint32(toRead) > c.remaining {
		toRead = int(c.remaining)
	}
	n, err := c.r.Read(p[:toRead])
	c.remaining -= uint32(n)
	return n, err
}
