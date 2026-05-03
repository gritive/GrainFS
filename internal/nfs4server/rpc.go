package nfs4server

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// maxFrameSize caps ONC RPC frame size to prevent DoS (4MB).
	// Must be larger than MAXWRITE (1MB) + RPC/COMPOUND overhead.
	maxFrameSize = 4 * 1024 * 1024

	// RPC constants
	rpcProgNFS  = 100003
	rpcVersNFS4 = 4
	rpcMsgCall  = 0
	rpcMsgReply = 1

	// Auth flavors
	authNone = 0
)

// writeRPCFrame writes a TCP record-marked RPC frame.
// Format: [4 bytes: length | 0x80000000 for last-fragment][payload]
func writeRPCFrame(w io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload))|0x80000000)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readRPCFrame reads a TCP record-marked RPC frame.
// Supports fragment reassembly (reads until last-fragment bit is set).
func readRPCFrame(r io.Reader) ([]byte, error) {
	return readRPCFrameInto(r, nil)
}

func readRPCFrameInto(r io.Reader, buf []byte) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	raw := binary.BigEndian.Uint32(hdr[:])
	lastFragment := (raw & 0x80000000) != 0
	length := raw & 0x7FFFFFFF
	if length > maxFrameSize {
		return nil, fmt.Errorf("RPC frame size %d exceeds max %d", length, maxFrameSize)
	}

	result := resizeFrameBuffer(buf, int(length))
	if _, err := io.ReadFull(r, result); err != nil {
		return nil, err
	}
	if lastFragment {
		return result, nil
	}

	// multi-fragment slow path
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, err
		}
		raw = binary.BigEndian.Uint32(hdr[:])
		lastFragment = (raw & 0x80000000) != 0
		length = raw & 0x7FFFFFFF
		if length > maxFrameSize {
			return nil, fmt.Errorf("RPC frame size %d exceeds max %d", length, maxFrameSize)
		}
		start := len(result)
		result = append(result, make([]byte, int(length))...)
		if _, err := io.ReadFull(r, result[start:]); err != nil {
			return nil, err
		}
		if lastFragment {
			break
		}
	}

	return result, nil
}

func resizeFrameBuffer(buf []byte, n int) []byte {
	if cap(buf) >= n {
		return buf[:n]
	}
	return make([]byte, n)
}
