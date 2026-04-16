package nfs4server

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// maxFrameSize caps ONC RPC frame size to prevent DoS (1MB).
	maxFrameSize = 1 * 1024 * 1024

	// RPC constants
	rpcProgNFS  = 100003
	rpcVersNFS4 = 4
	rpcMsgCall  = 0
	rpcMsgReply = 1

	// Auth flavors
	authNone = 0
	authSys  = 1
)

// writeRPCFrame writes a TCP record-marked RPC frame.
// Format: [4 bytes: length | 0x80000000 for last-fragment][payload]
func writeRPCFrame(w io.Writer, payload []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload))|0x80000000)
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readRPCFrame reads a TCP record-marked RPC frame.
// Supports fragment reassembly (reads until last-fragment bit is set).
func readRPCFrame(r io.Reader) ([]byte, error) {
	var result []byte

	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(r, header); err != nil {
			return nil, err
		}

		raw := binary.BigEndian.Uint32(header)
		lastFragment := (raw & 0x80000000) != 0
		length := raw & 0x7FFFFFFF

		if length > maxFrameSize {
			return nil, fmt.Errorf("RPC frame size %d exceeds max %d", length, maxFrameSize)
		}

		fragment := make([]byte, length)
		if _, err := io.ReadFull(r, fragment); err != nil {
			return nil, err
		}

		result = append(result, fragment...)

		if lastFragment {
			break
		}
	}

	return result, nil
}
