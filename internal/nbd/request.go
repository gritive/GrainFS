package nbd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type nbdRequest struct {
	flags  uint16
	typ    uint32
	handle [8]byte
	offset uint64
	length uint64
}

func readNBDRequest(conn net.Conn) (nbdRequest, error) {
	var hdr [28]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nbdRequest{}, err
	}
	return parseNBDRequest(hdr[:])
}

func parseNBDRequest(hdr []byte) (nbdRequest, error) {
	if len(hdr) != 28 {
		return nbdRequest{}, fmt.Errorf("bad request length: %d", len(hdr))
	}
	magic := binary.BigEndian.Uint32(hdr[0:4])
	if magic != nbdRequestMagic {
		return nbdRequest{}, fmt.Errorf("bad request magic: %x", magic)
	}

	var req nbdRequest
	req.flags = binary.BigEndian.Uint16(hdr[4:6])
	req.typ = uint32(binary.BigEndian.Uint16(hdr[6:8]))
	copy(req.handle[:], hdr[8:16])
	req.offset = binary.BigEndian.Uint64(hdr[16:24])
	req.length = uint64(binary.BigEndian.Uint32(hdr[24:28]))
	return req, nil
}

func validateRequestSize(req nbdRequest) error {
	switch req.typ {
	case nbdCmdRead, nbdCmdWrite, nbdCmdTrim, nbdCmdWriteZeroes, nbdCmdBlockStatus:
		if req.length > uint64(nbdMaxPayloadSize) {
			return fmt.Errorf("nbd request length %d exceeds max %d", req.length, nbdMaxPayloadSize)
		}
	}
	return nil
}
