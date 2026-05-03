package nbd

import (
	"encoding/binary"
	"net"
)

func (s *Server) sendErrorReply(conn net.Conn, handle []byte, errCode uint32, state handshakeState) error {
	if state.structuredReplies {
		return s.sendStructuredError(conn, handle, errCode)
	}
	return s.sendReply(conn, handle, errCode, nil)
}

func (s *Server) sendStructuredRead(conn net.Conn, handle []byte, offset uint64, data []byte) error {
	if err := writeStructuredHeader(conn, handle, nbdReplyFlagDone, nbdReplyTypeOffsetData, uint32(8+len(data))); err != nil {
		return err
	}
	var off [8]byte
	binary.BigEndian.PutUint64(off[:], offset)
	if _, err := conn.Write(off[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) sendBlockStatus(conn net.Conn, handle []byte, length uint32) error {
	var payload [12]byte
	binary.BigEndian.PutUint32(payload[0:4], nbdMetaContextBaseAllocID)
	binary.BigEndian.PutUint32(payload[4:8], length)
	binary.BigEndian.PutUint32(payload[8:12], 0)
	return s.sendStructuredChunk(conn, handle, nbdReplyFlagDone, nbdReplyTypeBlockStatus, payload[:])
}

func (s *Server) sendStructuredError(conn net.Conn, handle []byte, errCode uint32) error {
	var payload [6]byte
	binary.BigEndian.PutUint32(payload[0:4], errCode)
	return s.sendStructuredChunk(conn, handle, nbdReplyFlagDone, nbdReplyTypeError, payload[:])
}

func (s *Server) sendStructuredChunk(conn net.Conn, handle []byte, flags, typ uint16, payload []byte) error {
	if err := writeStructuredHeader(conn, handle, flags, typ, uint32(len(payload))); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func writeStructuredHeader(conn net.Conn, handle []byte, flags, typ uint16, length uint32) error {
	var hdr [20]byte
	binary.BigEndian.PutUint32(hdr[0:4], nbdStructuredReplyMagic)
	binary.BigEndian.PutUint16(hdr[4:6], flags)
	binary.BigEndian.PutUint16(hdr[6:8], typ)
	copy(hdr[8:16], handle)
	binary.BigEndian.PutUint32(hdr[16:20], length)
	_, err := conn.Write(hdr[:])
	return err
}
