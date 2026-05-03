package nbd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/gritive/GrainFS/internal/volume"
)

type nbdMetaContext struct {
	id   uint32
	name string
}

type handshakeState struct {
	clientFlags       uint32
	noZeroes          bool
	structuredReplies bool
	extendedHeaders   bool
	exportName        string
	metaContexts      []nbdMetaContext
}

func parseClientFlags(flags uint32) (handshakeState, error) {
	if unknown := flags &^ nbdKnownClientFlags; unknown != 0 {
		return handshakeState{}, fmt.Errorf("unknown client flags: 0x%x", unknown)
	}
	return handshakeState{
		clientFlags: flags,
		noZeroes:    flags&nbdFlagClientNoZeroes != 0,
	}, nil
}

func (s *Server) newstyleHandshake(conn net.Conn, vol *volume.Volume) (handshakeState, error) {
	hdr := make([]byte, 18)
	binary.BigEndian.PutUint64(hdr[0:8], nbdMagic)
	binary.BigEndian.PutUint64(hdr[8:16], nbdOptionMagic)
	binary.BigEndian.PutUint16(hdr[16:18], nbdFlagFixedNewstyle)
	if _, err := conn.Write(hdr); err != nil {
		return handshakeState{}, fmt.Errorf("write server header: %w", err)
	}

	var clientFlags [4]byte
	if _, err := io.ReadFull(conn, clientFlags[:]); err != nil {
		return handshakeState{}, fmt.Errorf("read client flags: %w", err)
	}
	state, err := parseClientFlags(binary.BigEndian.Uint32(clientFlags[:]))
	if err != nil {
		return handshakeState{}, err
	}

	for {
		var optHdr [16]byte
		if _, err := io.ReadFull(conn, optHdr[:]); err != nil {
			return handshakeState{}, fmt.Errorf("read option header: %w", err)
		}

		magic := binary.BigEndian.Uint64(optHdr[0:8])
		if magic != nbdOptionMagic {
			return handshakeState{}, fmt.Errorf("bad option magic: %x", magic)
		}

		optType := binary.BigEndian.Uint32(optHdr[8:12])
		optLen := binary.BigEndian.Uint32(optHdr[12:16])

		var optData []byte
		if optLen > 0 {
			optData = make([]byte, optLen)
			if _, err := io.ReadFull(conn, optData); err != nil {
				return handshakeState{}, fmt.Errorf("read option data: %w", err)
			}
		}

		switch optType {
		case nbdOptExportName:
			state.exportName = string(optData)
			return state, s.sendExportData(conn, vol, state)

		case nbdOptGo:
			return state, s.handleOptGo(conn, vol, optType)

		case nbdOptList:
			if err := s.handleOptList(conn, optType); err != nil {
				return handshakeState{}, err
			}

		case nbdOptAbort:
			_ = s.sendOptReply(conn, optType, nbdRepAck, nil)
			return handshakeState{}, fmt.Errorf("client aborted")

		default:
			_ = s.sendOptReply(conn, optType, nbdRepErrUnsup, nil)
		}
	}
}

func nbdTransmissionFlags() uint16 {
	return nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendTrim
}

func (s *Server) sendExportData(conn net.Conn, vol *volume.Volume, state handshakeState) error {
	length := 134
	if state.noZeroes {
		length = 10
	}
	buf := make([]byte, length)
	binary.BigEndian.PutUint64(buf[0:8], uint64(vol.Size))
	binary.BigEndian.PutUint16(buf[8:10], nbdTransmissionFlags())
	_, err := conn.Write(buf)
	return err
}

func (s *Server) handleOptGo(conn net.Conn, vol *volume.Volume, optType uint32) error {
	info := make([]byte, 12)
	binary.BigEndian.PutUint16(info[0:2], nbdInfoExport)
	binary.BigEndian.PutUint64(info[2:10], uint64(vol.Size))
	binary.BigEndian.PutUint16(info[10:12], nbdTransmissionFlags())
	if err := s.sendOptReply(conn, optType, nbdRepInfo, info); err != nil {
		return err
	}
	return s.sendOptReply(conn, optType, nbdRepAck, nil)
}

func (s *Server) handleOptList(conn net.Conn, optType uint32) error {
	nameBytes := []byte(s.volName)
	data := make([]byte, 4+len(nameBytes))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(nameBytes)))
	copy(data[4:], nameBytes)
	if err := s.sendOptReply(conn, optType, nbdRepServer, data); err != nil {
		return err
	}
	return s.sendOptReply(conn, optType, nbdRepAck, nil)
}

func (s *Server) sendOptReply(conn net.Conn, optType, replyType uint32, data []byte) error {
	hdr := make([]byte, 20)
	binary.BigEndian.PutUint64(hdr[0:8], nbdOptReplyMagic)
	binary.BigEndian.PutUint32(hdr[8:12], optType)
	binary.BigEndian.PutUint32(hdr[12:16], replyType)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(data)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}
	return nil
}
