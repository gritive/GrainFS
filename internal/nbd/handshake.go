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
	exportName        string
	metaContexts      []nbdMetaContext
}

type optionInfoRequest struct {
	name string
	info []uint16
}

type metaContextRequest struct {
	name     string
	contexts []string
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
		if optLen > nbdMaxOptionPayloadSize {
			_ = s.sendOptReply(conn, optType, nbdRepErrInvalid, nil)
			return handshakeState{}, fmt.Errorf("option payload length %d exceeds max %d", optLen, nbdMaxOptionPayloadSize)
		}

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

		case nbdOptInfo:
			req, err := parseOptionInfoRequest(optData)
			if err != nil {
				if err := s.sendOptReply(conn, optType, nbdRepErrInvalid, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			if req.name != s.volName {
				if err := s.sendOptReply(conn, optType, nbdRepErrUnknown, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			if err := s.sendInfoReplies(conn, vol, optType, req.info); err != nil {
				return handshakeState{}, err
			}
			if err := s.sendOptReply(conn, optType, nbdRepAck, nil); err != nil {
				return handshakeState{}, err
			}

		case nbdOptStructuredReply:
			state.structuredReplies = true
			if err := s.sendOptReply(conn, optType, nbdRepAck, nil); err != nil {
				return handshakeState{}, err
			}

		case nbdOptSetMetaContext:
			if !state.structuredReplies {
				if err := s.sendOptReply(conn, optType, nbdRepErrInvalid, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			req, err := parseMetaContextRequest(optData)
			if err != nil {
				if err := s.sendOptReply(conn, optType, nbdRepErrInvalid, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			if req.name != s.volName {
				if err := s.sendOptReply(conn, optType, nbdRepErrUnknown, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			state.metaContexts = state.metaContexts[:0]
			for _, contextName := range req.contexts {
				if contextName == "base:allocation" {
					state.metaContexts = append(state.metaContexts, nbdMetaContext{id: nbdMetaContextBaseAllocID, name: contextName})
				}
			}
			for _, context := range state.metaContexts {
				if err := s.sendMetaContextReply(conn, optType, context); err != nil {
					return handshakeState{}, err
				}
			}
			if err := s.sendOptReply(conn, optType, nbdRepAck, nil); err != nil {
				return handshakeState{}, err
			}

		case nbdOptListMetaContext:
			req, err := parseMetaContextRequest(optData)
			if err != nil {
				if err := s.sendOptReply(conn, optType, nbdRepErrInvalid, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			if req.name != s.volName {
				if err := s.sendOptReply(conn, optType, nbdRepErrUnknown, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			if len(req.contexts) == 0 {
				if err := s.sendMetaContextReply(conn, optType, nbdMetaContext{id: 0, name: "base:allocation"}); err != nil {
					return handshakeState{}, err
				}
			} else {
				for _, contextName := range req.contexts {
					if contextName == "base:" || contextName == "base:allocation" {
						if err := s.sendMetaContextReply(conn, optType, nbdMetaContext{id: 0, name: "base:allocation"}); err != nil {
							return handshakeState{}, err
						}
						break
					}
				}
			}
			if err := s.sendOptReply(conn, optType, nbdRepAck, nil); err != nil {
				return handshakeState{}, err
			}

		case nbdOptGo:
			req, err := parseOptionInfoRequest(optData)
			if err != nil {
				if err := s.sendOptReply(conn, optType, nbdRepErrInvalid, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			if req.name != s.volName {
				if err := s.sendOptReply(conn, optType, nbdRepErrUnknown, nil); err != nil {
					return handshakeState{}, err
				}
				continue
			}
			state.exportName = req.name
			if err := s.sendInfoReplies(conn, vol, optType, req.info); err != nil {
				return handshakeState{}, err
			}
			return state, s.sendOptReply(conn, optType, nbdRepAck, nil)

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

func (s handshakeState) hasMetaContext(id uint32) bool {
	for _, context := range s.metaContexts {
		if context.id == id {
			return true
		}
	}
	return false
}

func parseOptionInfoRequest(payload []byte) (optionInfoRequest, error) {
	if len(payload) < 6 {
		return optionInfoRequest{}, fmt.Errorf("short option info payload")
	}
	nameLen := int(binary.BigEndian.Uint32(payload[0:4]))
	if nameLen > len(payload)-6 {
		return optionInfoRequest{}, fmt.Errorf("invalid option info name length: %d", nameLen)
	}
	pos := 4 + nameLen
	infoCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if len(payload)-pos != infoCount*2 {
		return optionInfoRequest{}, fmt.Errorf("invalid option info count: %d", infoCount)
	}
	req := optionInfoRequest{
		name: string(payload[4 : 4+nameLen]),
		info: make([]uint16, infoCount),
	}
	for i := range req.info {
		req.info[i] = binary.BigEndian.Uint16(payload[pos : pos+2])
		pos += 2
	}
	return req, nil
}

func parseMetaContextRequest(payload []byte) (metaContextRequest, error) {
	if len(payload) < 8 {
		return metaContextRequest{}, fmt.Errorf("short meta context payload")
	}
	nameLen := int(binary.BigEndian.Uint32(payload[0:4]))
	if nameLen > len(payload)-8 {
		return metaContextRequest{}, fmt.Errorf("invalid meta context name length: %d", nameLen)
	}
	pos := 4 + nameLen
	count := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if count > (len(payload)-pos)/4 {
		return metaContextRequest{}, fmt.Errorf("invalid meta context count: %d", count)
	}
	req := metaContextRequest{
		name:     string(payload[4 : 4+nameLen]),
		contexts: make([]string, 0, count),
	}
	for i := 0; i < count; i++ {
		if len(payload)-pos < 4 {
			return metaContextRequest{}, fmt.Errorf("short meta context entry")
		}
		contextLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if contextLen > len(payload)-pos {
			return metaContextRequest{}, fmt.Errorf("invalid meta context length: %d", contextLen)
		}
		req.contexts = append(req.contexts, string(payload[pos:pos+contextLen]))
		pos += contextLen
	}
	if pos != len(payload) {
		return metaContextRequest{}, fmt.Errorf("trailing meta context payload")
	}
	return req, nil
}

func nbdTransmissionFlags() uint16 {
	return nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendTrim | nbdFlagSendWriteZeroes
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

func (s *Server) sendInfoReplies(conn net.Conn, vol *volume.Volume, optType uint32, requested []uint16) error {
	if err := s.sendExportInfo(conn, vol, optType); err != nil {
		return err
	}
	for _, infoType := range requested {
		switch infoType {
		case nbdInfoExport:
			continue
		case nbdInfoBlockSize:
			if err := s.sendBlockSizeInfo(conn, optType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) sendExportInfo(conn net.Conn, vol *volume.Volume, optType uint32) error {
	info := make([]byte, 12)
	binary.BigEndian.PutUint16(info[0:2], nbdInfoExport)
	binary.BigEndian.PutUint64(info[2:10], uint64(vol.Size))
	binary.BigEndian.PutUint16(info[10:12], nbdTransmissionFlags())
	return s.sendOptReply(conn, optType, nbdRepInfo, info)
}

func (s *Server) sendBlockSizeInfo(conn net.Conn, optType uint32) error {
	info := make([]byte, 14)
	binary.BigEndian.PutUint16(info[0:2], nbdInfoBlockSize)
	binary.BigEndian.PutUint32(info[2:6], nbdMinBlockSize)
	binary.BigEndian.PutUint32(info[6:10], nbdPreferredBlockSize)
	binary.BigEndian.PutUint32(info[10:14], nbdMaxPayloadSize)
	return s.sendOptReply(conn, optType, nbdRepInfo, info)
}

func (s *Server) sendMetaContextReply(conn net.Conn, optType uint32, context nbdMetaContext) error {
	data := make([]byte, 4+len(context.name))
	binary.BigEndian.PutUint32(data[0:4], context.id)
	copy(data[4:], context.name)
	return s.sendOptReply(conn, optType, nbdRepMetaContext, data)
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
