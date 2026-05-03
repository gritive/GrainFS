package cluster

import (
	"encoding/binary"
	"fmt"
)

// encodeGroupForwardPayload builds the StreamProposeGroupForward payload:
//
//	[4B groupIDLen big-endian][groupID bytes][propose data bytes]
//
// Used by GroupBackend's forward path to dispatch a propose to a remote group's
// raft.Node. Wire is independent of the legacy StreamProposeForward (0x06)
// payload, which remains a raw propose data byte slice.
//
//nolint:unused // package tests pin legacy group-forward wire compatibility.
func encodeGroupForwardPayload(groupID string, data []byte) []byte {
	gid := []byte(groupID)
	out := make([]byte, 4+len(gid)+len(data))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(gid)))
	copy(out[4:4+len(gid)], gid)
	copy(out[4+len(gid):], data)
	return out
}

// decodeGroupForwardPayload reverses encodeGroupForwardPayload.
// Returns groupID and the inner data, or an error on malformed input.
//
//nolint:unused // package tests pin legacy group-forward wire compatibility.
func decodeGroupForwardPayload(payload []byte) (string, []byte, error) {
	if len(payload) < 4 {
		return "", nil, fmt.Errorf("groupforward: payload too short: %d bytes", len(payload))
	}
	gidLen := binary.BigEndian.Uint32(payload[0:4])
	if int(gidLen) > len(payload)-4 {
		return "", nil, fmt.Errorf("groupforward: groupIDLen=%d exceeds remaining payload=%d",
			gidLen, len(payload)-4)
	}
	gid := string(payload[4 : 4+gidLen])
	data := payload[4+gidLen:]
	return gid, data, nil
}
