package cluster

import "encoding/binary"

// encodeSoleAuthEpoch encodes the per-bucket soleauth epoch as a 4-byte
// BigEndian uint32.
func encodeSoleAuthEpoch(epoch uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, epoch)
	return buf
}

// decodeSoleAuthEpoch decodes a 4-byte BigEndian uint32 epoch. A malformed or
// absent value (len != 4) decodes to 0.
func decodeSoleAuthEpoch(raw []byte) uint32 {
	if len(raw) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(raw)
}

// soleAuthTransitionAllowed reports whether the one-way soleauth guard permits
// the transition from → to. The allowed transitions are:
//
//	same → same  (idempotent)
//	off  → pending
//	pending → on
//	pending → off  (abort)
//
// Everything else is refused: off→on (must step through pending),
// on→pending and on→off (on is terminal), and any unknown state string.
func soleAuthTransitionAllowed(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case soleAuthOff:
		return to == soleAuthPending
	case soleAuthPending:
		return to == soleAuthOn || to == soleAuthOff
	}
	return false
}
