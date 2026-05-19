package cluster

import (
	"encoding/binary"
	"fmt"

	"github.com/gritive/GrainFS/internal/iam/jwt"
)

// jwtKeyStoreCodecVersion is the wire format version for the JKEY trailer payload.
const jwtKeyStoreCodecVersion uint16 = 1

// encodeJWTKeyStore serializes current and previous jwt.KeySeed values into
// a compact binary payload for the JKEY snapshot trailer.
//
// Wire format:
//
//	[uint16 version=1]
//	[uint8 has_current][uint8 has_previous]
//	if has_current:
//	  [uint16 kidLen][kid bytes][uint32 wrappedLen][wrapped bytes][uint32 dekGen][int64 demotedAt]
//	if has_previous:
//	  [uint16 kidLen][kid bytes][uint32 wrappedLen][wrapped bytes][uint32 dekGen][int64 demotedAt]
func encodeJWTKeyStore(current, previous *jwt.KeySeed) []byte {
	size := 4 // version(2) + has_current(1) + has_previous(1)
	if current != nil {
		size += 2 + len(current.Kid) + 4 + len(current.WrappedSecret) + 4 + 8
	}
	if previous != nil {
		size += 2 + len(previous.Kid) + 4 + len(previous.WrappedSecret) + 4 + 8
	}

	buf := make([]byte, 0, size)
	var hdr [4]byte
	binary.LittleEndian.PutUint16(hdr[0:2], jwtKeyStoreCodecVersion)
	if current != nil {
		hdr[2] = 1
	}
	if previous != nil {
		hdr[3] = 1
	}
	buf = append(buf, hdr[:]...)

	for _, seed := range []*jwt.KeySeed{current, previous} {
		if seed == nil {
			continue
		}
		kidBytes := []byte(seed.Kid)
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(len(kidBytes)))
		buf = append(buf, b[:]...)
		buf = append(buf, kidBytes...)

		var wl [4]byte
		binary.LittleEndian.PutUint32(wl[:], uint32(len(seed.WrappedSecret)))
		buf = append(buf, wl[:]...)
		buf = append(buf, seed.WrappedSecret...)

		var gen [4]byte
		binary.LittleEndian.PutUint32(gen[:], seed.DekGen)
		buf = append(buf, gen[:]...)

		var da [8]byte
		binary.LittleEndian.PutUint64(da[:], uint64(seed.DemotedAt))
		buf = append(buf, da[:]...)
	}
	return buf
}

// decodeJWTKeyStore parses a JKEY trailer payload produced by encodeJWTKeyStore.
func decodeJWTKeyStore(data []byte) (current, previous *jwt.KeySeed, err error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("jwt_key_store_codec: payload too short (%d bytes)", len(data))
	}
	version := binary.LittleEndian.Uint16(data[0:2])
	if version != jwtKeyStoreCodecVersion {
		return nil, nil, fmt.Errorf("jwt_key_store_codec: unknown version %d", version)
	}
	hasCurrent := data[2] != 0
	hasPrevious := data[3] != 0
	pos := 4

	readSeed := func(role string) (*jwt.KeySeed, error) {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("jwt_key_store_codec: truncated at kid length")
		}
		kidLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kidLen > len(data) {
			return nil, fmt.Errorf("jwt_key_store_codec: truncated at kid bytes")
		}
		kid := string(data[pos : pos+kidLen])
		pos += kidLen

		if pos+4 > len(data) {
			return nil, fmt.Errorf("jwt_key_store_codec: truncated at wrapped length")
		}
		wrappedLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+wrappedLen > len(data) {
			return nil, fmt.Errorf("jwt_key_store_codec: truncated at wrapped bytes")
		}
		wrapped := append([]byte(nil), data[pos:pos+wrappedLen]...)
		pos += wrappedLen

		if pos+4 > len(data) {
			return nil, fmt.Errorf("jwt_key_store_codec: truncated at dekGen")
		}
		dekGen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+8 > len(data) {
			return nil, fmt.Errorf("jwt_key_store_codec: truncated at demotedAt")
		}
		demotedAt := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8

		return &jwt.KeySeed{
			Kid:           kid,
			WrappedSecret: wrapped,
			DekGen:        dekGen,
			Role:          role,
			DemotedAt:     demotedAt,
		}, nil
	}

	if hasCurrent {
		current, err = readSeed("current")
		if err != nil {
			return nil, nil, err
		}
	}
	if hasPrevious {
		previous, err = readSeed("previous")
		if err != nil {
			return nil, nil, err
		}
	}
	return current, previous, nil
}
