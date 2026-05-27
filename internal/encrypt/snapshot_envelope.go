package encrypt

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

var snapshotEnvelopeMagic = [4]byte{'G', 'S', 'N', 'E'}

// SnapshotEnvelopeFormatV1 is the only supported on-disk format version.
const SnapshotEnvelopeFormatV1 uint16 = 1

type snapshotEnvelopeHeader struct {
	formatVersion    uint16
	activeKEKVersion uint32
	snapshotID       [16]byte
	clusterID        [16]byte
}

// snapshotEnvelopeHeaderLen is the fixed-size portion of the header (before wrappedDEK).
//
//	magic(4) + formatVersion(2) + activeKEKVersion(4) + snapshotID(16) + clusterID(16) + wrappedDEKLen(4)
const snapshotEnvelopeHeaderLen = 4 + 2 + 4 + 16 + 16 + 4

func (h snapshotEnvelopeHeader) encode(wrappedDEK []byte) []byte {
	out := make([]byte, snapshotEnvelopeHeaderLen, snapshotEnvelopeHeaderLen+len(wrappedDEK))
	copy(out[0:4], snapshotEnvelopeMagic[:])
	binary.BigEndian.PutUint16(out[4:6], h.formatVersion)
	binary.BigEndian.PutUint32(out[6:10], h.activeKEKVersion)
	copy(out[10:26], h.snapshotID[:])
	copy(out[26:42], h.clusterID[:])
	binary.BigEndian.PutUint32(out[42:46], uint32(len(wrappedDEK)))
	out = append(out, wrappedDEK...)
	return out
}

func decodeSnapshotEnvelopeHeader(buf []byte) (h snapshotEnvelopeHeader, wrappedDEK, body []byte, err error) {
	if len(buf) < snapshotEnvelopeHeaderLen {
		return h, nil, nil, fmt.Errorf("snapshot envelope: buffer %d shorter than header %d", len(buf), snapshotEnvelopeHeaderLen)
	}
	if string(buf[0:4]) != string(snapshotEnvelopeMagic[:]) {
		return h, nil, nil, errors.New("snapshot envelope: bad magic")
	}
	h.formatVersion = binary.BigEndian.Uint16(buf[4:6])
	h.activeKEKVersion = binary.BigEndian.Uint32(buf[6:10])
	copy(h.snapshotID[:], buf[10:26])
	copy(h.clusterID[:], buf[26:42])
	wrappedLen := binary.BigEndian.Uint32(buf[42:46])
	wrappedEnd := snapshotEnvelopeHeaderLen + int(wrappedLen)
	if wrappedLen == 0 || wrappedEnd > len(buf) {
		return h, nil, nil, fmt.Errorf("snapshot envelope: wrapped DEK length %d exceeds buffer %d", wrappedLen, len(buf))
	}
	return h, buf[snapshotEnvelopeHeaderLen:wrappedEnd], buf[wrappedEnd:], nil
}

const snapshotDEKSize = 32

// OpenedSnapshotHeader exposes the plaintext header fields after a successful Open.
type OpenedSnapshotHeader struct {
	formatVersion    uint16
	activeKEKVersion uint32
	snapshotID       [16]byte
}

func (h OpenedSnapshotHeader) FormatVersion() uint16    { return h.formatVersion }
func (h OpenedSnapshotHeader) ActiveKEKVersion() uint32 { return h.activeKEKVersion }
func (h OpenedSnapshotHeader) SnapshotID() [16]byte     { return h.snapshotID }

func snapshotDEKWrapAAD(clusterID []byte, sid [16]byte, formatVersion uint16, kekVer uint32) []byte {
	return BuildAAD(DomainSnapshotDEK, clusterID,
		FieldBytes(sid[:]), FieldUint16(formatVersion), FieldUint32(kekVer))
}

func snapshotBodyAAD(clusterID []byte, sid [16]byte, formatVersion uint16, kekVer uint32) []byte {
	return BuildAAD(DomainSnapshotBody, clusterID,
		FieldBytes(sid[:]), FieldUint16(formatVersion), FieldUint32(kekVer))
}

// SealSnapshotEnvelope seals body under a fresh per-snapshot ephemeral DEK and
// wraps that DEK under kek. The ephemeral DEK is used for exactly ONE body seal
// (no nonce reuse possible). clusterID must be 16 bytes; kek must be KEKSize.
func SealSnapshotEnvelope(kek, clusterID []byte, snapshotID [16]byte, activeKEKVersion uint32, body []byte) ([]byte, error) {
	if len(clusterID) != 16 {
		return nil, fmt.Errorf("snapshot envelope: cluster id must be 16 bytes, got %d", len(clusterID))
	}
	dek := make([]byte, snapshotDEKSize)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("snapshot envelope: gen ephemeral DEK: %w", err)
	}
	defer zeroize(dek)

	dekAAD := snapshotDEKWrapAAD(clusterID, snapshotID, SnapshotEnvelopeFormatV1, activeKEKVersion)
	wrappedDEK, err := aesgcmSealWithAAD(kek, dek, dekAAD)
	if err != nil {
		return nil, fmt.Errorf("snapshot envelope: wrap DEK: %w", err)
	}
	bodyAAD := snapshotBodyAAD(clusterID, snapshotID, SnapshotEnvelopeFormatV1, activeKEKVersion)
	sealedBody, err := aesgcmSealWithAAD(dek, body, bodyAAD)
	if err != nil {
		return nil, fmt.Errorf("snapshot envelope: seal body: %w", err)
	}
	hdr := snapshotEnvelopeHeader{
		formatVersion:    SnapshotEnvelopeFormatV1,
		activeKEKVersion: activeKEKVersion,
		clusterID:        toArr16(clusterID),
		snapshotID:       snapshotID,
	}
	return append(hdr.encode(wrappedDEK), sealedBody...), nil
}

// PeekSnapshotEnvelopeHeader reads the plaintext header (and slices) WITHOUT
// decrypting, so a caller can resolve the KEK for header.ActiveKEKVersion()
// before calling OpenSnapshotEnvelope.
func PeekSnapshotEnvelopeHeader(data []byte) (OpenedSnapshotHeader, []byte, []byte, error) {
	hdr, wrappedDEK, body, err := decodeSnapshotEnvelopeHeader(data)
	if err != nil {
		return OpenedSnapshotHeader{}, nil, nil, err
	}
	return OpenedSnapshotHeader{
		formatVersion:    hdr.formatVersion,
		activeKEKVersion: hdr.activeKEKVersion,
		snapshotID:       hdr.snapshotID,
	}, wrappedDEK, body, nil
}

// OpenSnapshotEnvelope reverses SealSnapshotEnvelope. The caller MUST supply the
// kek matching header.active_kek_version (resolved from the KEK store).
func OpenSnapshotEnvelope(kek, clusterID []byte, data []byte) (OpenedSnapshotHeader, []byte, error) {
	var out OpenedSnapshotHeader
	if len(clusterID) != 16 {
		return out, nil, fmt.Errorf("snapshot envelope: cluster id must be 16 bytes, got %d", len(clusterID))
	}
	hdr, wrappedDEK, sealedBody, err := decodeSnapshotEnvelopeHeader(data)
	if err != nil {
		return out, nil, err
	}
	if hdr.formatVersion != SnapshotEnvelopeFormatV1 {
		return out, nil, fmt.Errorf("snapshot envelope: unsupported format version %d", hdr.formatVersion)
	}
	if !bytesEqual16(hdr.clusterID, clusterID) {
		return out, nil, errors.New("snapshot envelope: cluster id mismatch")
	}
	dekAAD := snapshotDEKWrapAAD(clusterID, hdr.snapshotID, hdr.formatVersion, hdr.activeKEKVersion)
	dek, err := aesgcmOpenWithAAD(kek, wrappedDEK, dekAAD)
	if err != nil {
		return out, nil, fmt.Errorf("snapshot envelope: unwrap DEK: %w", err)
	}
	defer zeroize(dek)
	bodyAAD := snapshotBodyAAD(clusterID, hdr.snapshotID, hdr.formatVersion, hdr.activeKEKVersion)
	plainBody, err := aesgcmOpenWithAAD(dek, sealedBody, bodyAAD)
	if err != nil {
		return out, nil, fmt.Errorf("snapshot envelope: open body: %w", err)
	}
	out = OpenedSnapshotHeader{
		formatVersion:    hdr.formatVersion,
		activeKEKVersion: hdr.activeKEKVersion,
		snapshotID:       hdr.snapshotID,
	}
	return out, plainBody, nil
}

func toArr16(b []byte) [16]byte {
	var a [16]byte
	copy(a[:], b)
	return a
}

func bytesEqual16(a [16]byte, b []byte) bool {
	return len(b) == 16 && string(a[:]) == string(b[:16])
}
