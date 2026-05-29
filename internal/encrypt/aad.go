package encrypt

import (
	"encoding/binary"
	"fmt"
)

// AADDomain enumerates ciphertext domain tags. Different domains MUST have
// distinct tag values so the AEAD verifier rejects substitution across
// domain boundaries (e.g., a JWT key wrap injected as a shard ciphertext).
type AADDomain uint16

const (
	DomainShard              AADDomain = 0x0001
	DomainWAL                AADDomain = 0x0002
	DomainSnapshotBody       AADDomain = 0x0003
	DomainSnapshotDEK        AADDomain = 0x0004
	DomainJWTKey             AADDomain = 0x0005
	DomainDEKFSMWrap         AADDomain = 0x0006
	DomainKEKRotate          AADDomain = 0x0007
	DomainKEKCatchup         AADDomain = 0x0008
	DomainNBD                AADDomain = 0x0009
	DomainIAMAdmin           AADDomain = 0x000A
	DomainCapabilityAssertV1 AADDomain = 0x000B
	// DomainCASChunk binds content-addressed canonical chunk ciphertext.
	// Keyed by (cluster_id, canonical_locator) only — object-independent —
	// so a single stored copy decrypts for every object that references it.
	DomainCASChunk            AADDomain = 0x000C
	DomainIAMCredential       AADDomain = 0x000D
	DomainFSMValue            AADDomain = 0x000E
	DomainClusterConfigSecret AADDomain = 0x000F
	DomainProtocolCredential  AADDomain = 0x0010
	DomainRaftStoreKey        AADDomain = 0x0011
	DomainSpool               AADDomain = 0x0012
)

const aadMagic = "AAD\x01"

type aadFieldKind uint8

const (
	fieldKindString aadFieldKind = 0x01
	fieldKindBytes  aadFieldKind = 0x02
	fieldKindU16    aadFieldKind = 0x03
	fieldKindU32    aadFieldKind = 0x04
	fieldKindU64    aadFieldKind = 0x05
)

// AADField is one length-prefixed field in an AAD blob. Values are stored
// inline (string for String/Bytes payloads, uint64 for the integer kinds);
// the wire encoding in AppendAAD is unchanged. FieldString and the FieldUint*
// constructors allocate nothing. FieldBytes is the deliberate exception: it
// takes an immutable defensive copy of its input via string(b) (one alloc),
// which is consensus-critical — callers may mutate their input slice after
// construction, so this copy must not be removed to chase zero-alloc.
type AADField struct {
	kind aadFieldKind
	num  uint64
	str  string
}

func FieldString(s string) AADField {
	return AADField{kind: fieldKindString, str: s}
}

func FieldBytes(b []byte) AADField {
	return AADField{kind: fieldKindBytes, str: string(b)}
}

func FieldUint16(v uint16) AADField {
	return AADField{kind: fieldKindU16, num: uint64(v)}
}

func FieldUint32(v uint32) AADField {
	return AADField{kind: fieldKindU32, num: uint64(v)}
}

func FieldUint64(v uint64) AADField {
	return AADField{kind: fieldKindU64, num: v}
}

// BuildAAD assembles a binary AAD blob. cluster_id MUST be exactly 16
// bytes (panics otherwise — programmer error, not user input).
//
// Layout (big-endian integers throughout):
//
//	[magic "AAD\x01"  4B]
//	[domain_tag      uint16]
//	[cluster_id      16B   ]
//	[num_fields      uint8 ]
//	repeat num_fields times:
//	  [field_kind    uint8 ]
//	  [field_len     uint16]
//	  [field_bytes   ...   ]
func BuildAAD(domain AADDomain, clusterID []byte, fields ...AADField) []byte {
	return AppendAAD(nil, domain, clusterID, fields...)
}

// AppendAAD appends the canonical AAD blob (see BuildAAD) to dst and returns
// the extended slice, reusing dst's capacity when it suffices. AppendAAD(nil,
// ...) is byte-identical to BuildAAD.
func AppendAAD(dst []byte, domain AADDomain, clusterID []byte, fields ...AADField) []byte {
	if len(clusterID) != 16 {
		panic(fmt.Sprintf("BuildAAD: cluster_id must be 16 bytes, got %d", len(clusterID)))
	}
	if len(fields) > 255 {
		panic(fmt.Sprintf("BuildAAD: too many fields (%d > 255)", len(fields)))
	}

	out := dst
	out = append(out, []byte(aadMagic)...)
	out = binary.BigEndian.AppendUint16(out, uint16(domain))
	out = append(out, clusterID...)
	out = append(out, uint8(len(fields)))
	for _, f := range fields {
		out = append(out, byte(f.kind))
		switch f.kind {
		case fieldKindString, fieldKindBytes:
			if len(f.str) > 65535 {
				panic("BuildAAD: field exceeds 64KB")
			}
			out = binary.BigEndian.AppendUint16(out, uint16(len(f.str)))
			out = append(out, f.str...)
		case fieldKindU16:
			out = binary.BigEndian.AppendUint16(out, 2)
			out = binary.BigEndian.AppendUint16(out, uint16(f.num))
		case fieldKindU32:
			out = binary.BigEndian.AppendUint16(out, 4)
			out = binary.BigEndian.AppendUint32(out, uint32(f.num))
		case fieldKindU64:
			out = binary.BigEndian.AppendUint16(out, 8)
			out = binary.BigEndian.AppendUint64(out, f.num)
		default:
			panic("BuildAAD: unknown field kind")
		}
	}
	return out
}
