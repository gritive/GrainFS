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

// AADField is one length-prefixed field in an AAD blob.
type AADField struct {
	kind aadFieldKind
	data []byte
}

func FieldString(s string) AADField {
	return AADField{kind: fieldKindString, data: []byte(s)}
}

func FieldBytes(b []byte) AADField {
	return AADField{kind: fieldKindBytes, data: append([]byte(nil), b...)}
}

func FieldUint16(v uint16) AADField {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return AADField{kind: fieldKindU16, data: b}
}

func FieldUint32(v uint32) AADField {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return AADField{kind: fieldKindU32, data: b}
}

func FieldUint64(v uint64) AADField {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return AADField{kind: fieldKindU64, data: b}
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
	if len(clusterID) != 16 {
		panic(fmt.Sprintf("BuildAAD: cluster_id must be 16 bytes, got %d", len(clusterID)))
	}
	if len(fields) > 255 {
		panic(fmt.Sprintf("BuildAAD: too many fields (%d > 255)", len(fields)))
	}

	size := 4 + 2 + 16 + 1
	for _, f := range fields {
		size += 1 + 2 + len(f.data)
	}
	out := make([]byte, 0, size)
	out = append(out, []byte(aadMagic)...)
	out = binary.BigEndian.AppendUint16(out, uint16(domain))
	out = append(out, clusterID...)
	out = append(out, uint8(len(fields)))
	for _, f := range fields {
		out = append(out, byte(f.kind))
		if len(f.data) > 65535 {
			panic("BuildAAD: field exceeds 64KB")
		}
		out = binary.BigEndian.AppendUint16(out, uint16(len(f.data)))
		out = append(out, f.data...)
	}
	return out
}
