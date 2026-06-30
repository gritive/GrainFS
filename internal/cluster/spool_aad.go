package cluster

import "github.com/gritive/GrainFS/internal/encrypt"

// spoolRecordAADFieldsInto binds the per-spool domain string (e.g.
// "cluster-spool:<counter>", "cluster-multipart-part:<uploadID>:<n>") AND the
// record index into the AAD under DomainSpool. Including the record index keeps
// per-frame positional binding (records within one spool cannot be reordered,
// duplicated, or spliced without AEAD failure — preserving the old
// domain+record AAD). The domain string must be identical on write and read;
// the encrypted multipart-part codec recomputes it deterministically from IDs
// (uploadID/partNumber) — never from a filesystem path.
func spoolRecordAADFieldsInto(dst []encrypt.AADField, domain string, record uint64) []encrypt.AADField {
	if cap(dst) < 2 {
		dst = make([]encrypt.AADField, 2)
	} else {
		dst = dst[:2]
	}
	dst[0] = encrypt.FieldString(domain)
	dst[1] = encrypt.FieldUint64(record)
	return dst
}
