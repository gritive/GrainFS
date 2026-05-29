package cluster

import "github.com/gritive/GrainFS/internal/encrypt"

// spoolRecordAADFields binds the per-spool domain string (e.g.
// "cluster-spool:<counter>", "cluster-multipart-part:<uploadID>:<n>") AND the
// record index into the AAD under DomainSpool. Including the record index keeps
// per-frame positional binding (records within one spool cannot be reordered,
// duplicated, or spliced without AEAD failure — preserving the old
// domain+record AAD). The domain string must be identical on write and read;
// spool callers carry it on the in-memory spooledObject or recompute it
// deterministically from IDs — never from a filesystem path.
func spoolRecordAADFields(domain string, record uint64) []encrypt.AADField {
	return []encrypt.AADField{encrypt.FieldString(domain), encrypt.FieldUint64(record)}
}
