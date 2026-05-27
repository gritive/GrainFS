package eccodec

import "github.com/gritive/GrainFS/internal/encrypt"

// ShardEncryptor is eccodec's use-site view of the data-at-rest seam
// (storage.DataEncryptor). Defined here so eccodec need not import
// internal/storage (which would create an import cycle); *storage.EncryptorAdapter
// and *storage.DEKKeeperAdapter satisfy it structurally.
type ShardEncryptor interface {
	Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
}

// chunkFields returns baseFields with the per-chunk ordinal appended. The
// returned slice is freshly allocated so concurrent callers (cpupool) never
// race on a shared backing array.
func chunkFields(baseFields []encrypt.AADField, chunkIdx uint32) []encrypt.AADField {
	out := make([]encrypt.AADField, len(baseFields)+1)
	copy(out, baseFields)
	out[len(baseFields)] = encrypt.FieldUint32(chunkIdx)
	return out
}
