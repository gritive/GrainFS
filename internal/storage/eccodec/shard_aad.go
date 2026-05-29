package eccodec

import "github.com/gritive/GrainFS/internal/encrypt"

// ShardEncryptor is eccodec's use-site view of the data-at-rest seam
// (storage.DataEncryptor). Defined here so eccodec need not import
// internal/storage (which would create an import cycle); *storage.DEKKeeperAdapter
// satisfies it structurally.
type ShardEncryptor interface {
	Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	// SealTo is Seal that appends the ciphertext into dst, reusing dst's
	// capacity when it suffices. The output is byte-equivalent to Seal. Lets the
	// chunked writer recycle one ciphertext buffer across chunks instead of
	// allocating per chunk. Signature matches storage.DataEncryptor.SealTo.
	SealTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	// SealAtGen seals under a CALLER-SPECIFIED gen instead of the active gen, so
	// every chunk of one shard pins chunk 0's generation even if a DEK rotation
	// races the encode. Signature matches storage.DataEncryptor.SealAtGen so the
	// production adapters satisfy this view structurally.
	SealAtGen(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, gen uint32) (ct []byte, err error)
	// SealAtGenTo is SealAtGen that appends into dst (byte-equivalent to
	// SealAtGen), recycling the writer's ciphertext buffer for the pinned-gen
	// chunks. Signature matches storage.DataEncryptor.SealAtGenTo.
	SealAtGenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, gen uint32) (ct []byte, err error)
	Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
	// OpenTo is Open that appends the plaintext into dst, reusing dst's
	// capacity when it suffices. The output is byte-equivalent to Open.
	// dst and ct MUST NOT overlap. Signature matches storage.DataEncryptor.OpenTo
	// so the production adapters satisfy this view structurally.
	OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
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
