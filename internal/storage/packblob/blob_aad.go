package packblob

import "github.com/gritive/GrainFS/internal/encrypt"

// blobEntryAADFields binds a packed blob entry to its physical location and
// framing: (blobID, offset, key, flags). Domain DomainShard + clusterID are
// prepended by the DataEncryptor seam. This replaces the legacy
// "packblob:v2:"+blobID+offset+flags+key string AAD with positional fields.
func blobEntryAADFields(blobID, offset uint64, key string, flags byte) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldUint64(blobID),
		encrypt.FieldUint64(offset),
		encrypt.FieldString(key),
		encrypt.FieldUint16(uint16(flags)),
	}
}
