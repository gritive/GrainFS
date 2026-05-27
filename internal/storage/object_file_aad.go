package storage

import "github.com/gritive/GrainFS/internal/encrypt"

// objectFileAADFields binds a whole-object encrypted file chunk:
// (bucket, key, chunk_ordinal). Domain DomainShard + clusterID are added by
// the DataEncryptor seam.
func objectFileAADFields(bucket, key string, chunk uint32) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(bucket),
		encrypt.FieldString(key),
		encrypt.FieldUint32(chunk),
	}
}

// segmentFileAADFields binds a segment blob chunk to its unique blobID so
// segments of the same object cannot decrypt under each other's AAD even with
// identical plaintext (replaces the GFOBJENC1 per-segment domain string).
func segmentFileAADFields(bucket, key, blobID string, chunk uint32) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(bucket),
		encrypt.FieldString(key),
		encrypt.FieldString(blobID),
		encrypt.FieldUint32(chunk),
	}
}

// multipartPartAADFields binds a multipart part chunk: (uploadID, partNumber,
// chunk_ordinal).
func multipartPartAADFields(uploadID string, partNumber int, chunk uint32) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(uploadID),
		encrypt.FieldUint32(uint32(partNumber)),
		encrypt.FieldUint32(chunk),
	}
}
