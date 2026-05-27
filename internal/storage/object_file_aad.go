package storage

import "github.com/gritive/GrainFS/internal/encrypt"

// objectFileAADFields returns the BASE AAD fields binding a whole-object
// encrypted file: (bucket, key). Domain DomainShard + clusterID are added by
// the DataEncryptor seam; the per-chunk ordinal is appended by the
// encrypted-object-file layer.
func objectFileAADFields(bucket, key string) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(bucket),
		encrypt.FieldString(key),
	}
}

// segmentFileAADFields returns the BASE AAD fields binding a segment blob to
// its unique blobID so segments of the same object cannot decrypt under each
// other's AAD even with identical plaintext (replaces the GFOBJENC1
// per-segment domain string). The per-chunk ordinal is appended by the
// encrypted-object-file layer.
func segmentFileAADFields(bucket, key, blobID string) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(bucket),
		encrypt.FieldString(key),
		encrypt.FieldString(blobID),
	}
}

// multipartPartAADFields returns the BASE AAD fields binding a multipart part:
// (uploadID, partNumber). The per-chunk ordinal is appended by the
// encrypted-object-file layer.
func multipartPartAADFields(uploadID string, partNumber int) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(uploadID),
		encrypt.FieldUint32(uint32(partNumber)),
	}
}
