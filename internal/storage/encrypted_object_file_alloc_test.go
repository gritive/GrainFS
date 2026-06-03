package storage

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// identityDataEncryptor is a non-allocating-on-read stub DataEncryptor used to
// isolate the streaming reader's per-chunk allocation behaviour. "Encryption"
// is identity (ciphertext == plaintext); OpenTo appends into the caller's
// pre-sized dst so it never allocates when dst has capacity. This strips away
// XAES-256-GCM's inherent per-chunk gcm.New allocation so AllocsPerRun measures
// only what the reader itself allocates per chunk (the AAD slice, pre-fix).
type identityDataEncryptor struct{}

func (identityDataEncryptor) Seal(_ encrypt.AADDomain, _ []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return append([]byte(nil), plain...), 1, nil
}

func (identityDataEncryptor) SealTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return append(dst, plain...), 1, nil
}

func (identityDataEncryptor) SealAtGen(_ encrypt.AADDomain, _ []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	return append([]byte(nil), plain...), nil
}

func (identityDataEncryptor) SealAtGenTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	return append(dst, plain...), nil
}

func (identityDataEncryptor) Open(_ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return append([]byte(nil), ct...), nil
}

// OpenTo appends ct into dst. The streaming reader passes a plainBuf pre-sized
// to encryptedChunkSize, so this never reallocates — isolating the AAD slice as
// the only per-chunk read allocation under test.
func (identityDataEncryptor) OpenTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return append(dst, ct...), nil
}

var _ DataEncryptor = identityDataEncryptor{}

// TestEncryptedObjectReaderReusesAADAcrossChunks proves the streaming reader
// reuses one AAD scratch slice for the whole object instead of allocating a
// fresh slice per 128 KiB chunk. With the per-reader `fields` reuse, a full
// read allocates O(1) regardless of chunk count; the pre-fix code allocated
// ≥ chunkCount AAD slices. The non-allocating identity encryptor strips XAES's
// inherent per-chunk gcm.New so AllocsPerRun sees only the reader's own allocs.
func TestEncryptedObjectReaderReusesAADAcrossChunks(t *testing.T) {
	const chunkCount = 32
	enc := identityDataEncryptor{}
	fields := objectFileAADFields("bucket", "key")
	plaintext := bytes.Repeat([]byte("x"), encryptedChunkSize*chunkCount)
	path := filepath.Join(t.TempDir(), "obj.enc")

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader(plaintext), io.Discard)
	if err != nil {
		t.Fatalf("write encrypted object: %v", err)
	}

	readAll := func() {
		rc, err := openEncryptedObjectFile(path, enc, fields, size)
		if err != nil {
			t.Fatalf("open encrypted object: %v", err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			t.Fatalf("read encrypted object: %v", err)
		}
		_ = rc.Close()
	}

	allocs := testing.AllocsPerRun(20, readAll)
	// O(1) setup allocs only (reader struct, plainBuf, sealedBuf, fields slice,
	// copy buffer) — a handful, far below chunkCount. The pre-fix per-chunk AAD
	// allocation would push this to ≥ chunkCount. The threshold sits well below
	// chunkCount so a regression to per-chunk allocation fails loudly.
	if allocs >= chunkCount {
		t.Fatalf("reading a %d-chunk object allocated %.0f times; expected O(1) (per-chunk AAD allocation regressed)", chunkCount, allocs)
	}
}
