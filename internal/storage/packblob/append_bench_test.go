package packblob

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// BenchmarkAppendEncrypted measures the encrypted Append hot path (small-object
// PUT) for both allocs/op and B/op. It is the gate for the SealTo seam-pooling
// change: the seam's allocating Seal churned a fresh ciphertext + AAD per call;
// SealTo + the reintroduced sealed/AAD pools should recover that.
func BenchmarkAppendEncrypted(b *testing.B) {
	key32 := bytes.Repeat([]byte{0x66}, 32)
	enc, err := encrypt.NewEncryptor(key32)
	if err != nil {
		b.Fatal(err)
	}
	bs, err := NewEncryptedBlobStore(b.TempDir(), 256*1024*1024, enc)
	if err != nil {
		b.Fatal(err)
	}
	defer bs.Close()
	// Constant key (matches TestEncryptedBlobStoreAppendKeepsAllocationBound) so
	// the measured allocs/op isolate the seal path, not per-call key formatting.
	const key = "bucket/key"
	payload := bytes.Repeat([]byte("x"), 64*1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bs.Append(key, payload); err != nil {
			b.Fatal(err)
		}
	}
}
