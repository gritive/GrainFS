//go:build !race

package eccodec

import (
	"bytes"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

// AllocedBytesPerOp budgets run outside -race: race instrumentation adds its own
// allocations and inflates B/op past the pooling threshold (false failure).

// sealProbeEncryptor is a minimal ShardEncryptor that isolates the ciphertext
// buffer strategy: Seal / SealAtGen return a fresh slice (one per chunk),
// SealTo / SealAtGenTo append into the caller's dst (reused). It does no AAD or
// crypto work, so AllocedBytesPerOp reflects only the sealed-buffer policy --
// not encryptor internals or the per-chunk chunkFields slice (small, common to
// both paths).
type sealProbeEncryptor struct{}

const sealProbeTag = 16

func sealFresh(plain []byte) []byte {
	out := make([]byte, len(plain)+sealProbeTag)
	copy(out, plain)
	return out
}

func sealInto(dst, plain []byte) []byte {
	need := len(plain) + sealProbeTag
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
	}
	copy(dst, plain)
	return dst
}

func (sealProbeEncryptor) Seal(_ encrypt.AADDomain, _ []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return sealFresh(plain), 0, nil
}

func (sealProbeEncryptor) SealTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return sealInto(dst, plain), 0, nil
}

func (sealProbeEncryptor) SealAtGen(_ encrypt.AADDomain, _ []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	return sealFresh(plain), nil
}

func (sealProbeEncryptor) SealAtGenTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	return sealInto(dst, plain), nil
}

func (sealProbeEncryptor) Open(_ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, _ []byte) ([]byte, error) {
	return nil, nil // unused by EncodeEncryptedShard
}

func (sealProbeEncryptor) OpenTo(_ []byte, _ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, _ []byte) ([]byte, error) {
	return nil, nil // unused by EncodeEncryptedShard
}

// TestEncodeEncryptedShard_PoolsSealBuffer guards that the per-chunk ciphertext
// buffer is reused across chunks rather than freshly allocated per chunk.
//
// Pre-fix, EncodeEncryptedShard called enc.Seal / enc.SealAtGen, each returning
// a fresh slice, so a 32-chunk shard allocated ~chunkSize*numChunks of sealed
// buffers (B/op scaled with object size). This is the same un-pooled class #893
// fixed in storage.writeEncryptedObjectFile, and the cluster EC
// multipart-complete micro-bench (BenchmarkClusterMultipart_Complete) exhibited
// it. The fix routes through SealTo / SealAtGenTo with a single reused sealBuf,
// dropping B/op to ~one chunk.
func TestEncodeEncryptedShard_PoolsSealBuffer(t *testing.T) {
	enc := sealProbeEncryptor{}
	fields := shardBaseFields()
	const chunkSize = 4096
	const numChunks = 32
	data := bytes.Repeat([]byte("x"), chunkSize*numChunks)

	res := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := EncodeEncryptedShard(io.Discard, bytes.NewReader(data), enc, fields, chunkSize); err != nil {
				b.Fatal(err)
			}
		}
	})
	bpo := res.AllocedBytesPerOp()

	// One reused buffer => ~chunkSize, independent of chunk count. A per-chunk
	// allocation would be ~chunkSize*numChunks. A quarter of that is a wide
	// margin (covers the small per-chunk chunkFields slices) that still fails
	// decisively on the un-pooled path.
	limit := int64(chunkSize) * numChunks / 4
	require.Lessf(t, bpo, limit,
		"EncodeEncryptedShard allocated %d B/op for %d chunks (limit %d) — sealed buffer not pooled across chunks",
		bpo, numChunks, limit)
}

func TestEncodeEncryptedShard_UsesPooledInitialSealBuffer(t *testing.T) {
	enc := sealProbeEncryptor{}
	fields := shardBaseFields()
	const chunkSize = 4096
	data := bytes.Repeat([]byte("x"), chunkSize)

	res := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := EncodeEncryptedShard(io.Discard, bytes.NewReader(data), enc, fields, chunkSize); err != nil {
				b.Fatal(err)
			}
		}
	})
	bpo := res.AllocedBytesPerOp()

	require.Lessf(t, bpo, int64(chunkSize/2),
		"EncodeEncryptedShard allocated %d B/op for one chunk; first seal buffer should come from pool", bpo)
}
