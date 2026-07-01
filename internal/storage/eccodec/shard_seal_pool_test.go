package eccodec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEncodeEncryptedShardStreamToBuffer_RoundTrip guards the streaming encode
// variant: encoding from an io.Reader (no plaintext []byte materialized) must
// produce GFSENC3 output that decodes back to the original bytes — for both a
// known sizeHint (pre-sized buffer) and an unknown one (-1, no pre-size). This is
// the data-loss gate (write -> read-back -> compare). Byte-for-byte equality with
// the []byte path is NOT asserted: the AEAD nonce differs per encode, so the
// chunk layout matches but the ciphertext bytes do not — round-trip is the
// correct correctness check.
func TestEncodeEncryptedShardStreamToBuffer_RoundTrip(t *testing.T) {
	f := newFakeShardEncryptor(t)
	fields := shardBaseFields()
	data := bytes.Repeat([]byte("shard-stream-payload-"), 500) // >>1 chunk at chunkSize=1024
	const chunkSize = 1024

	for _, sizeHint := range []int64{int64(len(data)), -1} {
		payload, err := EncodeEncryptedShardStreamToBuffer(bytes.NewReader(data), sizeHint, f, fields, chunkSize)
		require.NoError(t, err)

		var got bytes.Buffer
		require.NoError(t, DecodeEncryptedShard(&got, bytes.NewReader(payload), f, fields))
		require.Equalf(t, data, got.Bytes(), "stream encode must round-trip (sizeHint=%d)", sizeHint)
	}
}

// TestEncryptedShardUpperBound_NeverUnderestimates guards that
// EncryptedShardUpperBound is a GUARANTEED upper bound on EncodeEncryptedShard's
// output: callers Grow a bytes.Buffer to it so it never doubles. A bound even one
// byte short would let the buffer grow and silently undo the pre-size, so this
// must hold for every boundary (empty, sub-chunk, exact, partial-tail,
// multi-chunk). It also checks the bound is not absurdly loose (< 2x+pad of
// actual) so the pre-size actually helps.
//
// The companion B/op pooling assertion (TestEncodeEncryptedShard_PoolsSealBuffer)
// lives in shard_seal_pool_norace_test.go since alloc budgets are unreliable
// under -race; this size check is alloc-independent and runs in every mode.
func TestEncryptedShardUpperBound_NeverUnderestimates(t *testing.T) {
	f := newFakeShardEncryptor(t)
	fields := shardBaseFields()
	cases := []struct{ dataLen, chunkSize int }{
		{0, 1024}, {1, 1024}, {1023, 1024}, {1024, 1024}, {1025, 1024},
		{6144, 1024}, {100000, 1024}, {5 << 20, 1 << 20},
	}
	for _, tc := range cases {
		data := bytes.Repeat([]byte("z"), tc.dataLen)
		var buf bytes.Buffer
		require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(data), f, fields, tc.chunkSize))
		ub := EncryptedShardUpperBound(tc.dataLen, tc.chunkSize)
		require.GreaterOrEqualf(t, ub, buf.Len(),
			"upper bound %d < actual %d for dataLen=%d chunkSize=%d", ub, buf.Len(), tc.dataLen, tc.chunkSize)
		require.Lessf(t, ub, buf.Len()*2+128,
			"upper bound %d too loose vs actual %d (dataLen=%d) — pre-size wastes memory", ub, buf.Len(), tc.dataLen)
	}
}
