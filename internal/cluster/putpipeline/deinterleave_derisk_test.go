package putpipeline

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestDeinterleaveDerisk proves, in isolation (no production reader touched),
// that the pipeline's stripe-INTERLEAVED shard layout can be reconstructed by a
// de-interleaving reader that knows (StripeBytes, K, M, size). This is the
// option-B mechanism: the writer stays single-pass/interleaved (the 2.89x win),
// and the read side de-interleaves per stripe.
//
// Layout (verified from CPUPool + ECSplitRawInto): the decrypted shard plaintext
// is [8-byte full-size header][stripe0 fragment][stripe1 fragment]... where each
// fragment is StripeBytes/K bytes (the last stripe padded to StripeBytes). The
// production reader mis-reads this body as one contiguous data slice — hence the
// gate ①a mismatch — but de-interleaving recovers the object exactly.
func TestDeinterleaveDerisk(t *testing.T) {
	const (
		k           = 2
		m           = 2
		stripeBytes = 1 << 20 // 1 MiB
		shardHeader = 8       // encodeShardHeader: 8-byte big-endian original size
	)
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))
	defer tr.Close()
	ss := cluster.NewMultiRootShardService(
		[]string{t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()}, tr,
		cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(fakeShardWAL{}))

	p := New(Config{
		DataDirs:    ss.DataDirs(),
		DEKKeeper:   keeper,
		ClusterID:   clusterID,
		ECConfig:    cluster.ECConfig{DataShards: k, ParityShards: m},
		StripeBytes: stripeBytes,
	})
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(sctx))
	}()

	// Non-aligned size exercises last-stripe padding + final trim.
	payload := make([]byte, 4*1024*1024+777)
	for i := range payload {
		payload[i] = byte((i * 7) % 251)
	}
	size := int64(len(payload))
	bucket, shardKey := "external", "obj-deint/v1"
	_, err := p.Put(ctx, PutRequest{Bucket: bucket, Key: shardKey, Body: bytes.NewReader(payload), SizeHint: &size})
	require.NoError(t, err)

	// Read each shard's decrypted plaintext = [8B header][interleaved fragments].
	bodies := make([][]byte, k+m)
	for i := 0; i < k+m; i++ {
		raw, rerr := ss.ReadLocalShard(bucket, shardKey, i)
		require.NoError(t, rerr, "shard %d", i)
		require.Greater(t, len(raw), shardHeader, "shard %d has header+body", i)
		bodies[i] = raw[shardHeader:] // strip the per-object size header
	}

	numStripes := int((size + stripeBytes - 1) / stripeBytes)

	enc, err := reedsolomon.New(k, m)
	require.NoError(t, err)

	// Per-stripe geometry (verified empirically): a full stripe carries
	// StripeBytes of data → fragment = StripeBytes/K; the last stripe carries
	// only the remainder (NOT padded to StripeBytes) → fragment = ceil(rem/K).
	// All k+m shards share the same per-stripe fragment size.
	deinterleave := func(drop []int) []byte {
		out := make([]byte, 0, int(size))
		offset := 0
		remaining := int(size)
		for s := 0; s < numStripes; s++ {
			dS := stripeBytes
			if remaining < dS {
				dS = remaining
			}
			fragSizeS := (dS + k - 1) / k // ceil(dS/k)
			frags := make([][]byte, k+m)
			for i := 0; i < k+m; i++ {
				frags[i] = append([]byte(nil), bodies[i][offset:offset+fragSizeS]...)
			}
			for _, d := range drop {
				frags[d] = nil
			}
			if len(drop) > 0 {
				require.NoError(t, enc.Reconstruct(frags), "stripe %d reconstruct", s)
			}
			stripeData := make([]byte, 0, k*fragSizeS)
			for kk := 0; kk < k; kk++ {
				stripeData = append(stripeData, frags[kk]...)
			}
			out = append(out, stripeData[:dS]...) // trim padding within the stripe
			offset += fragSizeS
			remaining -= dS
		}
		return out
	}

	// v1: all shards present → pure de-interleave (no RS math needed).
	require.Equal(t, payload, deinterleave(nil),
		"de-interleave with all shards present must recover the object")

	// v2: drop a DATA shard → per-stripe reedsolomon reconstruct (this is also
	// gate ③'s degraded-read machinery).
	require.Equal(t, payload, deinterleave([]int{0}),
		"de-interleave + RS reconstruct from survivors (data shard 0 dropped) must recover the object")
}
