package cluster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWriteQuorumMetaLocal_ConcurrentWriteReadNeverTorn is the regression guard
// for the quorum-meta torn-read race behind the flaky TestECRewrap_ConfigUpgradeRace
// ("upgrade head object: object not found").
//
// writeQuorumMetaLocal must publish the blob atomically: a concurrent reader of the
// same (bucket, key) must observe either the previous complete blob or the new one,
// never a truncated/empty file. The old O_TRUNC in-place write exposed a window
// where the file was 0 bytes mid-write; a reader hitting that window decoded an
// empty blob and the caller reported the object as missing. This happens whenever a
// write overlaps a read of the same key: a same-key overwrite PUT racing a GET/HEAD
// in production, or (as in the EC-upgrade test) a lingering best-effort quorum write
// still in flight after fanOutQuorumMeta returned on the k-th ack.
//
// With the in-place write this test fails (some reads decode an empty/short blob);
// with the tmp+rename write every successful read decodes cleanly.
func TestWriteQuorumMetaLocal_ConcurrentWriteReadNeverTorn(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 2048, ContentType: "application/octet-stream",
		ETag: "etag", VersionID: "v1", ECData: 1, ECParity: 1, NodeIDs: []string{"self", "self"},
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)

	// Seed so the file always exists; readers should only ever see a full blob.
	require.NoError(t, svc.writeQuorumMetaLocal("b", "k", blob))

	const writers, reads = 4, 4000
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers hammer the same key (each a full re-publish).
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = svc.writeQuorumMetaLocal("b", "k", blob)
				}
			}
		}()
	}

	// Reader asserts every present read decodes to the full command (never torn).
	for i := 0; i < reads; i++ {
		raw, rerr := svc.readQuorumMetaRaw("b", "k")
		if rerr != nil {
			// ErrObjectNotFound is impossible here (the file is seeded and only ever
			// renamed over), so any error is a torn read.
			t.Fatalf("read %d: quorum meta read errored (torn read): %v", i, rerr)
		}
		require.NotEmpty(t, raw, "read %d: observed an empty quorum-meta blob (torn O_TRUNC write)", i)
		decoded, derr := decodeQuorumMetaBlob(raw)
		require.NoError(t, derr, "read %d: observed a partial quorum-meta blob (torn write)", i)
		require.Equal(t, cmd.Key, decoded.Key, "read %d: wrong decoded blob", i)
	}

	close(stop)
	wg.Wait()
}
