package e2e

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestAppendSizeCapE2E pins the design § Follow-up 2 contract:
//   - RejectAtCap: append over cap returns 400 EntityTooLarge.
//   - ConcurrentRaceAtCap: concurrent appends near cap — FSM is the
//     single source of truth, exactly the first to fit succeeds; the
//     remainder must surface EntityTooLarge or InvalidWriteOffset.
//
// SingleNode intentionally absent: the single-node testbed uses a package-
// global server fixture, so --append-size-cap-bytes overrides would race
// across tests. Cap behavior is identical to cluster (FSM-side check is
// the same code path); cluster coverage is sufficient.
func TestAppendSizeCapE2E(t *testing.T) {
	smallCap := int64(4 * 1024)
	capArg := []string{"--append-size-cap-bytes", fmt.Sprintf("%d", smallCap)}

	t.Run("Cluster4Node", func(t *testing.T) {
		skipIfShort(t, "4-node cluster boot is too slow for -short")
		tgt := newClusterS3TargetWithExtraArgs(t, 4, capArg)
		runSizeCapCases(t, tgt, smallCap)
	})
}

func runSizeCapCases(t *testing.T, tgt s3Target, smallCap int64) {
	t.Helper()
	bucket := "append-size-cap-" + tgt.name
	tgt.createBkt(t, bucket)

	t.Run("RejectAtCap", func(t *testing.T) {
		key := "obj-over"
		body := bytes.Repeat([]byte("x"), int(smallCap-1))
		require.NoError(t, putAppend(tgt.pickNode(0), bucket, key, 0, body))
		err := putAppend(tgt.pickNode(0), bucket, key, smallCap-1, []byte("yz"))
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "EntityTooLarge", apiErr.ErrorCode(),
			"over-cap append must surface EntityTooLarge, got %s", apiErr.ErrorCode())
	})

	if !tgt.isCluster {
		return
	}

	t.Run("ConcurrentRaceAtCap", func(t *testing.T) {
		key := "obj-race"
		prefill := bytes.Repeat([]byte("x"), int(smallCap-4))
		require.NoError(t, putAppend(tgt.pickNode(0), bucket, key, 0, prefill))

		var wg sync.WaitGroup
		var successes atomic.Int64
		var rejects atomic.Int64
		for i := 0; i < tgt.nodes; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := putAppend(tgt.pickNode(i), bucket, key, smallCap-4, []byte("abcd"))
				if err == nil {
					successes.Add(1)
					return
				}
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) {
					code := apiErr.ErrorCode()
					if code == "EntityTooLarge" || code == "InvalidWriteOffset" {
						rejects.Add(1)
						return
					}
				}
				t.Errorf("node %d: unexpected err %v", i, err)
			}(i)
		}
		wg.Wait()
		require.Equal(t, int64(1), successes.Load(), "exactly one append must win")
		require.Equal(t, int64(tgt.nodes-1), rejects.Load(),
			"all losers must surface EntityTooLarge or InvalidWriteOffset")
	})
}
