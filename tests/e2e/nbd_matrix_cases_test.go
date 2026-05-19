package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func runNBDCases(t *testing.T, tgt *nbdTarget) {
	t.Run("ReadWriteRoundTrip", func(t *testing.T) {
		device := tgt.uniqueDevice(t, "rw-roundtrip", 4*1024*1024)

		// Cluster fixture requires __grainfs_volumes admin grant + bucket
		// creation before volume operations. The single-node fixture handles
		// this internally.
		if tgt.isCluster && tgt.cluster != nil {
			tgt.cluster.GrantAdminOnBuckets("__grainfs_volumes")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.Eventually(t, func() bool {
				_, err := tgt.cluster.S3Client(0).CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("__grainfs_volumes"),
				})
				return err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou")
			}, 30*time.Second, 500*time.Millisecond, "__grainfs_volumes bucket not writable")
		}

		client := dialE2ENBD(t, tgt.nbdAddr(0), device)
		defer client.Close()

		body := []byte("nbd-matrix-roundtrip-payload")
		client.WriteAt(t, 0, body)
		client.Flush(t)
		requireNBDReadEventually(t, client, 0, body)
	})
}

func TestE2E_NBDCasesSingleNode(t *testing.T) {
	skipIfShort(t, "skipping NBD matrix single-node in short mode")
	runNBDCases(t, newSingleNodeNBDTarget(t))
}

func TestE2E_NBDCasesCluster(t *testing.T) {
	skipIfShort(t, "skipping NBD matrix cluster in short mode")
	runNBDCases(t, newSharedClusterNBDTarget(t))
}
