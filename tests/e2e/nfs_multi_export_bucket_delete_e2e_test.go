package e2e

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func runNFSExportCases(t *testing.T, tgt *nfsTarget) {
	t.Run("BucketDeleteCascade", func(t *testing.T) {
		bucket, _ := tgt.uniqueExport(t, "delete-cascade")
		out, code := runCLI(t, tgt.dataDir(tgt.leaderIdx), "bucket", "delete", bucket, "--force")
		require.Equalf(t, 0, code, "bucket delete failed: %s", out)
		require.Eventually(t, func() bool {
			return !exportListHasBucketOnDataDir(t, tgt.dataDir(0), bucket)
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("BucketDeleteFailureKeepsExport", func(t *testing.T) {
		bucket, _ := tgt.uniqueExport(t, "delete-failure")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := tgt.s3Client(0).PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("key.txt"),
			Body:   bytes.NewReader([]byte("still here")),
		})
		require.NoError(t, err)
		out, code := runCLI(t, tgt.dataDir(tgt.leaderIdx), "bucket", "delete", bucket)
		require.NotEqual(t, 0, code, out)
		require.Contains(t, out, "conflict")
		require.True(t, exportListHasBucketOnDataDir(t, tgt.dataDir(0), bucket))
	})
}

func TestE2E_NFSExportCasesSingleNode(t *testing.T) {
	runNFSExportCases(t, newSingleNodeNFSTarget(t))
}

func TestE2E_NFSExportCasesCluster(t *testing.T) {
	runNFSExportCases(t, newSharedClusterNFSTarget(t))
}

// exportListHasBucketOnDataDir is the dataDir-parameterized form of
// exportListHasBucket from the old single-fixture tests.
func exportListHasBucketOnDataDir(t *testing.T, dataDir, bucket string) bool {
	t.Helper()
	rows := listNfsExportsOnDataDir(t, dataDir)
	for _, row := range rows {
		if row.Bucket == bucket {
			return true
		}
	}
	return false
}
