package e2e

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestE2E_NFSMultiExportBucketDeleteCascade(t *testing.T) {
	bucket := fmt.Sprintf("nfs-cascade-e2e-%d", freePort())
	createBucket(t, bucket)
	runNfsExportJSON(t, "add", bucket)

	out, code := runCLI(t, testServerDataDir, "bucket", "delete", bucket, "--force")
	require.Equalf(t, 0, code, "bucket delete failed: %s", out)

	require.Eventually(t, func() bool {
		return !exportListHasBucket(listNfsExports(t), bucket)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestE2E_NFSMultiExportBucketDeleteFailureKeepsExport(t *testing.T) {
	bucket := fmt.Sprintf("nfs-cascade-notempty-e2e-%d", freePort())
	createBucket(t, bucket)
	_, err := testS3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("key.txt"),
		Body:   bytes.NewReader([]byte("still here")),
	})
	require.NoError(t, err)
	runNfsExportJSON(t, "add", bucket)

	out, code := runCLI(t, testServerDataDir, "bucket", "delete", bucket)
	require.NotEqual(t, 0, code, out)
	require.Contains(t, out, "conflict")
	require.True(t, exportListHasBucket(listNfsExports(t), bucket), "export must remain when bucket delete fails")
}

func exportListHasBucket(rows []e2eNfsExport, bucket string) bool {
	for _, row := range rows {
		if row.Bucket == bucket {
			return true
		}
	}
	return false
}
