package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/require"
)

type incidentState struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Cause string `json:"cause"`
	Proof struct {
		Status    string `json:"status"`
		ReceiptID string `json:"receipt_id"`
	} `json:"proof"`
	Scope struct {
		Bucket  string `json:"bucket"`
		Key     string `json:"key"`
		ShardID int    `json:"shard_id"`
	} `json:"scope"`
}

func fetchIncidents(t *testing.T, endpoint string) []incidentState {
	t.Helper()
	resp, err := http.Get(endpoint + "/api/incidents?limit=50")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out []incidentState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func TestClusterIncidentMissingShardFixedWithReceiptE2E(t *testing.T) {
	const (
		clusterKey = "E2E-CLUSTER-INCIDENT-KEY"
		bucketName = "incident-bucket"
		keyName    = "incident-obj"
		numNodes   = 3
	)

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         numNodes,
		Mode:          ClusterModeStaticPeers,
		ClusterKey:    clusterKey,
		LogPrefix:     "grainfs-incident",
		ScrubInterval: "2s",
		DisableNFS:    true,
		DisableNBD:    true,
	})
	c.GrantAdminOnBuckets(bucketName)
	accessKey, secretKey := c.accessKey, c.secretKey
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	endpoints := c.httpURLs
	leaderIdx, err := waitForWritableEndpoint(ctx, endpoints, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
		return tryCreateBucket(attemptCtx, ecS3Client(endpoint, accessKey, secretKey), bucketName)
	})
	require.NoError(t, err)
	client := ecS3Client(endpoints[leaderIdx], accessKey, secretKey)

	payload := make([]byte, 256*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucketName), Key: aws.String(keyName), Body: bytes.NewReader(payload)})
	require.NoError(t, err)

	var victimNode int
	var victimShard string
	for i := range numNodes {
		root := filepath.Join(c.dataDirs[i], "shards", bucketName, keyName)
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, _ error) error {
			if d != nil && !d.IsDir() && filepath.Base(p) == "shard_0" {
				victimNode, victimShard = i, p
				return filepath.SkipAll
			}
			return nil
		})
		if victimShard != "" {
			break
		}
	}
	require.NotEmpty(t, victimShard)
	require.NoError(t, os.Remove(victimShard))

	require.Eventually(t, func() bool {
		info, err := os.Stat(victimShard)
		return err == nil && info.Size() > 0
	}, 30*time.Second, 500*time.Millisecond)

	var found incidentState
	require.Eventually(t, func() bool {
		for _, item := range fetchIncidents(t, endpoints[victimNode]) {
			if item.Cause == "missing_shard" && item.Scope.Bucket == bucketName && item.Scope.Key == keyName && item.State == "fixed" {
				found = item
				return item.Proof.Status == "signed" && item.Proof.ReceiptID != ""
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "fixed missing-shard incident with signed proof not found")

	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
		if testing.Verbose() {
			o.Logger = logging.NewStandardLogger(os.Stderr)
			o.LogSigning = true
		}
	})
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	_, receiptStatus := signedGet(t, ctx, signer, creds, endpoints[victimNode]+"/api/receipts/"+url.PathEscape(found.Proof.ReceiptID))
	require.Equal(t, http.StatusOK, receiptStatus)
}

func TestQuarantineIncidentE2E(t *testing.T) {
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
	}

	const (
		clusterKey = "E2E-QUARANTINE-INCIDENT-KEY"
		bucketName = "qi-bucket"
		numNodes   = 3
	)
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         numNodes,
		Mode:          ClusterModeStaticPeers,
		ClusterKey:    clusterKey,
		LogPrefix:     "grainfs-quarantine-incident",
		ScrubInterval: "2s",
		DisableNFS:    true,
		DisableNBD:    true,
	})
	c.GrantAdminOnBuckets(bucketName)
	accessKey, secretKey := c.accessKey, c.secretKey
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	leaderIdx, err := waitForWritableEndpoint(ctx, c.httpURLs, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
		return tryCreateBucket(attemptCtx, ecS3Client(endpoint, accessKey, secretKey), bucketName)
	})
	require.NoError(t, err)
	client := c.S3Client(leaderIdx)
	require.Eventually(t, func() bool {
		return tryPutObject(ctx, client, bucketName, "bad", []byte(strings.Repeat("bad", 1024))) == nil
	}, 120*time.Second, time.Second)
	require.Eventually(t, func() bool {
		return tryPutObject(ctx, client, bucketName, "good", []byte("good")) == nil
	}, 120*time.Second, time.Second)

	var corruptShard string
	for i := range numNodes {
		root := filepath.Join(c.dataDirs[i], "shards", bucketName, "bad")
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, _ error) error {
			if d != nil && !d.IsDir() && filepath.Base(p) == "shard_0" {
				corruptShard = p
				return filepath.SkipAll
			}
			return nil
		})
		if corruptShard != "" {
			break
		}
	}
	require.NotEmpty(t, corruptShard)
	f, err := os.OpenFile(corruptShard, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.Eventually(t, func() bool {
		for _, endpoint := range c.httpURLs {
			for _, item := range fetchIncidents(t, endpoint) {
				if item.Cause == "corrupt_blob" || item.Cause == "corrupt_shard" {
					return item.State == "isolated" || item.State == "needs-human"
				}
			}
		}
		return false
	}, 90*time.Second, 500*time.Millisecond)

	_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String("good")})
	require.NoError(t, err, "unrelated object in same bucket must keep working")
}
