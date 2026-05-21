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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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

func fetchIncidents(t testing.TB, endpoint string) []incidentState {
	t.Helper()
	resp, err := http.Get(endpoint + "/api/incidents?limit=50")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	var out []incidentState
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&out)).To(gomega.Succeed())
	return out
}

var _ = ginkgo.Describe("Cluster incidents", func() {
	ginkgo.It("fixes a missing shard incident with a signed receipt", func() {
		t := ginkgo.GinkgoTB()
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
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		ginkgo.DeferCleanup(cancel)
		endpoints := c.httpURLs
		leaderIdx, err := c.EnsureBucketWritable(ctx, bucketName, 120*time.Second)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		accessKey, secretKey := c.accessKey, c.secretKey
		client := ecS3Client(endpoints[leaderIdx], accessKey, secretKey)

		payload := make([]byte, 256*1024)
		_, err = rand.Read(payload)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucketName), Key: aws.String(keyName), Body: bytes.NewReader(payload)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		gomega.Expect(victimShard).NotTo(gomega.BeEmpty())
		gomega.Expect(os.Remove(victimShard)).To(gomega.Succeed())

		gomega.Eventually(func() bool {
			info, err := os.Stat(victimShard)
			return err == nil && info.Size() > 0
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(gomega.BeTrue())

		var found incidentState
		gomega.Eventually(func() bool {
			for _, item := range fetchIncidents(t, endpoints[victimNode]) {
				if item.Cause == "missing_shard" && item.Scope.Bucket == bucketName && item.Scope.Key == keyName && item.State == "fixed" {
					found = item
					return item.Proof.Status == "signed" && item.Proof.ReceiptID != ""
				}
			}
			return false
		}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "fixed missing-shard incident with signed proof not found")

		signer := v4.NewSigner(func(o *v4.SignerOptions) {
			o.DisableURIPathEscaping = true
			if testing.Verbose() {
				o.Logger = logging.NewStandardLogger(os.Stderr)
				o.LogSigning = true
			}
		})
		creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
		_, receiptStatus := signedGet(t, ctx, signer, creds, endpoints[victimNode]+"/api/receipts/"+url.PathEscape(found.Proof.ReceiptID))
		gomega.Expect(receiptStatus).To(gomega.Equal(http.StatusOK))
	})

	ginkgo.It("isolates a corrupt shard incident without blocking unrelated objects", func() {
		t := ginkgo.GinkgoTB()
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
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		ginkgo.DeferCleanup(cancel)
		leaderIdx, err := c.EnsureBucketWritable(ctx, bucketName, 120*time.Second)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		client := c.S3Client(leaderIdx)
		badPayload := []byte(strings.Repeat("bad", 128*1024))
		gomega.Eventually(func() bool {
			return tryPutObject(ctx, client, bucketName, "bad", badPayload) == nil
		}).WithTimeout(120 * time.Second).WithPolling(time.Second).Should(gomega.BeTrue())
		gomega.Eventually(func() bool {
			return tryPutObject(ctx, client, bucketName, "good", []byte("good")) == nil
		}).WithTimeout(120 * time.Second).WithPolling(time.Second).Should(gomega.BeTrue())

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
		gomega.Expect(corruptShard).NotTo(gomega.BeEmpty())
		f, err := os.OpenFile(corruptShard, os.O_RDWR, 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = f.Seek(-1, io.SeekEnd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = f.Write([]byte{0xff})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(f.Close()).To(gomega.Succeed())

		gomega.Eventually(func() bool {
			for _, endpoint := range c.httpURLs {
				for _, item := range fetchIncidents(t, endpoint) {
					if item.Cause == "corrupt_blob" || item.Cause == "corrupt_shard" {
						return item.State == "isolated" || item.State == "needs-human"
					}
				}
			}
			return false
		}).WithTimeout(90 * time.Second).WithPolling(500 * time.Millisecond).Should(gomega.BeTrue())

		_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String("good")})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "unrelated object in same bucket must keep working")
	})
})
