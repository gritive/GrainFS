package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// anyFileUnderContains recursively scans root for any regular file whose bytes
// contain needle. Used to assert spool/part temp files hold no plaintext.
func anyFileUnderContains(root string, needle []byte) bool {
	found := false
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d == nil || d.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		if bytes.Contains(raw, needle) {
			found = true
		}
		return nil
	})
	return found
}

func startEncryptionServer(t testing.TB) (*s3.Client, string, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-enc-e2e-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(removeE2EDir, dir)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 60*time.Second)
	client := s3ClientFor(endpoint, bootstrap.AccessKey, bootstrap.SecretKey)

	return client, dir, bootstrap.SAID
}

var _ = ginkgo.Describe("Encryption at rest", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("does not store plaintext in raw shards", func() {
			t := ginkgo.GinkgoTB()
			client, dataDir, saID := startEncryptionServer(t)

			ctx := context.Background()

			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, saID, "enc-test", client)

			content := strings.Repeat("this is sensitive data that must be encrypted at rest\n", 2048)
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("enc-test"),
				Key:    aws.String("secret.txt"),
				Body:   strings.NewReader(content),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var shardPaths []string
			shardRoot := filepath.Join(dataDir, "shards", "enc-test", "secret.txt")
			err = filepath.WalkDir(shardRoot, func(path string, d os.DirEntry, walkErr error) error {
				if walkErr != nil || d == nil || d.IsDir() {
					return nil
				}
				if strings.HasPrefix(filepath.Base(path), "shard_") {
					shardPaths = append(shardPaths, path)
				}
				return nil
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(shardPaths).NotTo(gomega.BeEmpty(), "expected encrypted object shards under %s", shardRoot)

			for _, shardPath := range shardPaths {
				rawShard, err := os.ReadFile(shardPath)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(string(rawShard)).NotTo(gomega.ContainSubstring("sensitive data"), "raw shard %s must not contain plaintext", shardPath)
				gomega.Expect(string(rawShard)).NotTo(gomega.ContainSubstring(content), "raw shard %s must not contain full plaintext", shardPath)
			}

			getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("enc-test"),
				Key:    aws.String("secret.txt"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(getOut.Body.Close)

			body, _ := io.ReadAll(getOut.Body)
			gomega.Expect(string(body)).To(gomega.Equal(content))
		})

		ginkgo.It("PUTs a large object through the encrypted spool and stores no plaintext temp bytes", func() {
			// Objects above maxSingleLocalShardMemoryFastPathBytes (16 MiB) bypass
			// the in-memory fast path and route through the upload spool even at
			// the single-node 1+0 profile, exercising the DomainSpool seal.
			t := ginkgo.GinkgoTB()
			client, dataDir, saID := startEncryptionServer(t)
			ctx := context.Background()
			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, saID, "spool-test", client)

			marker := []byte("spooled-plaintext-run-marker-")
			payload := bytes.Repeat(marker, (20<<20)/len(marker))
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("spool-test"),
				Key:    aws.String("spooled"),
				Body:   bytes.NewReader(payload),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("spool-test"),
				Key:    aws.String("spooled"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(getOut.Body.Close)
			got, _ := io.ReadAll(getOut.Body)
			gomega.Expect(got).To(gomega.Equal(payload))

			// The PUT spool is deleted after the PUT, so no plaintext run must
			// remain under the tmp dir. Durable ciphertext proof on the live
			// spool is owned by the cluster unit tests.
			gomega.Expect(anyFileUnderContains(filepath.Join(dataDir, "tmp"), marker)).To(gomega.BeFalse())
		})
	})

	ginkgo.Context("Multipart parts at rest", func() {
		ginkgo.It("stores multipart parts as ciphertext on disk and round-trips", func() {
			// Multipart part files are sealed through the DomainSpool seam whenever
			// shard encryption is active (independent of EC profile) and persist
			// until Complete, so the on-disk ciphertext check is race-free.
			t := ginkgo.GinkgoTB()
			client, dataDir, saID := startEncryptionServer(t)
			ctx := context.Background()
			createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, saID, "mpu-test", client)

			marker := []byte("multipart-part-plaintext-marker-")
			part1 := bytes.Repeat(marker, (6<<20)/len(marker))
			part2 := bytes.Repeat(marker, (6<<20)/len(marker))

			initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
				Bucket:      aws.String("mpu-test"),
				Key:         aws.String("mpu-obj"),
				ContentType: aws.String("application/octet-stream"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			uploadID := initOut.UploadId

			p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String("mpu-test"), Key: aws.String("mpu-obj"),
				UploadId: uploadID, PartNumber: aws.Int32(1), Body: bytes.NewReader(part1),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String("mpu-test"), Key: aws.String("mpu-obj"),
				UploadId: uploadID, PartNumber: aws.Int32(2), Body: bytes.NewReader(part2),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Parts persist until Complete: assert ciphertext on disk now.
			gomega.Expect(anyFileUnderContains(filepath.Join(dataDir, "parts"), marker)).To(gomega.BeFalse(),
				"multipart part files must be ciphertext on disk")

			_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket: aws.String("mpu-test"), Key: aws.String("mpu-obj"), UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{PartNumber: aws.Int32(1), ETag: p1.ETag},
						{PartNumber: aws.Int32(2), ETag: p2.ETag},
					},
				},
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("mpu-test"), Key: aws.String("mpu-obj"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(out.Body.Close)
			got, _ := io.ReadAll(out.Body)
			gomega.Expect(got).To(gomega.Equal(append(append([]byte{}, part1...), part2...)))
		})
	})
})
