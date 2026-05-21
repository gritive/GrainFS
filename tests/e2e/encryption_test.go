package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startEncryptionServer(t testing.TB) (*s3.Client, string, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-enc-e2e-*")
	require.NoError(t, err)
	ginkgo.DeferCleanup(os.RemoveAll, dir)

	keyFile := filepath.Join(dir, "encryption.key")
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(keyFile, key, 0o600))

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--encryption-key-file", keyFile,
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
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
			require.NoError(t, err)

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
			require.NoError(t, err)
			require.NotEmpty(t, shardPaths, "expected encrypted object shards under %s", shardRoot)

			for _, shardPath := range shardPaths {
				rawShard, err := os.ReadFile(shardPath)
				require.NoError(t, err)
				assert.NotContains(t, string(rawShard), "sensitive data", "raw shard %s must not contain plaintext", shardPath)
				assert.NotContains(t, string(rawShard), content, "raw shard %s must not contain full plaintext", shardPath)
			}

			getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String("enc-test"),
				Key:    aws.String("secret.txt"),
			})
			require.NoError(t, err)
			defer getOut.Body.Close()

			body, _ := io.ReadAll(getOut.Body)
			assert.Equal(t, content, string(body))
		})
	})

	ginkgo.Context("Cluster4Node", func() {
		ginkgo.It("starts the shared cluster fixture", func() {
			_ = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
	})
})
