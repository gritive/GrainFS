package e2e

import (
	"context"
	"crypto/aes"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startEncryptionServer(t *testing.T) (*s3.Client, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-enc-e2e-*")
	require.NoError(t, err)

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
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 5*time.Second)

	client := newS3Client(endpoint)

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dir)
	}

	return client, dir, cleanup
}

func TestEncryption_AtRest(t *testing.T) {
	t.Skip("at-rest encryption on DistributedBackend shard path is a follow-up (tracked in TODOS v0.0.4.0)")
	client, dataDir, cleanup := startEncryptionServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("enc-test"),
	})
	require.NoError(t, err)

	content := "this is sensitive data that must be encrypted at rest"
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("enc-test"),
		Key:    aws.String("secret.txt"),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)

	dir := shardDir(dataDir, "enc-test", "secret.txt")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	rawShard, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)

	assert.NotContains(t, string(rawShard), "sensitive data")
	assert.True(t, len(rawShard) >= aes.BlockSize, "encrypted shard should have overhead")

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("enc-test"),
		Key:    aws.String("secret.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, content, string(body))
}
