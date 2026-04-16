package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// JepsenTestRunner executes concurrent operations to test linearizability.
type JepsenTestRunner struct {
	clients    []*s3.Client
	numClients int
	numOps     int
}

// NewJepsenTestRunner creates a test runner with multiple S3 clients.
func NewJepsenTestRunner(endpoint string, numClients, numOps int) *JepsenTestRunner {
	clients := make([]*s3.Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = newS3Client(endpoint)
	}
	return &JepsenTestRunner{
		clients:    clients,
		numClients: numClients,
		numOps:     numOps,
	}
}

// RunConcurrentPuts executes concurrent PUT operations to the same key.
func (j *JepsenTestRunner) RunConcurrentPuts(ctx context.Context, bucket, key string) []error {
	var wg sync.WaitGroup
	errors := make([]error, j.numClients)

	for i := 0; i < j.numClients; i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("client-%d-value-%d", clientIdx, time.Now().UnixNano()))
			_, err := j.clients[clientIdx].PutObject(ctx, &s3.PutObjectInput{
				Bucket: &bucket,
				Key:    &key,
				Body:   bytes.NewReader(data),
			})
			errors[clientIdx] = err
		}(i)
	}
	wg.Wait()
	return errors
}

// VerifyLinearizable checks that the final state is consistent (no data loss).
func (j *JepsenTestRunner) VerifyLinearizable(ctx context.Context, t require.TestingT, bucket, key string) {
	// All clients should see the same final value
	var lastContent []byte
	for i := 0; i < j.numClients; i++ {
		resp, err := j.clients[i].GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		require.NoError(t, err, "client %d should get object", i)
		defer resp.Body.Close()

		content, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "client %d should read object", i)

		if lastContent != nil {
			require.Equal(t, string(lastContent), string(content),
				"all clients should see same value (linearizability)")
		}
		lastContent = content
	}
}
