//go:build compat

package compat

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

// TestRestartToOlderBinary is a canary / documentation test for downgrade behavior.
//
// It writes data with the current binary, then attempts to restart the same data
// directory with an older binary. GrainFS does NOT guarantee downgrade support.
// The test observes one of two outcomes without asserting either is mandatory:
//
//   - The old binary rejects the data dir and exits quickly (HTTP port never
//     becomes ready within the 10 s window) — expected "protected" behavior.
//   - The old binary starts and can serve the data — backwards-compatible layout.
//
// Either outcome causes the test to pass. Run with COMPAT_PREV_BIN set;
// otherwise the test is skipped.
func TestRestartToOlderBinary(t *testing.T) {
	prev := prevBinary(t)
	cur := getBinary()

	dataDir := t.TempDir()
	encKeyFile := makeSharedEncryptionKeyFile(t)

	ports := uniqueFreePorts(4)
	curHTTP, prevHTTP := ports[0], ports[1]
	curRaft, prevRaft := ports[2], ports[3]

	// Phase 1: start with current binary, write data.
	cmd1 := startGrainfsNode(t, cur, dataDir, curHTTP, curRaft, encKeyFile)
	require.NoError(t, waitForPort(curHTTP, 60*time.Second), "wait for cur node HTTP port")
	time.Sleep(2 * time.Second)

	ak, sk := bootstrapCompatAdmin(t, dataDir, 30*time.Second)
	curClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", curHTTP), ak, sk)

	bucket := "downgrade-test"
	_, err := curClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	_, err = curClient.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("canary"),
		Body:   strings.NewReader("downgrade-canary"),
	})
	require.NoError(t, err)

	terminateProcess(cmd1)

	// Phase 2: attempt to restart the same data dir with the old binary.
	oldCmd := startGrainfsNode(t, prev, dataDir, prevHTTP, prevRaft, encKeyFile)
	t.Cleanup(func() { terminateProcess(oldCmd) })

	if err := waitForPort(prevHTTP, 10*time.Second); err != nil {
		// Old binary did not accept the data dir — expected downgrade-protection behavior.
		t.Log("old binary rejected new data dir (downgrade protection — expected)")
		return
	}

	// Old binary started — check whether data is still readable.
	time.Sleep(2 * time.Second)
	oldClient := newCompatS3Client(fmt.Sprintf("http://127.0.0.1:%d", prevHTTP), ak, sk)
	res, getErr := oldClient.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("canary"),
	})
	if getErr != nil {
		t.Logf("old binary started but could not read data: %v (partial downgrade tolerance)", getErr)
		return
	}
	defer res.Body.Close()
	t.Log("old binary accepted new data and served objects (backwards-compatible data layout)")
}
