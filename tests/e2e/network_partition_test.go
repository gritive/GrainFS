package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("Network partition", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("keeps pre-partition writes intact after recovery", func() {
			runNetworkPartitionWithWrite(ginkgo.GinkgoTB())
		})
	})
})

func runNetworkPartitionWithWrite(t testing.TB) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-network-partition-*")
	require.NoError(t, err)
	ginkgo.DeferCleanup(func() { _ = os.RemoveAll(dir) })
	binary := getBinary()
	port := freePort()
	toxiPort := freePort()
	proxyPort := freePort()

	toxiproxyCmd := exec.Command("toxiproxy-server", "-port", fmt.Sprintf("%d", toxiPort))
	toxiproxyCmd.Stdout = os.Stdout
	toxiproxyCmd.Stderr = os.Stderr
	err = toxiproxyCmd.Start()
	if err != nil {
	}
	ginkgo.DeferCleanup(func() {
		if toxiproxyCmd != nil && toxiproxyCmd.Process != nil {
			_ = toxiproxyCmd.Process.Kill()
			_ = toxiproxyCmd.Wait()
		}
	})
	time.Sleep(2 * time.Second)

	// Start grainfs behind toxiproxy proxy
	ctx := context.Background()

	// Create proxy: 127.0.0.1:{proxyPort} -> 127.0.0.1:{port}. Avoid
	// localhost here because Toxiproxy may resolve it to ::1 while GrainFS is
	// only readiness-checked on IPv4 in this e2e harness.
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/proxies", toxiPort)
	proxyPayload := fmt.Sprintf(`{"name":"grainfs","upstream":"127.0.0.1:%d","listen":"127.0.0.1:%d"}`, port, proxyPort)
	req, _ := http.NewRequest("POST", proxyURL, strings.NewReader(proxyPayload))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err, "create toxiproxy proxy")
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create toxiproxy proxy")
	resp.Body.Close()

	// Start grainfs on actual port
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	ginkgo.DeferCleanup(func() { terminateProcess(cmd) })

	waitForPort(t, proxyPort, 30*time.Second)
	waitForPort(t, port, 30*time.Second)

	// Bootstrap admin SA via the on-disk admin UDS (not through the
	// proxy — UDS is on the same host as grainfs).
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey

	// Create bucket and write data via proxy
	s3Client := s3ClientFor(fmt.Sprintf("http://127.0.0.1:%d", proxyPort), ak, sk)
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, bootstrap.SAID, "partition-test", s3Client)

	// Write data before partition
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("partition-test"),
		Key:    aws.String("before-partition"),
		Body:   strings.NewReader("data before partition"),
	})
	require.NoError(t, err)

	// Inject network partition: 100% packet loss
	toxicURL := fmt.Sprintf("http://127.0.0.1:%d/proxies/grainfs/toxics", toxiPort)
	toxicPayload := `{"name":"partition","type":"timeout","stream":"upstream","toxicity":1.0,"attributes":{"timeout":0}}`
	req, _ = http.NewRequest("POST", toxicURL, strings.NewReader(toxicPayload))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, resp.StatusCode, "create network partition toxic")
	resp.Body.Close()

	// Attempt write during partition (should fail or timeout)
	t.Log("Writing during network partition (expected to fail)...")
	partitionCtx, cancelPartitionWrite := context.WithTimeout(ctx, 3*time.Second)
	ginkgo.DeferCleanup(cancelPartitionWrite)
	_, err = s3Client.PutObject(partitionCtx, &s3.PutObjectInput{
		Bucket: aws.String("partition-test"),
		Key:    aws.String("during-partition"),
		Body:   strings.NewReader("data during partition"),
	})
	require.Error(t, err, "write during network partition should fail")

	// Remove partition
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("http://127.0.0.1:%d/proxies/grainfs/toxics/partition", toxiPort), nil)
	resp, err = client.Do(req)
	if err == nil {
		resp.Body.Close()
	}

	// Wait for recovery
	time.Sleep(5 * time.Second)

	// Verify data before partition is still there
	t.Log("Verifying data integrity after partition recovery...")
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("partition-test"),
		Key:    aws.String("before-partition"),
	})
	require.NoError(t, err, "data before partition should be intact")
	ginkgo.DeferCleanup(func() { _ = getResp.Body.Close() })

	content, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	require.Equal(t, "data before partition", string(content))

	t.Log("Network partition test passed - data integrity verified")
}
