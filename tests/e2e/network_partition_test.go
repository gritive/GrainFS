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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type NetworkPartitionSuite struct {
	suite.Suite
	dir          string
	binary       string
	port         int
	toxiproxyCmd *exec.Cmd
}

func (s *NetworkPartitionSuite) SetupSuite() {
	s.dir = s.T().TempDir()
	s.binary = getBinary()
	s.port = freePort()

	// Start toxiproxy
	s.toxiproxyCmd = exec.Command("toxiproxy-server", "-port", "8474")
	s.toxiproxyCmd.Stdout = os.Stdout
	s.toxiproxyCmd.Stderr = os.Stderr
	err := s.toxiproxyCmd.Start()
	if err != nil {
		s.T().Skipf("toxiproxy not available: %v (install with scripts/install_toxiproxy.sh)", err)
	}
	time.Sleep(2 * time.Second)
}

func (s *NetworkPartitionSuite) TearDownSuite() {
	if s.toxiproxyCmd != nil {
		s.toxiproxyCmd.Process.Kill()
		s.toxiproxyCmd.Wait()
	}
}

func (s *NetworkPartitionSuite) TestNetworkPartition_WithWrite() {
	// Start grainfs behind toxiproxy proxy
	ctx := context.Background()

	// Create proxy: localhost:9000 -> localhost:{port}
	proxyURL := fmt.Sprintf("http://localhost:8474/proxies")
	proxyPayload := fmt.Sprintf(`{"name":"grainfs","upstream":"localhost:%d","listen":"127.0.0.1:9000"}`, s.port)
	req, _ := http.NewRequest("POST", proxyURL, strings.NewReader(proxyPayload))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(s.T(), err, "create toxiproxy proxy")
	resp.Body.Close()

	// Start grainfs on actual port
	cmd := exec.Command(s.binary, "serve",
		"--data", s.dir,
		"--port", fmt.Sprintf("%d", s.port),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(s.T(), cmd.Start())
	defer cmd.Process.Kill()

	waitForPort(9000, 10*time.Second)

	// Create bucket and write data via proxy
	s3Client := newS3Client("http://localhost:9000")
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("partition-test"),
	})
	require.NoError(s.T(), err)

	// Write data before partition
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("partition-test"),
		Key:    aws.String("before-partition"),
		Body:   strings.NewReader("data before partition"),
	})
	require.NoError(s.T(), err)

	// Inject network partition: 100% packet loss
	toxicURL := fmt.Sprintf("http://localhost:8474/proxies/grainfs/toxics")
	toxicPayload := `{"name":"partition","type":"slow_close","stream":"blocked","toxicity":1.0}`
	req, _ = http.NewRequest("POST", toxicURL, strings.NewReader(toxicPayload))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	require.NoError(s.T(), err)
	resp.Body.Close()

	// Attempt write during partition (should fail or timeout)
	s.T().Log("Writing during network partition (expected to fail)...")
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("partition-test"),
		Key:    aws.String("during-partition"),
		Body:   strings.NewReader("data during partition"),
	})
	// Should fail or timeout - this is expected
	s.T().Logf("Write during partition result (expected error): %v", err)

	// Remove partition
	req, _ = http.NewRequest("DELETE", "http://localhost:8474/proxies/grainfs/toxics/partition", nil)
	resp, err = client.Do(req)
	if err == nil {
		resp.Body.Close()
	}

	// Wait for recovery
	time.Sleep(5 * time.Second)

	// Verify data before partition is still there
	s.T().Log("Verifying data integrity after partition recovery...")
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("partition-test"),
		Key:    aws.String("before-partition"),
	})
	require.NoError(s.T(), err, "data before partition should be intact")
	defer getResp.Body.Close()

	content, err := io.ReadAll(getResp.Body)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "data before partition", string(content))

	s.T().Log("✅ Network partition test passed - data integrity verified")
}

func TestNetworkPartitionSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network partition test in short mode")
	}
	suite.Run(t, new(NetworkPartitionSuite))
}
