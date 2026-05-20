//go:build colima

package nfs4_colima

import (
	"bytes"
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
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/tests/colimafixture"
	"github.com/stretchr/testify/require"
)

func TestNFS4_ClusterMountWriteVisibleAcrossNodes(t *testing.T) {
	c := colimafixture.StartCluster(t, colimafixture.Options{EnableNFS: true})
	leaderDir := c.DataDirs[c.LeaderIdx]
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")
	bucket := fmt.Sprintf("cluster-nfs4-write-%d", time.Now().UnixNano())

	out, code := runClusterAdminCLI(t, leaderDir, "bucket", "create", bucket)
	require.Equalf(t, 0, code, "bucket create on leader: %s", out)
	grantClusterAdmin(t, c, leaderDir, bucket)

	out, code = runClusterAdminCLI(t, leaderDir, "nfs", "export", "add", bucket)
	require.Equalf(t, 0, code, "nfs export add: %s", out)
	requireClusterS3PutVisible(t, c, bucket, "s3-sanity.txt", []byte("s3-sanity"))

	mnt := fmt.Sprintf("/mnt/grainfs-nfs4-cluster-%d", time.Now().UnixNano())
	mountCmd := fmt.Sprintf(
		"set -e; sudo mkdir -p %s && "+
			"sudo mount -t nfs4 -o vers=4.1,port=%d %s:/%s %s && "+
			"echo cluster-hello | sudo tee %s/cluster-test.txt >/dev/null && "+
			"sudo sync && sudo umount %s",
		mnt, c.NFSPorts[0], hostIP, bucket, mnt, mnt, mnt,
	)
	t.Cleanup(func() {
		colimaSSH("sh", "-c", fmt.Sprintf("sudo umount -l %s 2>/dev/null; sudo rmdir %s 2>/dev/null", mnt, mnt)).Run() //nolint:errcheck
	})
	out = runColimaSSH(t, "sh", "-c", mountCmd)
	t.Logf("mount+write output: %s", out)

	for i := 0; i < len(c.HTTPPorts); i++ {
		i := i
		require.Eventuallyf(t, func() bool {
			status := clusterHeadObjectSigned(t, c.HTTPURL(i), c.AccessKey, c.SecretKey, bucket, "cluster-test.txt")
			return status == http.StatusOK
		}, 60*time.Second, 500*time.Millisecond, "node %d did not observe cluster-test.txt", i)
	}
}

func requireClusterS3PutVisible(t *testing.T, c *colimafixture.Cluster, bucket, key string, body []byte) {
	t.Helper()
	cli := s3.NewFromConfig(testAWSConfig(c.HTTPURL(c.LeaderIdx), c.AccessKey, c.SecretKey), func(o *s3.Options) {
		o.UsePathStyle = true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err, "S3 sanity PutObject")
	for i := 0; i < len(c.HTTPPorts); i++ {
		i := i
		require.Eventuallyf(t, func() bool {
			status := clusterHeadObjectSigned(t, c.HTTPURL(i), c.AccessKey, c.SecretKey, bucket, key)
			return status == http.StatusOK
		}, 30*time.Second, 500*time.Millisecond, "node %d did not observe S3 sanity object", i)
	}
}

func testAWSConfig(endpoint, accessKey, secretKey string) aws.Config {
	return aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint, SigningRegion: "us-east-1"}, nil
		}),
	}
}

func clusterHeadObjectSigned(t *testing.T, endpoint, ak, sk, bucket, key string) int {
	t.Helper()
	u := fmt.Sprintf("%s/%s/%s", strings.TrimRight(endpoint, "/"), bucket, key)
	req, err := http.NewRequest(http.MethodHead, u, nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, ak, sk, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func grantClusterAdmin(t *testing.T, c *colimafixture.Cluster, leaderDir, bucket string) {
	t.Helper()
	out, code := runClusterAdminCLI(t, leaderDir, "iam", "grant", "put", c.SAID, bucket, "Admin")
	require.Equalf(t, 0, code, "grant admin on %s: %s", bucket, out)
}

func runClusterAdminCLI(t *testing.T, dataDir string, args ...string) (string, int) {
	t.Helper()
	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}
	full := append(append([]string{}, args...), "--endpoint", dataDir+"/admin.sock")
	cmd := exec.Command(binary, full...)
	out, _ := cmd.CombinedOutput()
	code := 0
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	return string(out), code
}
