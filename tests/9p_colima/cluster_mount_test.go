//go:build colima

package p9_colima

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
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/tests/colimafixture"
	"github.com/stretchr/testify/require"
)

func Test9P_ClusterMountWriteVisibleAcrossNodes(t *testing.T) {
	c := colimafixture.StartCluster(t, colimafixture.Options{EnableP9: true})
	leaderDir := c.DataDirs[c.LeaderIdx]
	bucket := fmt.Sprintf("cluster-9p-write-%d", time.Now().UnixNano())

	out, code := runClusterAdminCLI(t, leaderDir, "bucket", "create", bucket)
	require.Equalf(t, 0, code, "bucket create on leader: %s", out)
	grantClusterAdmin(t, c, leaderDir, bucket)
	requireClusterS3PutVisible(t, c, bucket, "s3-sanity.txt", "s3-sanity")

	colimaSSH("sudo", "modprobe", "9p").Run()           //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet").Run()        //nolint:errcheck
	colimaSSH("sudo", "modprobe", "9pnet_virtio").Run() //nolint:errcheck

	const fileName = "cluster-9p.txt"
	const body = "cluster-9p-hello"
	mountCluster9PNode(t, c, 0, bucket, func(mnt string) {
		runColimaSSH(t, "sh", "-c", fmt.Sprintf("echo -n %q | sudo tee %s/%s >/dev/null", body, mnt, fileName))
		got := runColimaSSH(t, "sudo", "cat", mnt+"/"+fileName)
		require.Equal(t, body, got, "readback on writer node")
	})

	for i := 0; i < len(c.HTTPPorts); i++ {
		i := i
		require.Eventuallyf(t, func() bool {
			status := clusterHeadObjectSigned(t, c.HTTPURL(i), c.AccessKey, c.SecretKey, bucket, fileName)
			return status == http.StatusOK
		}, 60*time.Second, 500*time.Millisecond, "node %d did not observe %s", i, fileName)
	}
}

func mountCluster9PNode(t *testing.T, c *colimafixture.Cluster, nodeIdx int, bucket string, fn func(mnt string)) {
	t.Helper()
	hostIP := envOrDefault("HOST_IP", "192.168.5.2")
	mnt := fmt.Sprintf("/mnt/grainfs-9p-cluster-n%d-%d", nodeIdx, time.Now().UnixNano())
	runColimaSSH(t, "sudo", "mkdir", "-p", mnt)
	runColimaSSH(t, "sudo", "mount", "-t", "9p",
		"-o", fmt.Sprintf("trans=tcp,port=%d,version=9p2000.L,msize=262144,aname=/%s", c.P9Ports[nodeIdx], bucket),
		hostIP, mnt)
	defer func() {
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
		colimaSSH("sudo", "rmdir", mnt).Run()        //nolint:errcheck
	}()
	fn(mnt)
}

func requireClusterS3PutVisible(t *testing.T, c *colimafixture.Cluster, bucket, key, body string) {
	t.Helper()
	cli := s3.NewFromConfig(testAWSConfig(c.HTTPURL(c.LeaderIdx), c.AccessKey, c.SecretKey), func(o *s3.Options) {
		o.UsePathStyle = true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
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
