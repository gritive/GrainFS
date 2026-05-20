package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestS3ClientSmokeE2E exercises host-safe external S3-compatible clients
// against both shared single + shared cluster fixtures. FUSE-backed clients
// run from Linux in tests/fuse_s3_colima because macOS host FUSE availability
// is not a reliable e2e prerequisite.
func TestS3ClientSmokeE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runS3ClientSmokeCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runS3ClientSmokeCases(t, newSharedClusterS3Target(t))
	})
}

func runS3ClientSmokeCases(t *testing.T, tgt s3Target) {
	t.Helper()
	t.Run("MinIOMC", func(t *testing.T) { testS3ClientSmokeMinIOMC(t, tgt) })
}

func testS3ClientSmokeMinIOMC(t *testing.T, tgt s3Target) {
	_, err := exec.LookPath("mc")
	require.NoError(t, err, "mc is required for TestS3ClientSmokeE2E; install MinIO Client or run the Colima-specific FUSE smoke target separately")

	bucket := tgt.uniqueBucket(t, "mc")
	endpoint := tgt.endpoint(0)
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "mc-config")
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	srcPath := filepath.Join(tmpDir, "src.txt")
	require.NoError(t, os.WriteFile(srcPath, []byte("mc smoke"), 0o600))

	runClientCommand(t, nil, "mc", "--config-dir", configDir, "alias", "set", "grainfs", endpoint, tgt.accessKey, tgt.secretKey, "--api", "S3v4", "--path", "on")
	runClientCommand(t, nil, "mc", "--config-dir", configDir, "cp", srcPath, "grainfs/"+bucket+"/smoke.txt")
	out := runClientCommand(t, nil, "mc", "--config-dir", configDir, "cat", "grainfs/"+bucket+"/smoke.txt")
	require.Equal(t, "mc smoke", string(out))
	out = runClientCommand(t, nil, "mc", "--config-dir", configDir, "ls", "grainfs/"+bucket)
	require.Contains(t, string(out), "smoke.txt")
	runClientCommand(t, nil, "mc", "--config-dir", configDir, "rm", "grainfs/"+bucket+"/smoke.txt")
	requireObjectDeleted(t, tgt, bucket, "smoke.txt")
}

func requireObjectDeleted(t *testing.T, tgt s3Target, bucket, key string) {
	t.Helper()
	cli := tgt.pickNode(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		_, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		return err != nil
	}, 10*time.Second, 200*time.Millisecond)
}

func runClientCommand(t *testing.T, env []string, name string, args ...string) []byte {
	t.Helper()
	out, err := runClientCommandAllowError(t, env, name, args...)
	require.NoError(t, err, "%s %s\n%s", name, strings.Join(args, " "), out)
	return out
}

func runClientCommandAllowError(t *testing.T, env []string, name string, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return out, fmt.Errorf("%s timed out", name)
	}
	return out, err
}
