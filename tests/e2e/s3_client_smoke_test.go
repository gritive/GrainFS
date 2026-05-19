package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestS3ClientSmokeE2E exercises three external S3-compatible clients
// (mc, s3fs, goofys) against both shared single + shared cluster fixtures.
// Each client gets its own sub-test and its own bucket via uniqueBucket.
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
	t.Run("S3FS", func(t *testing.T) { testS3ClientSmokeS3FS(t, tgt) })
	t.Run("Goofys", func(t *testing.T) { testS3ClientSmokeGoofys(t, tgt) })
}

func testS3ClientSmokeMinIOMC(t *testing.T, tgt s3Target) {
	if _, err := exec.LookPath("mc"); err != nil {
	}

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

func testS3ClientSmokeS3FS(t *testing.T, tgt s3Target) {
	if runtime.GOOS == "windows" {
	}
	if _, err := exec.LookPath("s3fs"); err != nil {
	}

	bucket := tgt.uniqueBucket(t, "s3fs")
	endpoint := tgt.endpoint(0)
	tmpDir := t.TempDir()
	passwdPath := filepath.Join(tmpDir, "passwd")
	require.NoError(t, os.WriteFile(passwdPath, []byte(tgt.accessKey+":"+tgt.secretKey), 0o600))
	mountDir := filepath.Join(tmpDir, "mnt")
	require.NoError(t, os.MkdirAll(mountDir, 0o700))

	args := []string{
		bucket,
		mountDir,
		"-o", "passwd_file=" + passwdPath,
		"-o", "url=" + endpoint,
		"-o", "use_path_request_style",
		"-o", "nomultipart",
	}
	if out, err := runClientCommandAllowError(t, nil, "s3fs", args...); err != nil {
		skipIfMissingFUSE(t, "s3fs", out, err)
		t.Fatalf("s3fs mount failed: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("umount", mountDir).Run()
	})

	exerciseMountedClient(t, mountDir, "s3fs smoke")
	requireObjectDeleted(t, tgt, bucket, "smoke.txt")
}

func testS3ClientSmokeGoofys(t *testing.T, tgt s3Target) {
	if runtime.GOOS == "windows" {
	}
	if _, err := exec.LookPath("goofys"); err != nil {
	}

	bucket := tgt.uniqueBucket(t, "goofys")
	endpoint := tgt.endpoint(0)
	mountDir := filepath.Join(t.TempDir(), "mnt")
	require.NoError(t, os.MkdirAll(mountDir, 0o700))
	env := []string{
		"AWS_ACCESS_KEY_ID=" + tgt.accessKey,
		"AWS_SECRET_ACCESS_KEY=" + tgt.secretKey,
		"AWS_REGION=us-east-1",
	}

	args := []string{
		"--endpoint", endpoint,
		"--region", "us-east-1",
		bucket,
		mountDir,
	}
	if out, err := runClientCommandAllowError(t, env, "goofys", args...); err != nil {
		skipIfMissingFUSE(t, "goofys", out, err)
		t.Fatalf("goofys mount failed: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("umount", mountDir).Run()
	})

	exerciseMountedClient(t, mountDir, "goofys smoke")
	requireObjectDeleted(t, tgt, bucket, "smoke.txt")
}

func exerciseMountedClient(t *testing.T, mountDir, content string) {
	t.Helper()
	path := filepath.Join(mountDir, "smoke.txt")
	require.Eventually(t, func() bool {
		return os.WriteFile(path, []byte(content), 0o600) == nil
	}, 10*time.Second, 200*time.Millisecond)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, string(got))
	entries, err := os.ReadDir(mountDir)
	require.NoError(t, err)
	require.True(t, hasDirEntry(entries, "smoke.txt"))
	require.NoError(t, os.Remove(path))
}

func hasDirEntry(entries []os.DirEntry, name string) bool {
	for _, entry := range entries {
		if entry.Name() == name {
			return true
		}
	}
	return false
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

func skipIfMissingFUSE(t *testing.T, client string, out []byte, err error) {
	t.Helper()
	msg := strings.ToLower(string(out) + " " + err.Error())
	for _, marker := range []string{
		"fuse",
		"operation not permitted",
		"permission denied",
		"device not configured",
		"no such file or directory",
	} {
		if strings.Contains(msg, marker) {
		}
	}
}
