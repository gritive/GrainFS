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

func TestS3ClientSmoke(t *testing.T) {
	t.Run("MinIOMC", testS3ClientSmokeMinIOMC)
	t.Run("S3FS", testS3ClientSmokeS3FS)
	t.Run("Goofys", testS3ClientSmokeGoofys)
}

func testS3ClientSmokeMinIOMC(t *testing.T) {
	if _, err := exec.LookPath("mc"); err != nil {
		t.Skip("mc binary not found; install MinIO Client to run TestS3ClientSmoke/MinIOMC")
	}

	bucket := "client-smoke-mc"
	createBucket(t, bucket)
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "mc-config")
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	srcPath := filepath.Join(tmpDir, "src.txt")
	require.NoError(t, os.WriteFile(srcPath, []byte("mc smoke"), 0o600))

	runClientCommand(t, nil, "mc", "--config-dir", configDir, "alias", "set", "grainfs", testServerURL, testAccessKey, testSecretKey, "--api", "S3v4", "--path", "on")
	runClientCommand(t, nil, "mc", "--config-dir", configDir, "cp", srcPath, "grainfs/"+bucket+"/smoke.txt")
	out := runClientCommand(t, nil, "mc", "--config-dir", configDir, "cat", "grainfs/"+bucket+"/smoke.txt")
	require.Equal(t, "mc smoke", string(out))
	out = runClientCommand(t, nil, "mc", "--config-dir", configDir, "ls", "grainfs/"+bucket)
	require.Contains(t, string(out), "smoke.txt")
	runClientCommand(t, nil, "mc", "--config-dir", configDir, "rm", "grainfs/"+bucket+"/smoke.txt")
	requireObjectDeleted(t, bucket, "smoke.txt")
}

func testS3ClientSmokeS3FS(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("s3fs smoke requires a Unix-like FUSE environment")
	}
	if _, err := exec.LookPath("s3fs"); err != nil {
		t.Skip("s3fs binary not found; install s3fs to run TestS3ClientSmoke/S3FS")
	}

	bucket := "client-smoke-s3fs"
	createBucket(t, bucket)
	tmpDir := t.TempDir()
	passwdPath := filepath.Join(tmpDir, "passwd")
	require.NoError(t, os.WriteFile(passwdPath, []byte(testAccessKey+":"+testSecretKey), 0o600))
	mountDir := filepath.Join(tmpDir, "mnt")
	require.NoError(t, os.MkdirAll(mountDir, 0o700))

	args := []string{
		bucket,
		mountDir,
		"-o", "passwd_file=" + passwdPath,
		"-o", "url=" + testServerURL,
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
	requireObjectDeleted(t, bucket, "smoke.txt")
}

func testS3ClientSmokeGoofys(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("goofys smoke requires a Unix-like FUSE environment")
	}
	if _, err := exec.LookPath("goofys"); err != nil {
		t.Skip("goofys binary not found; install goofys to run TestS3ClientSmoke/Goofys")
	}

	bucket := "client-smoke-goofys"
	createBucket(t, bucket)
	mountDir := filepath.Join(t.TempDir(), "mnt")
	require.NoError(t, os.MkdirAll(mountDir, 0o700))
	env := []string{
		"AWS_ACCESS_KEY_ID=" + testAccessKey,
		"AWS_SECRET_ACCESS_KEY=" + testSecretKey,
		"AWS_REGION=us-east-1",
	}

	args := []string{
		"--endpoint", testServerURL,
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
	requireObjectDeleted(t, bucket, "smoke.txt")
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

func requireObjectDeleted(t *testing.T, bucket, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		_, err := testS3Client.HeadObject(ctx, &s3.HeadObjectInput{
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
			t.Skipf("%s mount capability unavailable: %v\n%s", client, err, out)
		}
	}
}
