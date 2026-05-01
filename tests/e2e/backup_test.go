package e2e

import (
	"context"
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
	"github.com/stretchr/testify/require"
)

func TestBackup_Restic_BackupAndRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backup test in short mode")
	}

	// Skip if restic not installed
	if _, err := exec.LookPath("restic"); err != nil {
		t.Skip("restic not installed - skipping backup test")
	}

	dir, err := os.MkdirTemp("", "grainfs-backup-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dataDir := filepath.Join(dir, "data")
	backupRepo := filepath.Join(dir, "backup")
	restoreDir := filepath.Join(dir, "restore")

	binary := getBinary()
	port := freePort()

	// Step 1: Start GrainFS and create test data
	t.Log("Step 1: Starting GrainFS and creating test data...")
	cmd := exec.Command(binary, "serve",
		"--data", dataDir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 10*time.Second)

	ctx := context.Background()
	client := newS3Client(endpoint)

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("backup-test"),
	})
	require.NoError(t, err)

	// Create test objects
	testData := map[string]string{
		"file1.txt":        "important data 1",
		"file2.txt":        "important data 2",
		"nested/file3.txt": "important data 3",
	}

	for key, content := range testData {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("backup-test"),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err, "put %s", key)
	}

	// Stop GrainFS to ensure data is flushed
	cmd.Process.Kill()
	cmd.Wait()
	time.Sleep(2 * time.Second)

	// Step 2: Initialize restic repository
	t.Log("Step 2: Initializing restic repository...")
	os.Setenv("RESTIC_REPOSITORY", backupRepo)
	os.Setenv("RESTIC_PASSWORD", "test-password")

	initCmd := exec.Command("restic", "init")
	initCmd.Stdout = os.Stdout
	initCmd.Stderr = os.Stderr
	require.NoError(t, initCmd.Run())

	// Step 3: Create backup using grainfs backup command
	t.Log("Step 3: Creating backup...")
	backupCmd := exec.Command(binary, "backup",
		"--repo", backupRepo,
		"--data", dataDir,
		"--tag", "e2e-test",
	)
	backupCmd.Stdout = os.Stdout
	backupCmd.Stderr = os.Stderr
	require.NoError(t, backupCmd.Run())

	// Step 4: Verify backup was created
	t.Log("Step 4: Verifying backup...")
	snapshotsCmd := exec.Command("restic", "snapshots", "--json")
	output, err := snapshotsCmd.Output()
	require.NoError(t, err, "list snapshots")
	require.Contains(t, string(output), "paths", "snapshot should contain paths")

	// Step 5: Corrupt original data (simulate disaster)
	t.Log("Step 5: Simulating data corruption...")
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0755)

	// Step 6: Restore from backup
	t.Log("Step 6: Restoring from backup...")
	restoreCmd := exec.Command(binary, "restore",
		"--repo", backupRepo,
		"--target", restoreDir,
	)
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr
	require.NoError(t, restoreCmd.Run())

	// Step 7: Verify restored data
	t.Log("Step 7: Verifying restored data...")

	// grainfs backup stores data under filepath.Base(dataDir), so after
	// `restic restore --target restoreDir` the data lands at:
	//   restoreDir/data   (not restoreDir itself)
	restoredDataDir := filepath.Join(restoreDir, filepath.Base(dataDir))

	// Start GrainFS with restored data
	restorePort := freePort()
	cmd2 := exec.Command(binary, "serve",
		"--data", restoredDataDir,
		"--port", fmt.Sprintf("%d", restorePort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
	)
	cmd2.Stdout = os.Stdout
	cmd2.Stderr = os.Stderr
	require.NoError(t, cmd2.Start())
	defer cmd2.Process.Kill()

	restoreEndpoint := fmt.Sprintf("http://127.0.0.1:%d", restorePort)
	waitForPort(t, restorePort, 10*time.Second)

	client2 := newS3Client(restoreEndpoint)

	// Verify all objects are present
	listOut, err := client2.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("backup-test"),
	})
	require.NoError(t, err, "list objects after restore")
	require.Len(t, listOut.Contents, len(testData), "all objects should be restored")

	// Verify object contents
	for key, expectedContent := range testData {
		resp, err := client2.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("backup-test"),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s after restore", key)
		defer resp.Body.Close()

		content, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "read %s after restore", key)
		require.Equal(t, expectedContent, string(content), "content of %s should match", key)
	}

	t.Log("✅ Backup and restore test passed - data integrity verified")
}
