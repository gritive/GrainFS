package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestBootstrapFirstSAWildcardGrantE2E (was F1): empty IAM → first sa
// create bootstraps an admin credential and persists the SA.
// Single-node only: bootstrap dispatch is single-process state.
func runIAMBootstrapFirstSAWildcardGrant(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapFirstSAWildcardGrantCases(t)
	})
}

func runBootstrapFirstSAWildcardGrantCases(t *testing.T) {
	t.Helper()
	dir, s3URL, _, _ := startUnbootstrappedE2EServer(t)
	sock := filepath.Join(dir, "admin.sock")

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	require.NotEmpty(t, bootstrap.AccessKey)
	require.NotEmpty(t, bootstrap.SecretKey)
	require.NotEmpty(t, bootstrap.SAID)

	var saList []map[string]any
	iamDo(t, sock, "GET", "/v1/iam/sa", nil, &saList)
	var saIDs []string
	for _, sa := range saList {
		if id, ok := sa["sa_id"].(string); ok {
			saIDs = append(saIDs, id)
		}
	}
	require.Contains(t, saIDs, bootstrap.SAID, "first SA must be persisted; got %v", saIDs)

	cli := s3ClientFor(s3URL, bootstrap.AccessKey, bootstrap.SecretKey)
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))
}

// TestBootstrapSecondSANoAutoGrantE2E (was F2): non-empty store → SA
// create does NOT auto-issue a wildcard grant. Single-node only.
func runIAMBootstrapSecondSANoAutoGrant(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapSecondSANoAutoGrantCases(t)
	})
}

func runBootstrapSecondSANoAutoGrantCases(t *testing.T) {
	t.Helper()
	dir, s3URL, _, _ := startUnbootstrappedE2EServer(t)
	sock := filepath.Join(dir, "admin.sock")

	// First SA → bootstrap.
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)

	// Second SA → must NOT receive auto wildcard grant.
	var out struct {
		SAID      string           `json:"sa_id"`
		Name      string           `json:"name"`
		AccessKey string           `json:"access_key"`
		SecretKey string           `json:"secret_key"`
		Grants    []map[string]any `json:"grants"`
	}
	iamDo(t, sock, "POST", "/v1/iam/sa", map[string]string{"name": "user1"}, &out)
	require.NotEqual(t, bootstrap.SAID, out.SAID, "second SA must NOT reuse bootstrap SAID")
	require.Empty(t, out.Grants, "second SA must have no auto-issued grants; got %+v", out.Grants)

	bootstrapClient := s3ClientFor(s3URL, bootstrap.AccessKey, bootstrap.SecretKey)
	const bucket = "bootstrap-owned"
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, bootstrap.SAID, bucket, bootstrapClient)

	userClient := s3ClientFor(s3URL, out.AccessKey, out.SecretKey)
	iamWaitKeyReady(t, s3URL, out.AccessKey, out.SecretKey, 10*time.Second)
	_, err := userClient.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	require.Error(t, err, "second SA without explicit grant must not access bootstrap-owned bucket")
}

// TestBootstrapPreBootstrapDeniedE2E (was F3): pre-bootstrap sigv4 traffic
// → AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch class error.
// Single-node only: pre-bootstrap state.
func runIAMBootstrapPreBootstrapDenied(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapPreBootstrapDeniedCases(t)
	})
}

func runBootstrapPreBootstrapDeniedCases(t *testing.T) {
	t.Helper()
	_, s3URL, _, _ := startUnbootstrappedE2EServer(t)

	cli := s3ClientFor(s3URL, "AKIA-fake-bootstrap-test", "fake-secret-bootstrap-test")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.Error(t, err, "ListBuckets with fabricated key must fail before bootstrap")

	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr), "expected smithy APIError, got %T: %v", err, err)
	// Acceptable codes from the S3 auth path. Project sigv4 surface uses
	// AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch.
	code := apiErr.ErrorCode()
	require.Contains(t,
		[]string{"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"},
		code,
		"unexpected error code %q from pre-bootstrap sigv4: %v", code, err,
	)
}

// TestBootstrapPostBootstrapVerbsE2E (was F4): post-bootstrap, the bootstrap
// creds drive ListBuckets, CreateBucket, PutObject, and GetObject end-to-end.
// Single-node only: tests bootstrap completion against a fresh single binary.
func runIAMBootstrapPostBootstrapVerbs(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runBootstrapPostBootstrapVerbsCases(t)
	})
}

func runBootstrapPostBootstrapVerbsCases(t *testing.T) {
	t.Helper()
	dir, s3URL, _, _ := startUnbootstrappedE2EServer(t)

	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	cli := s3ClientFor(s3URL, bootstrap.AccessKey, bootstrap.SecretKey)
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ListBuckets — admin SA must succeed (the bucket list itself may
	// contain auto-created defaults; the assertion is just that auth
	// passes and the verb returns 200).
	_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err, "ListBuckets")

	// Create bucket through the admin control plane.
	bucket := "f4-bootstrap-bucket"
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, bootstrap.SAID, bucket, cli)

	// PutObject.
	const payload = "hello-bootstrap-f4"
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj1"),
		Body:   strings.NewReader(payload),
	})
	require.NoError(t, err, "PutObject")

	// GetObject — body must round-trip.
	getOut, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj1"),
	})
	require.NoError(t, err, "GetObject")
	defer getOut.Body.Close()
	body, err := io.ReadAll(getOut.Body)
	require.NoError(t, err, "read GetObject body")
	require.Equal(t, payload, string(body), "object body round-trip mismatch")
}

// startUnbootstrappedE2EServer spawns a single-node grainfs binary like
// startIsolatedE2EServer but skips the admin SA bootstrap. F1/F2/F3/F4
// each drive the bootstrap themselves (or omit it, for F3).
func startUnbootstrappedE2EServer(t testing.TB) (dataDir, s3URL, adminSock string, port int) {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-e2e-bootstrap-*")
	require.NoError(t, err, "mkdtemp")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	port = freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start unbootstrapped e2e server")
	t.Cleanup(func() { terminateProcess(cmd) })

	s3URL = fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	adminSock = filepath.Join(dir, "admin.sock")

	// Wait for admin.sock then disable auto-snapshot for deterministic e2e
	// behavior. Tests that need the auto-snapshot loop PATCH it back to a
	// non-zero interval explicitly. PATCH /v1/cluster/config does not require
	// IAM bootstrap, so it works even though this helper intentionally skips
	// admin SA creation.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(adminSock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admin socket %s did not appear within 10s", adminSock)
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, retryPatchSnapshotIntervalM(dir, "0s", 10*time.Second), "disable auto-snapshot")
	return dir, s3URL, adminSock, port
}

func retryPatchSnapshotIntervalM(dataDir, dur string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := patchSnapshotIntervalM(dataDir, dur); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIAMBootstrapE2E groups IAM bootstrap SA grant scenarios.
func TestIAMBootstrapE2E(t *testing.T) {
	t.Run("FirstSAWildcardGrant", runIAMBootstrapFirstSAWildcardGrant)
	t.Run("SecondSANoAutoGrant", runIAMBootstrapSecondSANoAutoGrant)
	t.Run("PreBootstrapDenied", runIAMBootstrapPreBootstrapDenied)
	t.Run("PostBootstrapVerbs", runIAMBootstrapPostBootstrapVerbs)
}
