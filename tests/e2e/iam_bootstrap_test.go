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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// F1: empty IAM → first sa create returns wildcard grant.
//
// Verifies the bootstrap path uses the new InitFirstSA dispatch:
//   - The SA id is the well-known sa-default.
//   - A wildcard grant (bucket="*", role="admin") is created for that SA.
func TestE2E_Bootstrap_F1_FirstSACreateReturnsWildcardGrant(t *testing.T) {
	dir, _, _, _ := startUnbootstrappedE2EServer(t)
	sock := filepath.Join(dir, "admin.sock")

	ak, sk := bootstrapAdminViaUDS(t, dir)
	require.NotEmpty(t, ak)
	require.NotEmpty(t, sk)

	// SA must use the fixed DefaultSAID ("sa-default").
	var saList []map[string]any
	iamDo(t, sock, "GET", "/v1/iam/sa", nil, &saList)
	var saIDs []string
	for _, sa := range saList {
		if id, ok := sa["sa_id"].(string); ok {
			saIDs = append(saIDs, id)
		}
	}
	require.Contains(t, saIDs, "sa-default", "first SA must use DefaultSAID; got %v", saIDs)

	// Wildcard grant must be present for sa-default.
	grants := iamListGrants(t, sock, "sa-default", "")
	found := false
	for _, g := range grants {
		if g.Bucket == "*" && strings.EqualFold(g.Role, "admin") {
			found = true
			break
		}
	}
	require.True(t, found, "first SA must have wildcard admin grant; got %+v", grants)
}

// F2: non-empty store → SA create does NOT auto-issue a wildcard grant.
//
// The dispatch in HandleSACreate uses the InitFirstSA composite only when
// store.IsEmpty(); a follow-up create takes the regular SA + Key path.
func TestE2E_Bootstrap_F2_SecondSACreate_NoAutoGrant(t *testing.T) {
	dir, _, _, _ := startUnbootstrappedE2EServer(t)
	sock := filepath.Join(dir, "admin.sock")

	// First SA → bootstrap.
	bootstrapAdminViaUDS(t, dir)

	// Second SA → must NOT receive auto wildcard grant.
	var out struct {
		SAID      string           `json:"sa_id"`
		Name      string           `json:"name"`
		AccessKey string           `json:"access_key"`
		SecretKey string           `json:"secret_key"`
		Grants    []map[string]any `json:"grants"`
	}
	iamDo(t, sock, "POST", "/v1/iam/sa", map[string]string{"name": "user1"}, &out)
	require.NotEqual(t, "sa-default", out.SAID, "second SA must NOT reuse DefaultSAID")
	require.Empty(t, out.Grants, "second SA must have no auto-issued grants; got %+v", out.Grants)

	// Defence in depth: confirm no grants persisted for the second SA.
	grants := iamListGrants(t, sock, out.SAID, "")
	require.Empty(t, grants, "second SA must have no persisted grants; got %+v", grants)
}

// F3: pre-bootstrap sigv4 traffic → authentication failure.
//
// With no SA in the store, every access_key is unknown and the verifier
// must reject. We don't pin a specific status code (project comments
// suggest 401 InvalidAccessKeyId) — only that auth fails with an
// AccessDenied / InvalidAccessKeyId class error in the 4xx range.
func TestE2E_Bootstrap_F3_BeforeBootstrap_S3Returns401(t *testing.T) {
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

// F4: post-bootstrap, the bootstrap creds successfully drive ListBuckets,
// CreateBucket, PutObject, and GetObject end-to-end.
func TestE2E_Bootstrap_F4_PostBootstrap_ThreeVerbs(t *testing.T) {
	dir, s3URL, _, _ := startUnbootstrappedE2EServer(t)

	ak, sk := bootstrapAdminViaUDS(t, dir)
	cli := s3ClientFor(s3URL, ak, sk)
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ListBuckets — admin SA must succeed (the bucket list itself may
	// contain auto-created defaults; the assertion is just that auth
	// passes and the verb returns 200).
	_, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err, "ListBuckets")

	// CreateBucket.
	bucket := "f4-bootstrap-bucket"
	_, err = cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")

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
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start unbootstrapped e2e server")
	t.Cleanup(func() { terminateProcess(cmd) })

	s3URL = fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	adminSock = filepath.Join(dir, "admin.sock")
	return dir, s3URL, adminSock, port
}
