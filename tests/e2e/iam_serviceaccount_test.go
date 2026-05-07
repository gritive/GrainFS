package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// httpStatusFrom extracts the HTTP status code from an aws-sdk-go-v2 error,
// returning 0 if the error chain has no transport response.
func httpStatusFrom(err error) int {
	var rerr *smithyhttp.ResponseError
	if errors.As(err, &rerr) && rerr.Response != nil {
		return rerr.Response.StatusCode
	}
	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		// fallback: many auth errors surface only the API code, not status.
		switch aerr.ErrorCode() {
		case "AccessDenied":
			return http.StatusForbidden
		case "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return http.StatusForbidden
		}
	}
	return 0
}

// TestIAM_E2E_ET1_RevokedKey_Returns401 — alice creates an SA + bucket-scoped
// grant + bucket, PUTs an object, then admin revokes alice's only key. The
// next S3 operation must be denied (401/403) — proving the verifier no
// longer accepts the revoked credential.
func TestIAM_E2E_ET1_RevokedKey_Returns401(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	alice := iamCreateSA(t, srv.AdminSock, "alice-et1-revoke")
	iamWaitKeyReady(t, srv.S3URL, alice.AccessKey, alice.SecretKey, 10*time.Second)
	const bucket = "alice-et1-revoke-bkt"
	iamGrantPut(t, srv.AdminSock, alice.SAID, bucket, "Admin")

	cli := s3ClientFor(srv.S3URL, alice.AccessKey, alice.SecretKey)
	ctx := context.Background()
	if _, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("alice CreateBucket: %v", err)
	}
	if _, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"),
		Body: strings.NewReader("hello"),
	}); err != nil {
		t.Fatalf("alice PutObject: %v", err)
	}

	// Revoke alice's only access key.
	iamKeyRevoke(t, srv.AdminSock, alice.SAID, alice.AccessKey)

	// Subsequent GET must fail with auth-class status.
	_, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"),
	})
	if err == nil {
		t.Fatalf("GET after revoke succeeded; expected auth failure")
	}
	status := httpStatusFrom(err)
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		t.Fatalf("GET after revoke: status=%d err=%v; want 401/403", status, err)
	}
}

// TestIAM_E2E_ET1_ExpiredKey_Returns401 — rotate a short-TTL key for the
// SA, sleep past expiry, request must fail.
func TestIAM_E2E_ET1_ExpiredKey_Returns401(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	bob := iamCreateSA(t, srv.AdminSock, "bob-et1-expire")
	iamWaitKeyReady(t, srv.S3URL, bob.AccessKey, bob.SecretKey, 10*time.Second)
	const bucket = "bob-et1-expire-bkt"
	iamGrantPut(t, srv.AdminSock, bob.SAID, bucket, "Admin")

	// Use the original key to provision the bucket so the test exercises a
	// known-good baseline before introducing expiry.
	primary := s3ClientFor(srv.S3URL, bob.AccessKey, bob.SecretKey)
	if _, err := primary.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("bob CreateBucket: %v", err)
	}

	// Mint an expiring key (3s TTL — generous enough for slow CI).
	exp := iamKeyCreateExpiringIn(t, srv.AdminSock, bob.SAID, 3*time.Second)
	iamWaitKeyReady(t, srv.S3URL, exp.AccessKey, exp.SecretKey, 5*time.Second)
	expCli := s3ClientFor(srv.S3URL, exp.AccessKey, exp.SecretKey)

	// Confirm the expiring key works while still valid.
	if _, err := expCli.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("expiring key pre-expiry HeadBucket: %v", err)
	}

	// Wait past expiry.
	time.Sleep(4 * time.Second)

	_, err := expCli.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		t.Fatalf("HEAD after expiry succeeded; expected auth failure")
	}
	status := httpStatusFrom(err)
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		t.Fatalf("HEAD after expiry: status=%d err=%v; want 401/403", status, err)
	}
}

// TestIAM_E2E_ET2_RoleOpMatrix — for each (role, op) combination, build a
// dedicated SA with that role on a per-case bucket, then verify the op is
// allowed or denied as expected.
func TestIAM_E2E_ET2_RoleOpMatrix(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	// Bootstrap-creds client provisions the working bucket so Read/Write SAs
	// can target it. Admin and CreateBucket cases handle their own buckets.
	bootCli := s3ClientFor(srv.S3URL, srv.BootstrapAK, srv.BootstrapSK)
	const sharedBucket = "et2-shared"
	if _, err := bootCli.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(sharedBucket),
	}); err != nil {
		t.Fatalf("bootstrap CreateBucket %s: %v", sharedBucket, err)
	}
	// Seed an object so Get is meaningful for Read/Write roles.
	if _, err := bootCli.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(sharedBucket), Key: aws.String("seed"),
		Body: strings.NewReader("seed"),
	}); err != nil {
		t.Fatalf("bootstrap seed Put: %v", err)
	}

	cases := []struct {
		role  string
		op    string
		allow bool
	}{
		{"Read", "Get", true},
		{"Read", "Put", false},
		{"Read", "Delete", false},
		{"Write", "Get", true},
		{"Write", "Put", true},
		{"Write", "Delete", true},
		{"Write", "CreateBucket", false},
		{"Admin", "CreateBucket", true},
	}

	for i, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("%s_%s", tc.role, tc.op), func(t *testing.T) {
			sa := iamCreateSA(t, srv.AdminSock, fmt.Sprintf("et2-%s-%s-%d", tc.role, tc.op, i))
			iamWaitKeyReady(t, srv.S3URL, sa.AccessKey, sa.SecretKey, 10*time.Second)
			cli := s3ClientFor(srv.S3URL, sa.AccessKey, sa.SecretKey)
			ctx := context.Background()

			var (
				targetBucket = sharedBucket
				targetKey    = fmt.Sprintf("k-%s-%s-%d", tc.role, tc.op, i)
				err          error
			)

			switch tc.op {
			case "CreateBucket":
				// Each CreateBucket case uses its own fresh bucket so
				// concurrent runs don't collide.
				targetBucket = fmt.Sprintf("et2-cb-%d", i)
				// CreateBucket grant must target the bucket itself; the
				// authorize check runs against (sa, bucket).
				iamGrantPut(t, srv.AdminSock, sa.SAID, targetBucket, tc.role)
				_, err = cli.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(targetBucket),
				})
			case "Get":
				iamGrantPut(t, srv.AdminSock, sa.SAID, targetBucket, tc.role)
				_, err = cli.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(targetBucket), Key: aws.String("seed"),
				})
			case "Put":
				iamGrantPut(t, srv.AdminSock, sa.SAID, targetBucket, tc.role)
				_, err = cli.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(targetBucket), Key: aws.String(targetKey),
					Body: strings.NewReader("v"),
				})
			case "Delete":
				iamGrantPut(t, srv.AdminSock, sa.SAID, targetBucket, tc.role)
				// Pre-create the object as bootstrap so Delete has a target.
				if _, e := bootCli.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(targetBucket), Key: aws.String(targetKey),
					Body: strings.NewReader("v"),
				}); e != nil {
					t.Fatalf("bootstrap pre-Put: %v", e)
				}
				_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(targetBucket), Key: aws.String(targetKey),
				})
			default:
				t.Fatalf("unhandled op %q", tc.op)
			}

			if tc.allow {
				if err != nil {
					t.Fatalf("%s %s: expected allow but got err: %v", tc.role, tc.op, err)
				}
				return
			}
			// expected deny
			if err == nil {
				t.Fatalf("%s %s: expected deny but request succeeded", tc.role, tc.op)
			}
			status := httpStatusFrom(err)
			if status != http.StatusForbidden && status != http.StatusUnauthorized {
				t.Fatalf("%s %s: expected 401/403, got status=%d err=%v",
					tc.role, tc.op, status, err)
			}
		})
	}
}

// TestIAM_E2E_ET3_PresignedURL_RevokedKey_401 — alice presigns a GET, admin
// revokes alice's key, the presigned URL must no longer work.
func TestIAM_E2E_ET3_PresignedURL_RevokedKey_401(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	alice := iamCreateSA(t, srv.AdminSock, "alice-et3-presign")
	iamWaitKeyReady(t, srv.S3URL, alice.AccessKey, alice.SecretKey, 10*time.Second)
	const bucket = "alice-et3-presign-bkt"
	const key = "secret.txt"
	iamGrantPut(t, srv.AdminSock, alice.SAID, bucket, "Admin")

	cli := s3ClientFor(srv.S3URL, alice.AccessKey, alice.SecretKey)
	ctx := context.Background()
	if _, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("alice CreateBucket: %v", err)
	}
	if _, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: strings.NewReader("classified"),
	}); err != nil {
		t.Fatalf("alice PutObject: %v", err)
	}

	// Presign GET with 5min expiry. SigV4 baked in — the URL itself carries
	// the access key id, so revoking the key must invalidate the URL.
	presigned, err := s3auth.PresignURL(http.MethodGet,
		srv.S3URL+"/"+bucket+"/"+key,
		alice.AccessKey, alice.SecretKey, "us-east-1", 300)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}

	// Sanity check: presigned URL works while key is active.
	resp, err := http.Get(presigned)
	if err != nil {
		t.Fatalf("presigned GET (pre-revoke): %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "classified" {
		t.Fatalf("presigned GET (pre-revoke): status=%d body=%q", resp.StatusCode, body)
	}

	// Revoke alice's only key.
	iamKeyRevoke(t, srv.AdminSock, alice.SAID, alice.AccessKey)

	// The presigned URL must now be rejected.
	resp2, err := http.Get(presigned)
	if err != nil {
		t.Fatalf("presigned GET (post-revoke): %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized && resp2.StatusCode != http.StatusForbidden {
		got, _ := io.ReadAll(resp2.Body)
		t.Fatalf("presigned GET after revoke: status=%d body=%q; want 401/403",
			resp2.StatusCode, string(got))
	}
}

// TestIAM_E2E_SC8_NoPlaintextSecretOnDisk asserts the at-rest invariant:
// after creating an SA and forcing a healthy spread of FSM apply ticks,
// the SA's plaintext secret_key must not appear anywhere under the data
// directory. The plaintext lives only in-memory; persisted form is
// AES-256-GCM ciphertext.
func TestIAM_E2E_SC8_NoPlaintextSecretOnDisk(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	alice := iamCreateSA(t, srv.AdminSock, "alice-sc8")
	iamWaitKeyReady(t, srv.S3URL, alice.AccessKey, alice.SecretKey, 10*time.Second)

	// Drive enough mixed I/O to flush badger LSM and ensure the IAM FSM
	// has applied through several tick cycles. No public "force snapshot"
	// API exists; quantity replaces explicit flush.
	const bucket = "bucket-sc8"
	iamGrantPut(t, srv.AdminSock, alice.SAID, bucket, "Admin")

	cli := s3ClientFor(srv.S3URL, alice.AccessKey, alice.SecretKey)
	ctx := context.Background()
	if _, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("alice CreateBucket: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("k-%d", i)),
			Body:   strings.NewReader(fmt.Sprintf("payload-%d", i)),
		}); err != nil {
			t.Fatalf("alice Put %d: %v", i, err)
		}
	}

	// Generate enough Raft writes to force several apply/commit cycles.
	for i := 0; i < 30; i++ {
		iamCreateSA(t, srv.AdminSock, fmt.Sprintf("filler-%d", i))
	}

	hits := grepDataDir(t, srv.DataDir, alice.SecretKey)
	if len(hits) > 0 {
		t.Fatalf("alice.SecretKey appears in data dir: %v", hits)
	}
}

// TestIAM_E2E_ET5_StickyAuth_ClusterRestart asserts the auth_enabled sticky
// bit and the bootstrap credentials survive a full process restart against
// the same data dir. The second start passes NO --access-key/--secret-key
// flags, so the IAM state must rehydrate from the persisted snapshot/raft
// log: encryption.key on disk decrypts SecretKeyEnc, the SigV4 verifier
// accepts the original bootstrap key pair, and anonymous traffic is still
// rejected.
func TestIAM_E2E_ET5_StickyAuth_ClusterRestart(t *testing.T) {
	h := startIAMTestServerWithRestart(t)

	// First boot: bootstrap creds must already work; sticky bit is on.
	cli := h.Client()
	ctx := context.Background()
	_, err := cli.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("__et5_probe1__"),
	})
	if err != nil {
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) ||
			(apiErr.ErrorCode() != "NotFound" && apiErr.ErrorCode() != "NoSuchBucket") {
			t.Fatalf("bootstrap creds rejected on first boot: %v", err)
		}
	}

	h.Stop(t)

	// Restart with NO --access-key flag: the bootstrap shim is a no-op on a
	// non-empty store, so the durable IAM state alone must keep auth on and
	// the original bootstrap creds valid.
	h.Start(t)

	cli2 := h.Client()
	_, err = cli2.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("__et5_probe2__"),
	})
	if err != nil {
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) ||
			(apiErr.ErrorCode() != "NotFound" && apiErr.ErrorCode() != "NoSuchBucket") {
			t.Fatalf("bootstrap creds rejected after restart: %v", err)
		}
	}

	// Anonymous / bogus creds must still be rejected — sticky bit on.
	bogus := s3ClientFor(h.S3URL, "AKBOGUSAKBOGUSAK", "skBogusSecretKeyForTestThirty")
	_, err = bogus.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err == nil {
		t.Fatal("bogus creds accepted after restart — sticky bit not enforced")
	}
}

// TestIAM_E2E_ET6_WildcardRemovalPreservesDefaultSA verifies the *first
// half* of ET6: the P5 auto-grant hook (commit 6b02af2) issues an explicit
// (sa-default, bucket, Admin) grant on every CreateBucket, in addition to
// the bootstrap wildcard. The explicit grant is the durable per-bucket
// claim that survives any future wildcard cleanup.
//
// The second half — actually removing the wildcard via admin API and
// observing that only the explicit grant remains — is NOT testable in
// Phase 5: there is no admin route to remove a wildcard grant.
//
//	HandleGrantDelete → ProposeGrantDelete → applyGrantDelete only touches
//	the per-bucket `grants` map; the bootstrap default SA's wildcard lives
//	in the separate `wildcards` map and is unreachable from the admin API.
//	HandleGrantDelete with bucket="*" is a silent no-op today.
//
// Tracked as a Phase 5c gap (see design doc Status block). Fix sketch:
// add ProposeGrantWildcardDelete + route HandleGrantDelete with
// bucket=="*" to it.
func TestIAM_E2E_ET6_WildcardRemovalPreservesDefaultSA(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	defCli := s3ClientFor(srv.S3URL, srv.BootstrapAK, srv.BootstrapSK)
	const bucket = "et6-bucket"
	ctx := context.Background()
	if _, err := defCli.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("default CreateBucket: %v", err)
	}

	// Both grants must coexist: explicit (sa-default, et6-bucket, Admin)
	// auto-issued by the P5 hook AND the wildcard from bootstrap.
	grants := iamListGrants(t, srv.AdminSock, "sa-default", "")
	var foundExplicit, foundWildcard bool
	for _, g := range grants {
		if g.Bucket == bucket && g.Role == "Admin" {
			foundExplicit = true
		}
		if g.Bucket == "*" && g.Role == "Admin" {
			foundWildcard = true
		}
	}
	if !foundExplicit {
		t.Fatalf("expected explicit (sa-default, %s, Admin) grant; got %+v", bucket, grants)
	}
	if !foundWildcard {
		t.Fatalf("expected wildcard grant; got %+v", grants)
	}

	// Default SA can write into the owned bucket — exercises the (auto)
	// explicit grant path. (With wildcard still present this is also
	// allowed via wildcard; once the Phase 5c API gap is closed and the
	// wildcard is removable, this assertion proves the explicit grant
	// holds independently. For now it's a baseline regression guard.)
	if _, err := defCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("v")),
	}); err != nil {
		t.Fatalf("PutObject on owned bucket: %v", err)
	}
}

// grepDataDir scans every regular file under root for needle. Returns the
// matching paths (one per file). Sequential scan is fine for e2e: the
// data dir is small (test scope, single-node).
func grepDataDir(t *testing.T, root, needle string) []string {
	t.Helper()
	var hits []string
	needleBytes := []byte(needle)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil // unreadable (e.g. socket) — skip
		}
		if bytes.Contains(b, needleBytes) {
			hits = append(hits, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return hits
}
