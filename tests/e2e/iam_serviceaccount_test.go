package e2e

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/stretchr/testify/require"

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
// SA, wait until expiry is observed, request must fail.
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

	exp := iamKeyCreateExpiringIn(t, srv.AdminSock, bob.SAID, time.Second)
	iamWaitKeyReady(t, srv.S3URL, exp.AccessKey, exp.SecretKey, 5*time.Second)
	expCli := s3ClientFor(srv.S3URL, exp.AccessKey, exp.SecretKey)

	// Confirm the expiring key works while still valid.
	if _, err := expCli.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("expiring key pre-expiry HeadBucket: %v", err)
	}

	var err error
	require.Eventually(t, func() bool {
		_, err = expCli.HeadBucket(context.Background(), &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		return err != nil
	}, 3*time.Second, 50*time.Millisecond, "expiring key should stop authenticating after expires_at")

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

// TestIAM_E2E_SC8_NoPlaintextSecretOnDisk asserts the at-rest invariant on
// the IAM control-plane persistence path. IAM secrets are proposed through
// meta-raft; object data groups and dedup stores never persist IAM key
// material and are covered by separate data-plane tests.
func TestIAM_E2E_SC8_NoPlaintextSecretOnDisk(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	alice := iamCreateSA(t, srv.AdminSock, "alice-sc8")
	iamWaitKeyReady(t, srv.S3URL, alice.AccessKey, alice.SecretKey, 10*time.Second)

	hits := grepIAMControlPlaneDataDir(t, srv.DataDir, alice.SecretKey)
	if len(hits) > 0 {
		t.Fatalf("alice.SecretKey appears in IAM control-plane persistence: %v", hits)
	}
}

// TestIAM_E2E_ET6_WildcardRemovalPreservesDefaultSA exercises the full ET6
// flow now that the Phase-5c admin gap is closed (HandleGrantDelete routes
// bucket="*" to ProposeGrantWildcardDelete, gated by a footgun guard that
// requires at least one explicit grant on sa-default).
//
//  1. CreateBucket as default SA → P5 hook auto-issues the explicit
//     (sa-default, et6-bucket, Admin) grant alongside the bootstrap wildcard.
//  2. DELETE /v1/iam/grant {sa: sa-default, bucket: "*"} succeeds (guard
//     passes because the explicit grant exists).
//  3. After commit propagates, the wildcard is gone but the explicit grant
//     keeps default SA functional on its owned bucket.
//  4. Default SA is denied on a bucket owned by a different SA, proving
//     wildcard-bypass authorization no longer applies.
func TestIAM_E2E_ET6_WildcardRemovalPreservesDefaultSA(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	defCli := s3ClientFor(srv.S3URL, srv.BootstrapAK, srv.BootstrapSK)
	ctx := context.Background()
	const bucket = "et6-bucket"
	if _, err := defCli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Wait for the explicit grant to land in IAM (P5 auto-issues it).
	deadline := time.Now().Add(5 * time.Second)
	haveExplicit := false
	for time.Now().Before(deadline) {
		grants := iamListGrants(t, srv.AdminSock, "sa-default", "")
		for _, g := range grants {
			if g.Bucket == bucket && g.Role == "Admin" {
				haveExplicit = true
				break
			}
		}
		if haveExplicit {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !haveExplicit {
		t.Fatal("explicit (sa-default, et6-bucket, Admin) grant did not land within 5s")
	}

	// Remove the wildcard grant. Must succeed: explicit grant on et6-bucket exists.
	iamGrantDelete(t, srv.AdminSock, "sa-default", "*")

	// Wait for the wildcard removal to commit + propagate.
	deadline = time.Now().Add(5 * time.Second)
	wildcardGone := false
	for time.Now().Before(deadline) {
		grants := iamListGrants(t, srv.AdminSock, "sa-default", "")
		hasWildcard := false
		for _, g := range grants {
			if g.Bucket == "*" {
				hasWildcard = true
				break
			}
		}
		if !hasWildcard {
			wildcardGone = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !wildcardGone {
		t.Fatal("wildcard grant still present 5s after delete")
	}

	// Default SA still works on owned bucket via explicit grant.
	if _, err := defCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("after-wildcard-removal"),
		Body:   bytes.NewReader([]byte("v")),
	}); err != nil {
		t.Fatalf("PutObject after wildcard removal: %v", err)
	}

	// Default SA is denied on a bucket where it has no explicit grant.
	// Use a separate SA to create+own the other-bucket so default has no grant on it.
	other := iamCreateSA(t, srv.AdminSock, "other-sa")
	iamGrantPut(t, srv.AdminSock, other.SAID, "other-bucket", "Admin")
	iamWaitKeyReady(t, srv.S3URL, other.AccessKey, other.SecretKey, 10*time.Second)
	otherCli := s3ClientFor(srv.S3URL, other.AccessKey, other.SecretKey)
	if _, err := otherCli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("other-bucket")}); err != nil {
		t.Fatalf("other CreateBucket: %v", err)
	}

	if _, err := defCli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("other-bucket"),
		Key:    aws.String("k"),
	}); err == nil {
		t.Fatal("default SA still has access to other-sa's bucket after wildcard removal; expected 403")
	}
}

// iamKeyCreateScoped issues a new scoped key for saID restricted to buckets.
// Mirrors iamKeyCreateExpiringIn; returns the decoded KeyCreateResponse.
func iamKeyCreateScoped(t *testing.T, sock, saID string, buckets []string) iamKeyResult {
	t.Helper()
	var out iamKeyResult
	iamDo(t, sock, "POST", "/v1/iam/sa/"+saID+"/key",
		map[string]any{"buckets": buckets}, &out)
	return out
}

// iamAdminRaw issues a raw admin UDS request and returns (statusCode, body).
// Unlike iamDo it does NOT fatal on 4xx so callers can assert error cases.
func iamAdminRaw(t *testing.T, sock, method, path string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		require.NoError(t, err, "iamAdminRaw: marshal")
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, "http://unix"+path, rdr)
	require.NoError(t, err, "iamAdminRaw: build request")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := iamUDSClient(sock).Do(req)
	require.NoErrorf(t, err, "iamAdminRaw: %s %s", method, path)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody
}

// TestE2E_IAM_ScopedKey_RightBucket_OK — scoped key for "logs" bucket grants
// access to objects inside "logs".
func TestE2E_IAM_ScopedKey_RightBucket_OK(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	ctx := context.Background()

	// Bootstrap client provisions the bucket and seeds an object.
	bootCli := s3ClientFor(srv.S3URL, srv.BootstrapAK, srv.BootstrapSK)
	const bucket = "st1-logs"
	_, err := bootCli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket logs")
	_, err = bootCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("obj1"),
		Body: strings.NewReader("hello"),
	})
	require.NoError(t, err, "PutObject")

	// Create SA alice with Read grant on "logs".
	alice := iamCreateSA(t, srv.AdminSock, "alice-st1")
	iamGrantPut(t, srv.AdminSock, alice.SAID, bucket, "Read")

	// Issue scoped key restricted to "logs".
	scoped := iamKeyCreateScoped(t, srv.AdminSock, alice.SAID, []string{bucket})
	iamWaitKeyReady(t, srv.S3URL, scoped.AccessKey, scoped.SecretKey, 10*time.Second)

	cli := s3ClientFor(srv.S3URL, scoped.AccessKey, scoped.SecretKey)
	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("obj1"),
	})
	require.NoError(t, err, "GetObject on in-scope bucket")
	defer out.Body.Close()
	got, _ := io.ReadAll(out.Body)
	require.Equal(t, "hello", string(got), "GetObject body")
}

// TestE2E_IAM_ScopedKey_WrongBucket_403 — scoped key for "logs" is blocked on
// "reports". Audit reason key_scope_mismatch is enforced server-side (unit
// coverage); here we verify the 403 surface behaviour.
func TestE2E_IAM_ScopedKey_WrongBucket_403(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	ctx := context.Background()

	bootCli := s3ClientFor(srv.S3URL, srv.BootstrapAK, srv.BootstrapSK)

	// Create both buckets via bootstrap.
	for _, bkt := range []string{"st2-logs", "st2-reports"} {
		_, err := bootCli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bkt)})
		require.NoErrorf(t, err, "CreateBucket %s", bkt)
	}
	_, err := bootCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("st2-reports"), Key: aws.String("secret"),
		Body: strings.NewReader("classified"),
	})
	require.NoError(t, err, "PutObject reports")

	// SA alice gets grants on both buckets but a key scoped to "logs" only.
	alice := iamCreateSA(t, srv.AdminSock, "alice-st2")
	iamGrantPut(t, srv.AdminSock, alice.SAID, "st2-logs", "Read")
	iamGrantPut(t, srv.AdminSock, alice.SAID, "st2-reports", "Read")
	scoped := iamKeyCreateScoped(t, srv.AdminSock, alice.SAID, []string{"st2-logs"})
	iamWaitKeyReady(t, srv.S3URL, scoped.AccessKey, scoped.SecretKey, 10*time.Second)

	cli := s3ClientFor(srv.S3URL, scoped.AccessKey, scoped.SecretKey)
	_, err = cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("st2-reports"), Key: aws.String("secret"),
	})
	require.Error(t, err, "GetObject on out-of-scope bucket succeeded; expected 403")
	status := httpStatusFrom(err)
	require.Containsf(t, []int{http.StatusForbidden, http.StatusUnauthorized}, status, "GetObject out-of-scope: err=%v; want 403", err)
	// Audit reason key_scope_mismatch is asserted by server unit tests; here
	// the 403 response is the observable contract.
}

// TestE2E_IAM_KeyCreate_OverScope_400 — requesting a key scoped to a bucket
// the SA has no grant on must return 400 with the bucket name in the body.
// (400 Bad Request matches the project's existing admin-validation pattern.)
func TestE2E_IAM_KeyCreate_OverScope_400(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	// SA alice has a grant on "st3-logs" only.
	alice := iamCreateSA(t, srv.AdminSock, "alice-st3")
	iamGrantPut(t, srv.AdminSock, alice.SAID, "st3-logs", "Read")

	// Request a key scoped to both "st3-logs" and "st3-reports" (no grant).
	status, body := iamAdminRaw(t, srv.AdminSock, "POST",
		"/v1/iam/sa/"+alice.SAID+"/key",
		map[string]any{"buckets": []string{"st3-logs", "st3-reports"}},
	)
	require.Equalf(t, http.StatusBadRequest, status, "KeyCreate over-scope: body=%s", string(body))
	require.Containsf(t, string(body), "st3-reports", "400 body does not mention denied bucket: body=%s", string(body))
}

// TestE2E_IAM_LegacyKey_NilScope_AccessAllGrants is the R2 REGRESSION CRITICAL
// test. A key issued without --bucket (BucketScope == nil) must still access
// all buckets the SA is granted on. Pre-v0.0.99.0 behaviour must be preserved.
func TestE2E_IAM_LegacyKey_NilScope_AccessAllGrants(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	ctx := context.Background()

	bootCli := s3ClientFor(srv.S3URL, srv.BootstrapAK, srv.BootstrapSK)
	for _, bkt := range []string{"st4-logs", "st4-reports"} {
		_, err := bootCli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bkt)})
		require.NoErrorf(t, err, "CreateBucket %s", bkt)
		_, err = bootCli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bkt), Key: aws.String("obj"),
			Body: strings.NewReader("data"),
		})
		require.NoErrorf(t, err, "PutObject %s", bkt)
	}

	// SA bob has grants on both buckets.
	bob := iamCreateSA(t, srv.AdminSock, "bob-st4")
	iamGrantPut(t, srv.AdminSock, bob.SAID, "st4-logs", "Read")
	iamGrantPut(t, srv.AdminSock, bob.SAID, "st4-reports", "Read")

	// Legacy key: POST /key with empty body → BucketScope is nil.
	var legacy iamKeyResult
	iamDo(t, srv.AdminSock, "POST", "/v1/iam/sa/"+bob.SAID+"/key", map[string]any{}, &legacy)
	iamWaitKeyReady(t, srv.S3URL, legacy.AccessKey, legacy.SecretKey, 10*time.Second)

	cli := s3ClientFor(srv.S3URL, legacy.AccessKey, legacy.SecretKey)

	// Legacy key must access both granted buckets without any scope restriction.
	for _, bkt := range []string{"st4-logs", "st4-reports"} {
		out, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String("obj"),
		})
		require.NoErrorf(t, err, "legacy key GetObject on %s (backward compat broken)", bkt)
		out.Body.Close()
	}
}

// TestE2E_IAM_ScopedKey_SnapshotRoundtrip verifies that a bucket-scoped key
// retains its scope after a cluster restart (snapshot + raft replay). The
// key must still be accepted on the in-scope bucket and rejected on an
// out-of-scope bucket after restart.
func TestE2E_IAM_ScopedKey_SnapshotRoundtrip(t *testing.T) {
	h := startIAMTestServerWithRestart(t)

	ctx := context.Background()
	bootCli := s3ClientFor(h.S3URL, h.BootstrapAK, h.BootstrapSK)

	// Provision buckets and seed an object before shutdown.
	for _, bkt := range []string{"st5-logs", "st5-reports"} {
		_, err := bootCli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bkt)})
		require.NoErrorf(t, err, "CreateBucket %s", bkt)
	}
	_, err := bootCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("st5-logs"), Key: aws.String("obj"),
		Body: strings.NewReader("persistent"),
	})
	require.NoError(t, err, "PutObject st5-logs")

	// SA with grant on "st5-logs", issued a key scoped to "st5-logs".
	alice := iamCreateSA(t, h.AdminSock, "alice-st5")
	iamGrantPut(t, h.AdminSock, alice.SAID, "st5-logs", "Read")
	scoped := iamKeyCreateScoped(t, h.AdminSock, alice.SAID, []string{"st5-logs"})
	iamWaitKeyReady(t, h.S3URL, scoped.AccessKey, scoped.SecretKey, 10*time.Second)

	// Stop and restart the cluster. The IAM store rehydrates from snapshot/raft.
	h.Stop(t)
	h.Start(t)

	// Re-build the scoped client pointing at the restarted server.
	iamWaitKeyReady(t, h.S3URL, scoped.AccessKey, scoped.SecretKey, 15*time.Second)
	scopedCli := s3ClientFor(h.S3URL, scoped.AccessKey, scoped.SecretKey)

	// In-scope bucket must still work.
	out, err2 := scopedCli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("st5-logs"), Key: aws.String("obj"),
	})
	require.NoError(t, err2, "GetObject on in-scope bucket after restart")
	out.Body.Close()

	// Out-of-scope bucket must still be blocked.
	_, err2 = scopedCli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("st5-reports"), Key: aws.String("obj"),
	})
	require.Error(t, err2, "GetObject on out-of-scope bucket after restart succeeded; expected 403")
	require.Containsf(t, []int{http.StatusForbidden, http.StatusUnauthorized}, httpStatusFrom(err2), "GetObject out-of-scope after restart: want 403")
}

func grepIAMControlPlaneDataDir(t *testing.T, root, needle string) []string {
	t.Helper()
	return grepDataDir(t, filepath.Join(root, "meta_raft"), needle)
}

// grepDataDir scans every regular file under root for needle. It is kept
// simple because callers must pass the narrow persistence root they intend to
// verify, not the whole GrainFS data dir.
func grepDataDir(t *testing.T, root, needle string) []string {
	t.Helper()
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("stat %s: %v", root, err)
	}
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

func TestGrepIAMControlPlaneDataDirScansOnlyMetaRaft(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta_raft", "raft-v2", "000001.vlog")
	groupPath := filepath.Join(dir, "groups", "group-1", "raft-v2", "000001.vlog")
	require.NoError(t, os.MkdirAll(filepath.Dir(metaPath), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(groupPath), 0o755))
	require.NoError(t, os.WriteFile(groupPath, []byte("control-plane-secret"), 0o644))
	require.Empty(t, grepIAMControlPlaneDataDir(t, dir, "control-plane-secret"))

	err := os.WriteFile(metaPath, []byte("control-plane-secret"), 0o644)
	require.NoError(t, err)
	require.Equal(t, []string{metaPath}, grepIAMControlPlaneDataDir(t, dir, "control-plane-secret"))
}

// TestIAM_E2E_PolicyBypassClosed verifies the Phase 5d #4 fix: bucket
// policy CRUD now flows through the IAM authz layer rather than
// short-circuiting it. Pre-fix, alice (Read on her own bucket) could
// PUT/GET/DELETE bob's bucket policy — multi-team escape hatch.
func TestIAM_E2E_PolicyBypassClosed(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	ctx := context.Background()

	// alice has Read on alice-policy-bkt only.
	alice := iamCreateSA(t, srv.AdminSock, "alice-policy")
	iamGrantPut(t, srv.AdminSock, alice.SAID, "alice-policy-bkt", "Read")
	iamWaitKeyReady(t, srv.S3URL, alice.AccessKey, alice.SecretKey, 10*time.Second)

	// bob has Admin on bob-policy-bkt and creates the bucket.
	bob := iamCreateSA(t, srv.AdminSock, "bob-policy")
	iamGrantPut(t, srv.AdminSock, bob.SAID, "bob-policy-bkt", "Admin")
	iamWaitKeyReady(t, srv.S3URL, bob.AccessKey, bob.SecretKey, 10*time.Second)

	bobCli := s3ClientFor(srv.S3URL, bob.AccessKey, bob.SecretKey)
	if _, err := bobCli.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("bob-policy-bkt"),
	}); err != nil {
		t.Fatalf("bob CreateBucket: %v", err)
	}

	aliceCli := s3ClientFor(srv.S3URL, alice.AccessKey, alice.SecretKey)

	// 1) alice attempting to PUT bob-bucket policy must 403.
	_, err := aliceCli.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String("bob-policy-bkt"),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	if err == nil {
		t.Fatal("alice (Read on alice-bucket) was allowed to PutBucketPolicy on bob-bucket; expected 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized {
		t.Fatalf("alice PutBucketPolicy: status=%d err=%v; want 401/403", status, err)
	}

	// 2) alice GET bob-bucket?policy must also 403 (no Read grant on bob-bucket).
	_, err = aliceCli.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String("bob-policy-bkt"),
	})
	if err == nil {
		t.Fatal("alice was allowed to GetBucketPolicy on bob-bucket; expected 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized {
		t.Fatalf("alice GetBucketPolicy: status=%d err=%v; want 401/403", status, err)
	}

	// 3) alice DELETE bob-bucket?policy must also 403 (no Admin grant on bob-bucket).
	_, err = aliceCli.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String("bob-policy-bkt"),
	})
	if err == nil {
		t.Fatal("alice was allowed to DeleteBucketPolicy on bob-bucket; expected 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized {
		t.Fatalf("alice DeleteBucketPolicy: status=%d err=%v; want 401/403", status, err)
	}

	// 4) bob (Admin on bob-bucket) can PUT his own bucket's policy.
	if _, err = bobCli.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String("bob-policy-bkt"),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bob-policy-bkt/*"]}]}`),
	}); err != nil {
		t.Fatalf("bob (Admin) PutBucketPolicy on own bucket: %v", err)
	}
}
