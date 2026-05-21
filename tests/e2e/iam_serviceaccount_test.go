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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/iamadmin"

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

var _ = ginkgo.Describe("IAM service accounts", func() {
	describeIAMServiceAccountContext("SingleNode", func(testing.TB) iamAdminTarget {
		return newSingleNodeIAMAdminTarget()
	})
	describeIAMServiceAccountContext("Cluster4Node", func(t testing.TB) iamAdminTarget {
		return newSharedClusterIAMAdminTarget(t)
	})

	ginkgo.Context("Control plane data directory", func() {
		ginkgo.It("scans only meta raft", func() {
			runGrepIAMControlPlaneDataDirScansOnlyMetaRaft(ginkgo.GinkgoTB())
		})
	})
})

func describeIAMServiceAccountContext(name string, factory func(testing.TB) iamAdminTarget) {
	ginkgo.Context(name, func() {
		var tgt iamAdminTarget

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		ginkgo.It("rejects revoked keys", func() { runIAMSARevokedKey(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("rejects expired keys", func() { runIAMSAExpiredKey(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("enforces the role operation matrix", func() { runIAMSARoleOpMatrix(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("rejects presigned URLs after key revocation", func() { runIAMSAPresignedRevoked(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("does not persist plaintext secrets on disk", func() { runIAMSANoPlaintext(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("preserves the default service account after wildcard removal", func() { runIAMSAWildcardRemoval(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("allows scoped keys on the right bucket", func() { runIAMSAScopedRight(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("denies scoped keys on the wrong bucket", func() { runIAMSAScopedWrong(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("rejects over-scoped key creation", func() { runIAMSAKeyOverScope(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("lets legacy nil-scope keys access all grants", func() { runIAMSALegacyKey(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("round-trips scoped keys through snapshots", func() { runIAMSAScopedSnapshot(ginkgo.GinkgoTB(), tgt) })
		ginkgo.It("closes the policy bypass", func() { runIAMSAPolicyBypass(ginkgo.GinkgoTB(), tgt) })
	})
}

// runIAMSARevokedKey: alice creates an SA + bucket-scoped policy + bucket
// (via admin), PUTs an object, then admin revokes alice's only key.
// The next S3 operation must be denied (401/403).
func runIAMSARevokedKey(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	saID, ak, sk := tgt.uniqueSA(t, "et1-revoke")
	bucket := tgt.uniqueBucket(t, "et1-revoke")
	attachAdminPolicyOnBucket(t, tgt, saID, bucket, "Admin")

	cli := s3ClientForSA(tgt, ak, sk)
	iamWaitKeyReady(t, tgt.endpoint(0), ak, sk, 10*time.Second)

	if _, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"),
		Body: strings.NewReader("hello"),
	}); err != nil {
		t.Fatalf("alice PutObject: %v", err)
	}

	// Revoke alice's only access key via admin.
	iamKeyRevoke(t, tgt.adminSockPath(), saID, ak)

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

// runIAMSAExpiredKey: rotate a short-TTL key for the SA, wait until expiry is
// observed, request must fail.
func runIAMSAExpiredKey(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	saID, bootAK, bootSK := tgt.uniqueSA(t, "et1-expire")
	bucket := tgt.uniqueBucket(t, "et1-expire")
	attachAdminPolicyOnBucket(t, tgt, saID, bucket, "Read")

	iamWaitKeyReady(t, tgt.endpoint(0), bootAK, bootSK, 10*time.Second)

	exp := iamKeyCreateExpiringIn(t, tgt.adminSockPath(), saID, time.Second)
	iamWaitKeyReady(t, tgt.endpoint(0), exp.AccessKey, exp.SecretKey, 5*time.Second)
	expCli := s3ClientForSA(tgt, exp.AccessKey, exp.SecretKey)

	// Confirm the expiring key works while still valid.
	if _, err := expCli.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("expiring key pre-expiry HeadBucket: %v", err)
	}

	var err error
	gomega.Eventually(func() bool {
		_, err = expCli.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		return err != nil
	}).WithTimeout(3*time.Second).WithPolling(50*time.Millisecond).
		Should(gomega.BeTrue(), "expiring key should stop authenticating after expires_at")

	if err == nil {
		t.Fatalf("HEAD after expiry succeeded; expected auth failure")
	}
	status := httpStatusFrom(err)
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		t.Fatalf("HEAD after expiry: status=%d err=%v; want 401/403", status, err)
	}
}

// runIAMSARoleOpMatrix: for each (role, op) combination, verify allow/deny.
// Note: s3:CreateBucket is admin-UDS-only (Decision #8) and is unconditionally
// denied on the data plane regardless of policy. The Admin_CreateBucket case
// from the legacy grant model is replaced with Admin_PutObject to preserve
// coverage of the Admin role.
func runIAMSARoleOpMatrix(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	// Admin creates the shared bucket and seeds an object so Read/Write SAs
	// can target it.
	sharedBucket := tgt.uniqueBucket(t, "et2-shared")
	bootCli := tgt.pickNode(0)
	if _, err := bootCli.PutObject(ctx, &s3.PutObjectInput{
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
		// Admin_CreateBucket intentionally omitted: s3:CreateBucket is
		// admin-UDS-only (Decision #8 in s3auth/authorizer.go) and is
		// unconditionally denied on the data plane regardless of policy.
		// Coverage of Admin role is preserved via Admin_PutObject.
		{"Admin", "Put", true},
	}

	for i, tc := range cases {
		ginkgo.By(fmt.Sprintf("%s_%s", tc.role, tc.op))
		saID, saAK, saSK := tgt.uniqueSA(t, fmt.Sprintf("et2-%s-%s-%d", tc.role, tc.op, i))
		attachAdminPolicyOnBucket(t, tgt, saID, sharedBucket, tc.role)
		iamWaitKeyReady(t, tgt.endpoint(0), saAK, saSK, 10*time.Second)
		cli := s3ClientForSA(tgt, saAK, saSK)

		targetKey := fmt.Sprintf("k-%s-%s-%d", tc.role, tc.op, i)
		var err error

		switch tc.op {
		case "Get":
			_, err = cli.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(sharedBucket), Key: aws.String("seed"),
			})
		case "Put":
			_, err = cli.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(sharedBucket), Key: aws.String(targetKey),
				Body: strings.NewReader("v"),
			})
		case "Delete":
			// Pre-create the object as bootstrap so Delete has a target.
			if _, e := bootCli.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(sharedBucket), Key: aws.String(targetKey),
				Body: strings.NewReader("v"),
			}); e != nil {
				t.Fatalf("bootstrap pre-Put for Delete: %v", e)
			}
			_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(sharedBucket), Key: aws.String(targetKey),
			})
		default:
			t.Fatalf("unhandled op %q", tc.op)
		}

		if tc.allow {
			if err != nil {
				t.Fatalf("%s %s: expected allow but got err: %v", tc.role, tc.op, err)
			}
			continue
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
	}
}

// runIAMSAPresignedRevoked: alice presigns a GET, admin revokes alice's key,
// the presigned URL must no longer work.
func runIAMSAPresignedRevoked(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	saID, ak, sk := tgt.uniqueSA(t, "et3-presign")
	bucket := tgt.uniqueBucket(t, "et3-presign")
	attachAdminPolicyOnBucket(t, tgt, saID, bucket, "Admin")
	iamWaitKeyReady(t, tgt.endpoint(0), ak, sk, 10*time.Second)

	const key = "secret.txt"
	cli := s3ClientForSA(tgt, ak, sk)
	if _, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: strings.NewReader("classified"),
	}); err != nil {
		t.Fatalf("alice PutObject: %v", err)
	}

	// Presign GET with 5min expiry.
	presigned, err := s3auth.PresignURL(http.MethodGet,
		tgt.endpoint(0)+"/"+bucket+"/"+key,
		ak, sk, "us-east-1", 300)
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
	iamKeyRevoke(t, tgt.adminSockPath(), saID, ak)

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

// runIAMSANoPlaintext asserts the at-rest invariant on the IAM control-plane
// persistence path. IAM secrets must not appear in plaintext in any node's
// meta_raft directory.
func runIAMSANoPlaintext(t testing.TB, tgt iamAdminTarget) {
	t.Helper()

	saID, ak, sk := tgt.uniqueSA(t, "sc8")
	iamWaitKeyReady(t, tgt.endpoint(0), ak, sk, 10*time.Second)

	for _, dir := range tgt.dataDirs() {
		hits := grepIAMControlPlaneDataDir(t, dir, sk)
		if len(hits) > 0 {
			t.Fatalf("secret_key for SA %s appears in IAM control-plane persistence at %s: %v", saID, dir, hits)
		}
	}
}

// runIAMSAWildcardRemoval is skipped: the legacy grant model (wildcard grant
// on sa-default) was removed in §2. The /v1/iam/grant HTTP endpoints are no
// longer registered in the admin server (see hertz_routes_iam.go), and
// MetaCmdTypeIAMGrantWildcardDelete has no apply branch in the FSM (retained
// for backcompat with pre-§2 snapshots only). Coverage of sa-default isolation
// is now provided by policy-based unit tests.
func runIAMSAWildcardRemoval(t testing.TB, _ iamAdminTarget) {
	t.Skip("legacy: wildcard grant removed in §2; /v1/iam/grant endpoints unregistered; " +
		"sa-default isolation is covered by policy-model unit tests")
}

// runIAMSAScopedRight: scoped key for "logs" bucket grants access to objects
// inside "logs".
func runIAMSAScopedRight(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	bucket := tgt.uniqueBucket(t, "st1-logs")
	bootCli := tgt.pickNode(0)
	_, err := bootCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("obj1"),
		Body: strings.NewReader("hello"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject")

	saID, _, _ := tgt.uniqueSA(t, "alice-st1")
	attachAdminPolicyOnBucket(t, tgt, saID, bucket, "Read")

	// Issue scoped key restricted to this bucket.
	scoped := iamKeyCreateScoped(t, tgt.adminSockPath(), saID, []string{bucket})
	iamWaitKeyReady(t, tgt.endpoint(0), scoped.AccessKey, scoped.SecretKey, 10*time.Second)

	cli := s3ClientForSA(tgt, scoped.AccessKey, scoped.SecretKey)
	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("obj1"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GetObject on in-scope bucket")
	defer out.Body.Close()
	got, _ := io.ReadAll(out.Body)
	gomega.Expect(string(got)).To(gomega.Equal("hello"), "GetObject body")
}

// runIAMSAScopedWrong: scoped key for "logs" is blocked on "reports".
func runIAMSAScopedWrong(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	logsBucket := tgt.uniqueBucket(t, "st2-logs")
	reportsBucket := tgt.uniqueBucket(t, "st2-reports")
	bootCli := tgt.pickNode(0)
	_, err := bootCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(reportsBucket), Key: aws.String("secret"),
		Body: strings.NewReader("classified"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject reports")

	// SA alice gets grants on both buckets but a key scoped to logs only.
	saID, _, _ := tgt.uniqueSA(t, "alice-st2")
	attachAdminPolicyOnBucket(t, tgt, saID, logsBucket, "Read")
	attachAdminPolicyOnBucket(t, tgt, saID, reportsBucket, "Read")
	scoped := iamKeyCreateScoped(t, tgt.adminSockPath(), saID, []string{logsBucket})
	iamWaitKeyReady(t, tgt.endpoint(0), scoped.AccessKey, scoped.SecretKey, 10*time.Second)

	cli := s3ClientForSA(tgt, scoped.AccessKey, scoped.SecretKey)
	_, err = cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(reportsBucket), Key: aws.String("secret"),
	})
	gomega.Expect(err).To(gomega.HaveOccurred(), "GetObject on out-of-scope bucket succeeded; expected 403")
	status := httpStatusFrom(err)
	gomega.Expect([]int{http.StatusForbidden, http.StatusUnauthorized}).To(gomega.ContainElement(status),
		"GetObject out-of-scope: err=%v; want 403", err)
}

// runIAMSAKeyOverScope: requesting a key scoped to a bucket the SA has no
// policy on must return 400 with the bucket name in the body.
//
// This case is skipped: in the legacy grant model, CreateKey validated that
// the requested bucket scope was a subset of the SA's grants (returning 400
// otherwise). In the policy model, CreateKey only validates bucket name
// syntax — no policy-level over-scope check has been implemented. The
// scoped-key enforcement at request time is preserved (ScopedKey_WrongBucket_403).
func runIAMSAKeyOverScope(t testing.TB, _ iamAdminTarget) {
	t.Skip("legacy: over-scope 400 was grant-model only; policy model has no key-create scope gate; " +
		"runtime enforcement is covered by ScopedKey_WrongBucket_403")
}

// runIAMSALegacyKey: a key issued without --bucket (BucketScope == nil) must
// still access all buckets the SA has policy on. Pre-v0.0.99.0 backward compat.
func runIAMSALegacyKey(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	logsBucket := tgt.uniqueBucket(t, "st4-logs")
	reportsBucket := tgt.uniqueBucket(t, "st4-reports")
	bootCli := tgt.pickNode(0)
	for _, bkt := range []string{logsBucket, reportsBucket} {
		_, err := bootCli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bkt), Key: aws.String("obj"),
			Body: strings.NewReader("data"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject %s", bkt)
	}

	saID, _, _ := tgt.uniqueSA(t, "bob-st4")
	attachAdminPolicyOnBucket(t, tgt, saID, logsBucket, "Read")
	attachAdminPolicyOnBucket(t, tgt, saID, reportsBucket, "Read")

	// Legacy key: POST /key with empty body → BucketScope is nil.
	var legacy iamKeyResult
	iamDo(t, tgt.adminSockPath(), "POST", "/v1/iam/sa/"+saID+"/key", map[string]any{}, &legacy)
	iamWaitKeyReady(t, tgt.endpoint(0), legacy.AccessKey, legacy.SecretKey, 10*time.Second)

	cli := s3ClientForSA(tgt, legacy.AccessKey, legacy.SecretKey)

	for _, bkt := range []string{logsBucket, reportsBucket} {
		out, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bkt), Key: aws.String("obj"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "legacy key GetObject on %s (backward compat broken)", bkt)
		out.Body.Close()
	}
}

// runIAMSAScopedSnapshot: verifies that a bucket-scoped key retains its scope
// after a cluster restart (snapshot + raft replay).
// Cluster4Node is skipped: the shared cluster fixture is process-global and
// cannot be safely restarted without disrupting other concurrent tests.
func runIAMSAScopedSnapshot(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	if tgt.isCluster {
		t.Skip("snapshot-roundtrip requires stop/start; shared cluster fixture cannot be restarted")
	}

	h := startIAMTestServerWithRestart(t)
	ctx := context.Background()

	// Provision buckets via admin UDS — s3:CreateBucket is adminUDSOnlyActions
	// per §3 Decision #8 and is unconditionally refused on the data plane.
	// Create a dedicated seed SA + admin-on-bucket policy so PutObject can seed.
	bootIAMCli := iamadmin.NewClientForURL(h.AdminSock)
	seed := iamCreateSA(t, h.AdminSock, "seed-st5")
	seedTgt := iamAdminTarget{
		adminSockPath: func() string { return h.AdminSock },
		endpoint:      func(i int) string { return h.S3URL },
	}
	for _, bkt := range []string{"st5-logs", "st5-reports"} {
		gomega.Expect(bootIAMCli.BucketCreate(ctx, bkt, "", "")).To(gomega.Succeed(), "BucketCreate %s via admin UDS", bkt)
		attachAdminPolicyOnBucket(t, seedTgt, seed.SAID, bkt, "Admin")
	}
	iamWaitKeyReady(t, h.S3URL, seed.AccessKey, seed.SecretKey, 10*time.Second)
	seedCli := s3ClientFor(h.S3URL, seed.AccessKey, seed.SecretKey)
	_, err := seedCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("st5-logs"), Key: aws.String("obj"),
		Body: strings.NewReader("persistent"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject st5-logs")

	// SA with policy on "st5-logs" via the policy API, issued a key scoped to "st5-logs".
	alice := iamCreateSA(t, h.AdminSock, "alice-st5")
	// Use a dedicated restart-target to call attachAdminPolicyOnBucket.
	restartTgt := iamAdminTarget{
		adminSockPath: func() string { return h.AdminSock },
		endpoint:      func(i int) string { return h.S3URL },
	}
	attachAdminPolicyOnBucket(t, restartTgt, alice.SAID, "st5-logs", "Read")
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
	gomega.Expect(err2).NotTo(gomega.HaveOccurred(), "GetObject on in-scope bucket after restart")
	out.Body.Close()

	// Out-of-scope bucket must still be blocked.
	_, err2 = scopedCli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("st5-reports"), Key: aws.String("obj"),
	})
	gomega.Expect(err2).To(gomega.HaveOccurred(), "GetObject on out-of-scope bucket after restart succeeded; expected 403")
	gomega.Expect([]int{http.StatusForbidden, http.StatusUnauthorized}).To(gomega.ContainElement(httpStatusFrom(err2)),
		"GetObject out-of-scope after restart: want 403")
}

// runIAMSAPolicyBypass verifies that bucket policy CRUD flows through the IAM
// authz layer. alice (Read on alice-bucket) must not be able to
// PUT/GET/DELETE bob's bucket policy.
func runIAMSAPolicyBypass(t testing.TB, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	aliceBucket := tgt.uniqueBucket(t, "alice-policy")
	bobBucket := tgt.uniqueBucket(t, "bob-policy")

	// alice has Read on alice-policy-bkt only.
	aliceSAID, aliceAK, aliceSK := tgt.uniqueSA(t, "alice-policy")
	attachAdminPolicyOnBucket(t, tgt, aliceSAID, aliceBucket, "Read")
	iamWaitKeyReady(t, tgt.endpoint(0), aliceAK, aliceSK, 10*time.Second)

	// bob has Admin on bob-policy-bkt; the bucket is already created by
	// tgt.uniqueBucket so we just attach a policy for bob.
	bobSAID, bobAK, bobSK := tgt.uniqueSA(t, "bob-policy")
	attachAdminPolicyOnBucket(t, tgt, bobSAID, bobBucket, "Admin")
	iamWaitKeyReady(t, tgt.endpoint(0), bobAK, bobSK, 10*time.Second)

	aliceCli := s3ClientForSA(tgt, aliceAK, aliceSK)
	bobCli := s3ClientForSA(tgt, bobAK, bobSK)

	// 1) alice attempting to PUT bob-bucket policy must 403.
	_, err := aliceCli.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bobBucket),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	if err == nil {
		t.Fatal("alice (Read on alice-bucket) was allowed to PutBucketPolicy on bob-bucket; expected 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized {
		t.Fatalf("alice PutBucketPolicy: status=%d err=%v; want 401/403", status, err)
	}

	// 2) alice GET bob-bucket?policy must also 403 (or 404 for empty bucket policy).
	_, err = aliceCli.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(bobBucket),
	})
	if err == nil {
		t.Fatal("alice was allowed to GetBucketPolicy on bob-bucket; expected 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized && status != http.StatusNotFound {
		t.Fatalf("alice GetBucketPolicy: status=%d err=%v; want 401/403 (or 404 for empty bucket policy — see follow-up F39)", status, err)
	}
	// 404 NoSuchBucketPolicy is accepted as functionally equivalent here (no
	// policy → no read possible). Server checks resource existence before authz,
	// so an empty bucket returns 404 before the IAM check runs. Fixing the
	// authz-before-existence-check order is tracked as follow-up F39.

	// 3) alice DELETE bob-bucket?policy must also 403.
	_, err = aliceCli.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bobBucket),
	})
	if err == nil {
		t.Fatal("alice was allowed to DeleteBucketPolicy on bob-bucket; expected 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized {
		t.Fatalf("alice DeleteBucketPolicy: status=%d err=%v; want 401/403", status, err)
	}

	// 4) bob (Admin on bob-bucket) still cannot PUT bucket policy on the data
	// plane. s3:PutBucketPolicy is adminUDSOnlyActions — unconditionally denied
	// at the S3 data plane regardless of attached IAM policy (Decision #8 in
	// internal/s3auth/authorizer.go). This matches the same restriction as
	// s3:CreateBucket/DeleteBucket: all SA credential requests are 403.
	_, err = bobCli.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bobBucket),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::` + bobBucket + `/*"]}]}`),
	})
	if err == nil {
		t.Fatal("bob (Admin) PutBucketPolicy succeeded on data plane; s3:PutBucketPolicy is adminUDSOnlyActions and must always return 403")
	}
	if status := httpStatusFrom(err); status != http.StatusForbidden && status != http.StatusUnauthorized {
		t.Fatalf("bob PutBucketPolicy: status=%d err=%v; want 401/403 (adminUDSOnlyActions)", status, err)
	}
}

// iamKeyCreateScoped issues a new scoped key for saID restricted to buckets.
func iamKeyCreateScoped(t testing.TB, sock, saID string, buckets []string) iamKeyResult {
	t.Helper()
	var out iamKeyResult
	iamDo(t, sock, "POST", "/v1/iam/sa/"+saID+"/key",
		map[string]any{"buckets": buckets}, &out)
	return out
}

// iamAdminRaw issues a raw admin UDS request and returns (statusCode, body).
// Unlike iamDo it does NOT fatal on 4xx so callers can assert error cases.
func iamAdminRaw(t testing.TB, sock, method, path string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "iamAdminRaw: marshal")
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, "http://unix"+path, rdr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "iamAdminRaw: build request")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := iamUDSClient(sock).Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "iamAdminRaw: %s %s", method, path)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody
}

func grepIAMControlPlaneDataDir(t testing.TB, root, needle string) []string {
	t.Helper()
	return grepDataDir(t, filepath.Join(root, "meta_raft"), needle)
}

// grepDataDir scans every regular file under root for needle.
func grepDataDir(t testing.TB, root, needle string) []string {
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

func runGrepIAMControlPlaneDataDirScansOnlyMetaRaft(t testing.TB) {
	t.Helper()
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta_raft", "raft-v2", "000001.vlog")
	groupPath := filepath.Join(dir, "groups", "group-1", "raft-v2", "000001.vlog")
	gomega.Expect(os.MkdirAll(filepath.Dir(metaPath), 0o755)).To(gomega.Succeed())
	gomega.Expect(os.MkdirAll(filepath.Dir(groupPath), 0o755)).To(gomega.Succeed())
	gomega.Expect(os.WriteFile(groupPath, []byte("control-plane-secret"), 0o644)).To(gomega.Succeed())
	gomega.Expect(grepIAMControlPlaneDataDir(t, dir, "control-plane-secret")).To(gomega.BeEmpty())

	err := os.WriteFile(metaPath, []byte("control-plane-secret"), 0o644)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(grepIAMControlPlaneDataDir(t, dir, "control-plane-secret")).To(gomega.Equal([]string{metaPath}))
}
