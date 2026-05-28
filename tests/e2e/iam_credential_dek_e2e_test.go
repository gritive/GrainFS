package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// IAM credential DEK seal (R2): the bootstrap admin SA's secret key is sealed
// under the gen-aware DEK via the storage.DataEncryptor seam (DomainIAMCredential
// AAD). The end-user-facing assertion is "sigv4-signed S3 requests work" before
// AND after a restart — the latter exercises the two-pass-decode in
// MetaFSM.Restore (the live DEKKeeper is rebuilt before the IAM applier sees
// any apply, via wireIAMEncryptor in dek_keeper_restore.go).
//
// Single-node coverage. Cluster cache-invalidation (rotate-on-A,
// observe-rejection-on-B) requires a 3-node IAM harness which doesn't yet
// exist in this tree — captured as a TODO follow-up.

var _ = ginkgo.Describe("IAM credential DEK seal (R2)", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("signs S3 requests after bootstrap (DEK-sealed creds verify on hot path)", func() {
			runIAMCredentialDEKBootstrapSigV4(ginkgo.GinkgoTB())
		})

		ginkgo.It("survives a restart (R2 two-pass decode rehydrates DEK-sealed creds)", func() {
			runIAMCredentialDEKSurvivesRestart(ginkgo.GinkgoTB())
		})
	})
})

func runIAMCredentialDEKBootstrapSigV4(t testing.TB) {
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)

	const bucket = "r2-dek-probe"
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{srv.DataDir}, srv.BootstrapSAID, bucket, srv.Client)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ginkgo.DeferCleanup(cancel)
	_, err := srv.Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(),
		"sigv4 HeadBucket with DEK-sealed bootstrap creds must succeed — proves WrapSecret + cache + sigv4 path are aligned post-R2")
}

func runIAMCredentialDEKSurvivesRestart(t testing.TB) {
	h := startIAMTestServerWithRestart(t)

	// Seed a bucket so we have a target to probe after restart.
	const bucket = "r2-restart-probe"
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{h.DataDir}, h.BootstrapSAID, bucket, h.Client())

	// Sanity-check probe before restart.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ginkgo.DeferCleanup(cancel)
	_, err := h.Client().HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "pre-restart HeadBucket")

	// Restart. The destination FSM's IAM applier has NO encryptor wired
	// until the two-pass decode in MetaFSM.Restore builds a transient
	// adapter from the DEK trailer and rehydrates the SA plaintext, then
	// wireIAMEncryptor swaps in the live keeper post-Restore.
	h.Stop(t)
	h.Start(t)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	ginkgo.DeferCleanup(cancel2)
	_, err = h.Client().HeadBucket(ctx2, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(),
		"post-restart HeadBucket with SAME bootstrap creds must succeed — proves R2 two-pass decode + live keeper re-wire are correct")
}
