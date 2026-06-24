package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// External PDP Slice 7 PR-B — data-plane (S3) enforcement matrix.
//
// The data-plane decorator (boot_phases_srvopts.go wraps the policy authorizer
// with pdp.NewDecorator(..., "data_plane")) consults the external PDP with a
// deny-override rule ONLY when iam.pdp.enabled && iam.pdp.data_plane.enabled.
// Each spec drives an S3 op against a running single-node server, flipping
// the iam.pdp config via the admin UDS between Its.
//
// In every spec the GrainFS inner authorizer ALLOWS the op (the bootstrap SA
// has bucket-admin), so the PDP is the deciding layer. The mock PDP is an
// httptest server bound to loopback http (the only scheme the SSRF egress
// filter permits) with a flip-able decision and a request counter.
//
// Cluster parity is deferred: there is no multi-node IAM harness that exposes a
// per-node admin UDS for setPDPConfig + a shared mock PDP. The control-plane
// PDP suite is likewise single-node.

// pdpDataPlaneConfigJSON builds an iam.pdp document with data_plane.enabled:true
// (so the data-plane-scope decorator enforces) pointing at the given PDP URL
// with the given failure policy ("closed"|"open"). No cache block.
func pdpDataPlaneConfigJSON(url, policy string) string {
	return fmt.Sprintf(
		`{"enabled":true,"endpoint":"%s","failure_policy":"%s","data_plane":{"enabled":true}}`,
		url, policy,
	)
}

// pdpDataPlaneConfigJSONDisabled builds an iam.pdp document with the top-level
// gate enabled but data_plane.enabled OMITTED (defaults false), so the
// data-plane decorator is a pure pass-through and the PDP is never consulted on
// S3 ops.
func pdpDataPlaneConfigJSONDisabled(url, policy string) string {
	return fmt.Sprintf(
		`{"enabled":true,"endpoint":"%s","failure_policy":"%s"}`,
		url, policy,
	)
}

// pdpDataPlaneConfigJSONWithCache builds an iam.pdp document with data_plane
// enforcement AND a cache block (ttl_allow/ttl_deny/grace_ttl). Empty duration
// strings are omitted by the parser (treated as 0).
func pdpDataPlaneConfigJSONWithCache(url, policy, ttlAllow, ttlDeny, graceTTL string) string {
	return fmt.Sprintf(
		`{"enabled":true,"endpoint":"%s","failure_policy":"%s","data_plane":{"enabled":true},"cache":{"ttl_allow":"%s","ttl_deny":"%s","grace_ttl":"%s"}}`,
		url, policy, ttlAllow, ttlDeny, graceTTL,
	)
}

// startMockPDP starts an httptest mock PDP whose decision is flip-able via the
// returned atomics and whose inbound requests are counted. When down is set it
// returns 500 (a transport-level failure that triggers the failure policy /
// grace, NOT a DenyError). DeferCleanup closes it.
func startMockPDP(allow, down *atomic.Bool, reqs *atomic.Int64) string {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		if down.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if allow.Load() {
			_ = json.NewEncoder(w).Encode(map[string]string{"decision": "allow", "reason": "e2e"})
			return
		}
		// A deny is signalled by 403 + body (matches the control-plane mock and
		// internal/iam/pdp/client.go's DenyError mapping).
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"decision": "deny", "reason": "e2e-deny"})
	})
	mock := httptest.NewServer(h)
	ginkgo.DeferCleanup(mock.Close)
	return mock.URL
}

// s3ErrCode extracts the S3 wire error code (e.g. "AccessDenied") from an
// aws-sdk-go-v2 operation error.
func s3ErrCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

var _ = ginkgo.Describe("External PDP data-plane (S3)", ginkgo.Ordered, func() {
	var (
		srv      iamTestServer
		bucket   string
		pdpURL   string
		pdpAllow atomic.Bool
		pdpDown  atomic.Bool
		pdpReqs  atomic.Int64
	)

	// putWhileDisabled PUTs an object while the data-plane PDP is OFF, so the PUT
	// itself is never PDP-gated. The subsequent GET (under the spec's config) is
	// the operation under test.
	putWhileDisabled := func(key string, body []byte) {
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpDataPlaneConfigJSONDisabled(pdpURL, "closed"))
		_, err := srv.Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "seed PutObject must succeed while data-plane PDP is off")
	}

	getObject := func(key string) ([]byte, error) {
		out, err := srv.Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, err
		}
		defer out.Body.Close()
		return io.ReadAll(out.Body)
	}

	ginkgo.BeforeAll(func() {
		pdpURL = startMockPDP(&pdpAllow, &pdpDown, &pdpReqs)

		srv = startIAMTestServer(ginkgo.GinkgoTB())
		ginkgo.DeferCleanup(srv.Stop)

		// One bucket for the whole suite; bucket-admin attached to the bootstrap
		// SA so the GrainFS inner authorizer always allows (PDP becomes the
		// deciding layer). Each It uses a distinct key to keep cached decisions
		// from bleeding across Ordered specs.
		bucket = bucketNameFor("single", "pdp-dataplane-s3", "matrix")
		createBucketWithAdminPolicyAttachViaUDSAny(ginkgo.GinkgoTB(),
			[]string{srv.DataDir}, srv.BootstrapSAID, bucket, srv.Client)
	})

	ginkgo.It("does NOT consult the PDP when data_plane.enabled is false", func() {
		pdpAllow.Store(false) // mock would DENY if it were ever called
		pdpDown.Store(false)
		putWhileDisabled("dp-off.txt", []byte("dp-off"))

		// Top-level enabled:true but data_plane omitted ⇒ pass-through.
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpDataPlaneConfigJSONDisabled(pdpURL, "closed"))
		pdpReqs.Store(0)

		got, err := getObject("dp-off.txt")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "GET must succeed when the data-plane decorator is disabled")
		gomega.Expect(got).To(gomega.Equal([]byte("dp-off")))
		gomega.Expect(pdpReqs.Load()).To(gomega.Equal(int64(0)),
			"the PDP must not be consulted on the data plane when data_plane.enabled is false")
	})

	ginkgo.It("returns AccessDenied when GrainFS allows but the PDP denies", func() {
		putWhileDisabled("dp-deny.txt", []byte("dp-deny"))

		pdpAllow.Store(false)
		pdpDown.Store(false)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpDataPlaneConfigJSON(pdpURL, "closed"))

		_, err := getObject("dp-deny.txt")
		gomega.Expect(err).To(gomega.HaveOccurred(), "PDP deny must block the GET")
		gomega.Expect(s3ErrCode(err)).To(gomega.Equal("AccessDenied"),
			"a data-plane PDP deny must surface as S3 403/AccessDenied: %v", err)
	})

	ginkgo.It("succeeds when GrainFS allows and the PDP allows", func() {
		putWhileDisabled("dp-allow.txt", []byte("dp-allow"))

		pdpAllow.Store(true)
		pdpDown.Store(false)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpDataPlaneConfigJSON(pdpURL, "closed"))

		got, err := getObject("dp-allow.txt")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PDP allow must permit the GET")
		gomega.Expect(got).To(gomega.Equal([]byte("dp-allow")))
	})

	ginkgo.It("denies when the PDP is down and failure_policy is closed", func() {
		putWhileDisabled("dp-closed.txt", []byte("dp-closed"))

		pdpAllow.Store(false)
		pdpDown.Store(true)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpDataPlaneConfigJSON(pdpURL, "closed"))

		_, err := getObject("dp-closed.txt")
		gomega.Expect(err).To(gomega.HaveOccurred(), "fail-closed must deny the GET when the PDP is down")
		gomega.Expect(s3ErrCode(err)).To(gomega.Equal("AccessDenied"),
			"fail-closed PDP-down must surface as S3 403/AccessDenied: %v", err)
	})

	ginkgo.It("succeeds when the PDP is down and failure_policy is open", func() {
		putWhileDisabled("dp-open.txt", []byte("dp-open"))

		pdpAllow.Store(false)
		pdpDown.Store(true)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpDataPlaneConfigJSON(pdpURL, "open"))

		got, err := getObject("dp-open.txt")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "fail-open must permit the GET when the PDP is down")
		gomega.Expect(got).To(gomega.Equal([]byte("dp-open")))
	})

	ginkgo.It("grace-serves a stale allow through a brief PDP outage", func() {
		putWhileDisabled("dp-grace.txt", []byte("dp-grace"))

		pdpAllow.Store(true)
		pdpDown.Store(false)
		// Short allow-TTL so the cached allow goes stale quickly; long grace so the
		// stale entry can be grace-served once the PDP starts erroring. Fail-closed:
		// only grace (not the failure policy) can save the request during the outage,
		// so a success through the outage IS the proof of grace.
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
			pdpDataPlaneConfigJSONWithCache(pdpURL, "closed", "1s", "", "60s"))

		// Warm the cache with a fresh allow on the exact key used below.
		got, err := getObject("dp-grace.txt")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "warm GET must be allowed by the PDP")
		gomega.Expect(got).To(gomega.Equal([]byte("dp-grace")))

		// Age the cached entry past ttl_allow (1s) but well within grace_ttl (60s).
		time.Sleep(1500 * time.Millisecond)

		// Take the PDP down: every consult now returns 500.
		pdpDown.Store(true)

		// Same key ⇒ same cache key: stale lookup → PDP re-consult fails → grace
		// serves the stale allow. Fail-closed would otherwise deny.
		got, err = getObject("dp-grace.txt")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "grace must serve the stale allow through the outage")
		gomega.Expect(got).To(gomega.Equal([]byte("dp-grace")))
	})

	// SSRF hard-deny (endpoint pointing at a blocked target) is intentionally NOT
	// asserted here: the SSRF egress filter rejects a private/blocked endpoint at
	// config-set/parse time, so setPDPConfig itself would fail rather than the op
	// yielding a clean data-plane deny. That path is covered by unit tests in
	// internal/iam/pdp.
})
