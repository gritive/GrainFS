package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/credentialadmin"
)

// pdpConfigJSON builds the iam.pdp config document pointing at a unix-socket
// PDP with the given failure policy ("closed"|"open").
func pdpConfigJSON(sock, policy string) string {
	return fmt.Sprintf(`{"enabled":true,"endpoint":"unix://%s","failure_policy":"%s"}`, sock, policy)
}

// pdpConfigJSONWithCache builds the iam.pdp config document with a nested cache
// block (ttl_allow/ttl_deny/grace_ttl). Empty duration strings are omitted so a
// caller can populate only the knobs a spec needs.
func pdpConfigJSONWithCache(sock, policy, ttlAllow, ttlDeny, graceTTL string) string {
	return fmt.Sprintf(
		`{"enabled":true,"endpoint":"unix://%s","failure_policy":"%s","cache":{"ttl_allow":"%s","ttl_deny":"%s","grace_ttl":"%s"}}`,
		sock, policy, ttlAllow, ttlDeny, graceTTL,
	)
}

// setPDPConfig writes the iam.pdp config document via the admin UDS single-key
// config PUT (PUT /v1/config/iam.pdp). The decorator reads this per request, so
// the value takes effect without a restart.
func setPDPConfig(t testing.TB, adminSock, value string) {
	t.Helper()
	tp, err := adminapi.NewTransport(adminSock)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create admin transport for iam.pdp config")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gomega.Expect(tp.Put(ctx, "/v1/config/iam.pdp", adminapi.ConfigSetReq{Value: value}, nil)).
		To(gomega.Succeed(), "PUT /v1/config/iam.pdp")
}

// tryCreateProtocolCredential attaches the GrainFS grant for the credential
// resource (so the inner authorizer allows and the PDP is actually consulted),
// then attempts to mint a protocol credential and RETURNS the create error.
// On success it schedules a revoke cleanup.
func tryCreateProtocolCredential(t testing.TB, adminSock, saID, protocol, resource, mode string) error {
	t.Helper()
	attachProtocolCredentialPolicy(t, adminSock, saID, protocol, resource)
	tp, err := adminapi.NewTransport(adminSock)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create admin transport for protocol credential")
	cli := &credentialadmin.Client{Transport: tp}
	cred, err := cli.Create(context.Background(), credentialadmin.CreateReq{
		SAID:     saID,
		Protocol: protocol,
		Resource: resource,
		Mode:     mode,
	})
	if err != nil {
		return err
	}
	ginkgo.DeferCleanup(func() {
		_, _ = cli.Revoke(context.Background(), cred.ID)
	})
	return nil
}

// createProtocolCredentialOnly mints a protocol credential WITHOUT (re)attaching
// the GrainFS grant — the caller is expected to have attached the policy once,
// while the PDP is reachable. This keeps the credential-create wire request
// (action+resource+principal) identical across repeated calls so the PDP
// decision cache key matches, and avoids re-running the admin policy ops (which
// the cache+grace specs cannot afford during a simulated outage). Returns the
// create error and schedules a revoke cleanup on success.
func createProtocolCredentialOnly(t testing.TB, adminSock, saID, protocol, resource, mode string) error {
	t.Helper()
	tp, err := adminapi.NewTransport(adminSock)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create admin transport for protocol credential")
	cli := &credentialadmin.Client{Transport: tp}
	cred, err := cli.Create(context.Background(), credentialadmin.CreateReq{
		SAID:     saID,
		Protocol: protocol,
		Resource: resource,
		Mode:     mode,
	})
	if err != nil {
		return err
	}
	ginkgo.DeferCleanup(func() {
		_, _ = cli.Revoke(context.Background(), cred.ID)
	})
	return nil
}

var _ = ginkgo.Describe("External PDP adapter (Slice 1)", ginkgo.Ordered, func() {
	var (
		srv      iamTestServer
		pdpSock  string
		pdpAllow atomic.Bool
		pdpDown  atomic.Bool
		pdpReqs  atomic.Int64
	)

	ginkgo.BeforeAll(func() {
		// Mock PDP on a unix socket. ParseConfig requires an absolute path, so
		// keep the socket under a short temp dir to stay within sun_path.
		dir, err := os.MkdirTemp("", "grainfs-pdp-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mkdtemp for pdp socket")
		ginkgo.DeferCleanup(func() { _ = os.RemoveAll(dir) })
		pdpSock = filepath.Join(dir, "pdp.sock")

		ln, err := net.Listen("unix", pdpSock)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "listen on pdp socket")

		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Count every inbound request BEFORE the down-check so the grace spec can
			// prove the PDP was re-consulted during the simulated outage.
			pdpReqs.Add(1)
			if pdpDown.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			dec := "deny"
			if pdpAllow.Load() {
				dec = "allow"
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"decision": dec, "reason": "e2e"})
		})
		mock := &httptest.Server{Listener: ln, Config: &http.Server{Handler: h}}
		mock.Start()
		ginkgo.DeferCleanup(mock.Close)

		srv = startIAMTestServer(ginkgo.GinkgoTB())
		ginkgo.DeferCleanup(srv.Stop)

		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpConfigJSON(pdpSock, "closed"))
	})

	ginkgo.It("denies the credential mint when the PDP denies", func() {
		pdpAllow.Store(false)
		pdpDown.Store(false)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpConfigJSON(pdpSock, "closed"))

		err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-deny", "rw")
		gomega.Expect(err).To(gomega.HaveOccurred(), "PDP deny should block the mint")
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("denied by external policy"))
	})

	ginkgo.It("allows the credential mint when the PDP allows", func() {
		pdpAllow.Store(true)
		pdpDown.Store(false)

		err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-allow", "rw")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PDP allow should permit the mint")
	})

	ginkgo.It("denies when the PDP is down and failure_policy is closed", func() {
		pdpAllow.Store(false)
		pdpDown.Store(true)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpConfigJSON(pdpSock, "closed"))

		err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-closed", "rw")
		gomega.Expect(err).To(gomega.HaveOccurred(), "fail-closed should block the mint when PDP is down")
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("pdp_unavailable"))
	})

	ginkgo.It("allows when the PDP is down and failure_policy is open", func() {
		pdpAllow.Store(false)
		pdpDown.Store(true)
		setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock, pdpConfigJSON(pdpSock, "open"))

		err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-open", "rw")
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "fail-open should permit the mint when PDP is down")
	})

	ginkgo.Context("cache + grace", func() {
		ginkgo.It("serves the second identical authorization from the cache (PDP consulted once)", func() {
			pdpAllow.Store(true)
			pdpDown.Store(false)
			// Long allow-TTL, no grace: a fresh hit on the 2nd identical request.
			setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
				pdpConfigJSONWithCache(pdpSock, "closed", "60s", "", "0s"))

			// Attach the GrainFS grant ONCE while the PDP is up. The credential-create
			// authorization is what the PDP gates (admin policy ops carry no actor on
			// the UDS, so PolicyPut/Attach are not PDP-consulted) — so the two creates
			// below produce the SAME (action,resource,principal) wire request and thus
			// the same cache key.
			attachProtocolCredentialPolicy(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-cache")

			pdpReqs.Store(0)

			err := createProtocolCredentialOnly(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-cache", "rw")
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "first authorization should be allowed by the PDP")

			err = createProtocolCredentialOnly(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-cache", "rw")
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "second authorization should be allowed (cache hit)")

			gomega.Expect(pdpReqs.Load()).To(gomega.Equal(int64(1)),
				"the second identical authorization must be served from the cache, not re-consult the PDP")
		})

		ginkgo.It("grace-serves a stale allow through a brief PDP outage", func() {
			pdpAllow.Store(true)
			pdpDown.Store(false)
			// Short allow-TTL so the entry goes stale quickly; long grace so the stale
			// entry can be grace-served once the PDP starts erroring. Fail-closed: only
			// grace (not the failure policy) can save the request during the outage.
			setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
				pdpConfigJSONWithCache(pdpSock, "closed", "1s", "", "60s"))

			attachProtocolCredentialPolicy(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-grace")

			pdpReqs.Store(0)

			// Op #1 (PDP up, allow) populates the cache.
			err := createProtocolCredentialOnly(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-grace", "rw")
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "first authorization should be allowed by the PDP")
			gomega.Expect(pdpReqs.Load()).To(gomega.Equal(int64(1)), "op #1 consults the PDP once")

			// Age the cached entry past ttl_allow (1s) but well within grace_ttl (60s).
			// The server uses real time.Now, so a real sleep makes the entry STALE.
			time.Sleep(1500 * time.Millisecond)

			// Take the PDP down: it now returns 500 to every request.
			pdpDown.Store(true)

			// Op #2 on the SAME (action,resource): stale lookup -> PDP re-consult fails
			// -> grace serves the stale allow. Fail-closed would otherwise deny.
			err = createProtocolCredentialOnly(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-grace", "rw")
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "grace must serve the stale allow through the outage")
			gomega.Expect(pdpReqs.Load()).To(gomega.Equal(int64(2)),
				"op #2 must re-consult the PDP (and fail) before grace-serving — proving the stale path, not a fresh cache hit")
		})
	})
})
