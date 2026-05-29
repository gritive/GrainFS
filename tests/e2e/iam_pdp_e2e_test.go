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

var _ = ginkgo.Describe("External PDP adapter (Slice 1)", ginkgo.Ordered, func() {
	var (
		srv      iamTestServer
		pdpSock  string
		pdpAllow atomic.Bool
		pdpDown  atomic.Bool
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
})
