package e2e

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// pdpRemoteConfigDoc builds an iam.pdp config document for an https remote PDP
// with a pinned CA and (optionally) ssrf.allow_private. The PEM carries literal
// newlines, so the document is assembled via a struct + json.Marshal rather than
// fmt.Sprintf — embedding the PEM into a raw string literal would be invalid JSON.
func pdpRemoteConfigDoc(endpoint, policy, caPEM string, allowPrivate bool) string {
	type rawTLS struct {
		CAPEM string `json:"ca_pem,omitempty"`
	}
	type rawSSRF struct {
		AllowPrivate bool `json:"allow_private,omitempty"`
	}
	type rawCfg struct {
		Enabled       bool     `json:"enabled"`
		Endpoint      string   `json:"endpoint"`
		FailurePolicy string   `json:"failure_policy"`
		TLS           *rawTLS  `json:"tls,omitempty"`
		SSRF          *rawSSRF `json:"ssrf,omitempty"`
	}
	doc := rawCfg{Enabled: true, Endpoint: endpoint, FailurePolicy: policy}
	if caPEM != "" {
		doc.TLS = &rawTLS{CAPEM: caPEM}
	}
	if allowPrivate {
		doc.SSRF = &rawSSRF{AllowPrivate: true}
	}
	out, err := json.Marshal(doc)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "marshal iam.pdp remote config doc")
	return string(out)
}

// setPDPToken sets the External-PDP bearer token via the admin UDS
// (POST /v1/iam/pdp/token). The server seals it under the cluster-consistent IAM
// DEK path; the decorator attaches it as "Authorization: Bearer <token>" on
// https requests only.
func setPDPToken(adminSock, token string) {
	tp, err := adminapi.NewTransport(adminSock)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create admin transport for pdp token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gomega.Expect(tp.Post(ctx, "/v1/iam/pdp/token", adminapi.PDPSetTokenReq{Token: token}, nil)).
		To(gomega.Succeed(), "POST /v1/iam/pdp/token")
}

var _ = ginkgo.Describe("External PDP adapter (remote https + token + SSRF)", ginkgo.Ordered, func() {
	var (
		srv      iamTestServer
		caPEM    string
		httpsURL string
		pdpAllow atomic.Bool
		pdpDown  atomic.Bool
		lastAuth atomic.Value // string: last observed Authorization header
	)

	const bearer = "s3cr3t-pdp-token"

	ginkgo.BeforeAll(func() {
		// Mock PDP over TLS (httptest.NewTLSServer binds 127.0.0.1). The handler
		// records the inbound Authorization header so a spec can prove the bearer
		// token reached the PDP.
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lastAuth.Store(r.Header.Get("Authorization"))
			if pdpDown.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			dec := "deny"
			if pdpAllow.Load() {
				dec = "allow"
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"decision": dec, "reason": "e2e-tls"})
		})
		mock := httptest.NewTLSServer(h)
		ginkgo.DeferCleanup(mock.Close)
		httpsURL = mock.URL

		// PEM-encode the server's self-signed cert for ca_pem pinning.
		cert := mock.Certificate()
		gomega.Expect(cert).NotTo(gomega.BeNil(), "httptest TLS server certificate")
		caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))

		srv = startIAMTestServer(ginkgo.GinkgoTB())
		ginkgo.DeferCleanup(srv.Stop)
	})

	ginkgo.Context("https + pinned ca_pem + bearer token", func() {
		ginkgo.It("attaches the bearer token the PDP observes, and honors allow/deny", func() {
			// allow_private is REQUIRED: httptest TLS binds loopback, which the https
			// SSRF filter blocks unless allow_private relaxes it.
			setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
				pdpRemoteConfigDoc(httpsURL, "closed", caPEM, true))
			setPDPToken(srv.AdminSock, bearer)

			// PDP allows: the mint succeeds AND the mock observed the bearer token.
			pdpAllow.Store(true)
			pdpDown.Store(false)
			lastAuth.Store("")

			err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-tls-allow", "rw")
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PDP allow over TLS should permit the mint")
			gomega.Expect(lastAuth.Load()).To(gomega.Equal("Bearer "+bearer),
				"the PDP must observe the bearer token on the https request")
		})

		ginkgo.It("deny-override still holds over TLS (PDP deny blocks the mint)", func() {
			setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
				pdpRemoteConfigDoc(httpsURL, "closed", caPEM, true))
			setPDPToken(srv.AdminSock, bearer)
			pdpAllow.Store(false)
			pdpDown.Store(false)

			err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-tls-deny", "rw")
			gomega.Expect(err).To(gomega.HaveOccurred(), "PDP deny over TLS should block the mint")
			gomega.Expect(err.Error()).To(gomega.ContainSubstring("denied by external policy"))
		})

		ginkgo.It("fail-open serves the GrainFS allow through a TLS PDP outage", func() {
			setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
				pdpRemoteConfigDoc(httpsURL, "open", caPEM, true))
			setPDPToken(srv.AdminSock, bearer)
			pdpAllow.Store(false)
			pdpDown.Store(true)

			err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-tls-open", "rw")
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "fail-open should permit the mint when the TLS PDP is down")
		})
	})

	ginkgo.Context("SSRF hard-deny", func() {
		ginkgo.It("hard-denies even under failure_policy=open when the dial is SSRF-blocked", func() {
			// https://localhost:<port> passes ParseConfig (localhost is a plausible
			// DNS name, validated only at dial time), then the Dialer.Control hook
			// resolves it to loopback and rejects it as an SSRF egress violation —
			// WITHOUT allow_private. A SSRF-blocked dial is a HARD DENY that must NOT
			// fall through to fail-open. No server need listen on the port: Control
			// fires before connect.
			ssrfEndpoint := fmt.Sprintf("https://localhost:%d", freePort())
			setPDPConfig(ginkgo.GinkgoTB(), srv.AdminSock,
				pdpRemoteConfigDoc(ssrfEndpoint, "open", "", false))

			// Attach the GrainFS grant first (tryCreateProtocolCredential does this)
			// so GrainFS-allow lets the request reach the PDP dial — otherwise the
			// GrainFS-deny short-circuit would mask the SSRF path.
			err := tryCreateProtocolCredential(ginkgo.GinkgoTB(), srv.AdminSock, srv.BootstrapSAID, "s3", "bucket/pdp-ssrf", "rw")
			gomega.Expect(err).To(gomega.HaveOccurred(),
				"SSRF-blocked dial must HARD-DENY despite failure_policy=open")
			gomega.Expect(err.Error()).To(gomega.ContainSubstring("pdp_unavailable"),
				"SSRF hard-deny surfaces the fail-closed reason, not a fail-open allow")
		})
	})

	// TODO(pdp): cross-node DEK-parity e2e. Setting the bearer token on node A and
	// proving node B unseals it (via Raft-replicated DEK) is deferred: tests/e2e
	// has no ready multi-node harness that bootstraps IAM + exposes per-node admin
	// sockets (newSharedClusterS3Target yields only an s3Target). DEK cross-node
	// replication is already verified in code + unit tests; building a full cluster
	// IAM/PDP harness here is out of scope for this slice.
})
