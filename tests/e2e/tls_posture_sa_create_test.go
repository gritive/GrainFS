// FU#3 / F#26-tls-posture: e2e coverage for the admin UDS pre-check that
// rejects the first SA create when the local TLS posture is unsafe.
//
// The bug: on a fresh Phase 0 cluster with no TLS cert and no trusted-proxy.cidr,
// the FSM applies the SACreate AND tries to atomically flip iam.anon-enabled
// to false. The flip's reload hook refuses on bad posture, the FSM logs a
// warning and continues — leaving "SA committed, anon still on". The fix
// rejects the admin UDS RPC up front with a "precondition" error containing
// a remediation hint, before propose runs.
//
// Dual-target per R10: SingleNode + Cluster3Node. Cluster only exercises the
// rejection case (the admin UDS path is local; full duplication is unnecessary).
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TLS posture SA create proves that the admin UDS rejects the first SA create
// when the node-local TLS posture would refuse the implied anon flip, and
// accepts it once one of the three remediation knobs is in place.
var _ = ginkgo.Describe("TLS posture SA create", func() {
	ginkgo.BeforeEach(func() {
		// Neutralize any host-level GRAINFS_TLS_CERT/KEY that would silently
		// relax the "no cert" precondition by pointing at an unrelated file.
		setEnvForSpec("GRAINFS_TLS_CERT", "")
		setEnvForSpec("GRAINFS_TLS_KEY", "")
	})

	describeTLSPostureSACreateContext("SingleNode", "single", newPhase0SingleNodeTarget)
	describeTLSPostureSACreateContext("Cluster3Node", "cluster3", newPhase0ClusterTarget)
})

func setEnvForSpec(key, value string) {
	old, ok := os.LookupEnv(key)
	gomega.Expect(os.Setenv(key, value)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		if ok {
			gomega.Expect(os.Setenv(key, old)).To(gomega.Succeed())
			return
		}
		gomega.Expect(os.Unsetenv(key)).To(gomega.Succeed())
	})
}

func describeTLSPostureSACreateContext(name, tgtName string, factory func(testing.TB) *phase0Target) {
	ginkgo.Context(name, func() {
		runTLSPostureSACreateCases(tgtName, factory)
	})
}

func runTLSPostureSACreateCases(tgtName string, newFixture func(testing.TB) *phase0Target) {
	ginkgo.It("rejects the first SA create without cert or trusted proxy", func() {
		t := ginkgo.GinkgoTB()
		tgt := newFixture(t)

		status, body := postIAMSARaw(t, tgt.adminSock(0), "admin")
		gomega.Expect(status).To(gomega.Equal(http.StatusPreconditionFailed),
			"first SA create on bad-posture fixture must return 412 Precondition Failed; body=%s", body)
		// Body must mention all three remediation knobs (the
		// enforceTLSPostureValues error message is reused).
		gomega.Expect(body).To(gomega.ContainSubstring("GRAINFS_TLS_CERT"),
			"error body must name the GRAINFS_TLS_CERT env var remediation; body=%s", body)
		gomega.Expect(body).To(gomega.ContainSubstring("trusted-proxy.cidr"),
			"error body must name the trusted-proxy.cidr remediation; body=%s", body)
	})

	// Cluster only exercises the rejection path — admin UDS is per-node, parity
	// is in the rejection check itself, not in the success knobs.
	if tgtName == "cluster3" {
		return
	}

	ginkgo.It("accepts the first SA create when a TLS cert exists", func() {
		t := ginkgo.GinkgoTB()
		tgt := newFixture(t)
		// Drop a self-signed cert at the default TLS path. The posture gate
		// only needs the file to exist (it doesn't read or validate the cert
		// at SA-create time).
		certDir := filepath.Join(dataDirFor(t, tgt.adminSock(0)), "tls")
		gomega.Expect(ensureDir(certDir)).To(gomega.Succeed())
		certPath := filepath.Join(certDir, "cert.pem")
		keyPath := filepath.Join(certDir, "key.pem")
		writeSelfSignedCertE2E(t, certPath, keyPath)

		status, body := postIAMSARaw(t, tgt.adminSock(0), "admin")
		gomega.Expect([]int{http.StatusOK, http.StatusCreated}).To(gomega.ContainElement(status),
			"SA create with cert on disk must succeed; body=%s", body)
	})

	ginkgo.It("accepts the first SA create when trusted-proxy.cidr is set", func() {
		t := ginkgo.GinkgoTB()
		tgt := newFixture(t)
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		status, body := postIAMSARaw(t, tgt.adminSock(0), "admin")
		gomega.Expect([]int{http.StatusOK, http.StatusCreated}).To(gomega.ContainElement(status),
			"SA create with trusted-proxy.cidr set must succeed; body=%s", body)
	})

	ginkgo.It("does not block a second SA create after posture becomes unsafe", func() {
		t := ginkgo.GinkgoTB()
		tgt := newFixture(t)
		// Use the trusted-proxy.cidr knob to let the first SA through.
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		status, body := postIAMSARaw(t, tgt.adminSock(0), "first-"+strconv.FormatInt(time.Now().UnixNano(), 36))
		gomega.Expect([]int{http.StatusOK, http.StatusCreated}).To(gomega.ContainElement(status),
			"first SA create with trusted-proxy.cidr must succeed; body=%s", body)

		// Clear trusted-proxy.cidr so the local posture is now "bad". A second
		// SA create must still succeed — the pre-check fires only on an empty
		// store.
		unsetTrustedProxy(t, tgt.adminSock(0))
		gomega.Eventually(func() bool {
			return getTrustedProxy(t, tgt.adminSock(0)) == ""
		}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(gomega.BeTrue(),
			"trusted-proxy.cidr must clear before second SA create probe")

		status, body = postIAMSARaw(t, tgt.adminSock(0), "second-"+strconv.FormatInt(time.Now().UnixNano(), 36))
		gomega.Expect([]int{http.StatusOK, http.StatusCreated}).To(gomega.ContainElement(status),
			"second SA create must succeed even with bad posture (pre-check is empty-store-only); body=%s", body)
	})
}

// postIAMSARaw POSTs /v1/iam/sa over the admin UDS and returns (status, body).
// Unlike iamCreateSA / iamDo it does NOT fatal on non-2xx — the test inspects
// the status explicitly.
func postIAMSARaw(t testing.TB, sock, name string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"name": name})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost, "http://unix/v1/iam/sa", bytes.NewReader(body))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := iamUDSClient(sock).Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respBody)
}

// dataDirFor extracts the dataDir from an admin.sock path. The admin socket
// lives at <dataDir>/admin.sock so the parent dir IS the dataDir.
func dataDirFor(t testing.TB, sock string) string {
	t.Helper()
	return filepath.Dir(sock)
}

// ensureDir creates dir with default mode if missing. Wraps os.MkdirAll so
// existing dirs are not an error.
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	return nil
}

// unsetTrustedProxy clears trusted-proxy.cidr via admin UDS DELETE.
func unsetTrustedProxy(t testing.TB, sock string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodDelete, "http://unix/v1/config/trusted-proxy.cidr", nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	resp, err := iamUDSClient(sock).Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	gomega.Expect(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent).To(gomega.BeTrue(),
		"DELETE /v1/config/trusted-proxy.cidr → %d", resp.StatusCode)
}

// getTrustedProxy reads trusted-proxy.cidr and returns its raw string value.
func getTrustedProxy(t testing.TB, sock string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodGet, "http://unix/v1/config/trusted-proxy.cidr", nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	resp, err := iamUDSClient(sock).Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ""
	}
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	body, _ := io.ReadAll(resp.Body)
	var entry struct {
		Value string `json:"value"`
	}
	gomega.Expect(json.Unmarshal(body, &entry)).To(gomega.Succeed())
	return entry.Value
}
