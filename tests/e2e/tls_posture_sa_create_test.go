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
	"github.com/stretchr/testify/require"
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
	t := ginkgo.GinkgoTB()
	old, ok := os.LookupEnv(key)
	require.NoError(t, os.Setenv(key, value))
	ginkgo.DeferCleanup(func() {
		if ok {
			require.NoError(t, os.Setenv(key, old))
			return
		}
		require.NoError(t, os.Unsetenv(key))
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
		require.Equalf(t, http.StatusPreconditionFailed, status,
			"first SA create on bad-posture fixture must return 412 Precondition Failed; body=%s", body)
		// Body must mention all three remediation knobs (the
		// enforceTLSPostureValues error message is reused).
		require.Containsf(t, body, "GRAINFS_TLS_CERT",
			"error body must name the GRAINFS_TLS_CERT env var remediation; body=%s", body)
		require.Containsf(t, body, "trusted-proxy.cidr",
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
		require.NoError(t, ensureDir(certDir))
		certPath := filepath.Join(certDir, "cert.pem")
		keyPath := filepath.Join(certDir, "key.pem")
		writeSelfSignedCertE2E(t, certPath, keyPath)

		status, body := postIAMSARaw(t, tgt.adminSock(0), "admin")
		require.Containsf(t, []int{http.StatusOK, http.StatusCreated}, status,
			"SA create with cert on disk must succeed; body=%s", body)
	})

	ginkgo.It("accepts the first SA create when trusted-proxy.cidr is set", func() {
		t := ginkgo.GinkgoTB()
		tgt := newFixture(t)
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		status, body := postIAMSARaw(t, tgt.adminSock(0), "admin")
		require.Containsf(t, []int{http.StatusOK, http.StatusCreated}, status,
			"SA create with trusted-proxy.cidr set must succeed; body=%s", body)
	})

	ginkgo.It("does not block a second SA create after posture becomes unsafe", func() {
		t := ginkgo.GinkgoTB()
		tgt := newFixture(t)
		// Use the trusted-proxy.cidr knob to let the first SA through.
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		status, body := postIAMSARaw(t, tgt.adminSock(0), "first-"+strconv.FormatInt(time.Now().UnixNano(), 36))
		require.Containsf(t, []int{http.StatusOK, http.StatusCreated}, status,
			"first SA create with trusted-proxy.cidr must succeed; body=%s", body)

		// Clear trusted-proxy.cidr so the local posture is now "bad". A second
		// SA create must still succeed — the pre-check fires only on an empty
		// store.
		unsetTrustedProxy(t, tgt.adminSock(0))
		require.Eventually(t, func() bool {
			return getTrustedProxy(t, tgt.adminSock(0)) == ""
		}, 2*time.Second, 50*time.Millisecond,
			"trusted-proxy.cidr must clear before second SA create probe")

		status, body = postIAMSARaw(t, tgt.adminSock(0), "second-"+strconv.FormatInt(time.Now().UnixNano(), 36))
		require.Containsf(t, []int{http.StatusOK, http.StatusCreated}, status,
			"second SA create must succeed even with bad posture (pre-check is empty-store-only); body=%s", body)
	})
}

// postIAMSARaw POSTs /v1/iam/sa over the admin UDS and returns (status, body).
// Unlike iamCreateSA / iamDo it does NOT fatal on non-2xx — the test inspects
// the status explicitly.
func postIAMSARaw(t testing.TB, sock, name string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"name": name})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost, "http://unix/v1/iam/sa", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := iamUDSClient(sock).Do(req)
	require.NoError(t, err)
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
	require.NoError(t, err)
	resp, err := iamUDSClient(sock).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Truef(t,
		resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent,
		"DELETE /v1/config/trusted-proxy.cidr → %d", resp.StatusCode)
}

// getTrustedProxy reads trusted-proxy.cidr and returns its raw string value.
func getTrustedProxy(t testing.TB, sock string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodGet, "http://unix/v1/config/trusted-proxy.cidr", nil)
	require.NoError(t, err)
	resp, err := iamUDSClient(sock).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ""
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	var entry struct {
		Value string `json:"value"`
	}
	require.NoError(t, json.Unmarshal(body, &entry))
	return entry.Value
}
