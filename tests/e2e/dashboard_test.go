package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// dashboardFactory mirrors volumeScrubFactory. TokenURLAndRotate rotates the
// dashboard token; isolating each case in its own fixture prevents one
// rotate from invalidating another case's expectations.
type dashboardFactory func(args ...string) s3Target

var _ = ginkgo.Describe("Dashboard", func() {
	for _, tc := range []struct {
		name string
		mk   func(t testing.TB) dashboardFactory
	}{
		{
			name: "SingleNode",
			mk: func(t testing.TB) dashboardFactory {
				return func(args ...string) s3Target {
					return newDedicatedSingleNodeS3Target(t, args)
				}
			},
		},
		{
			name: "Cluster4Node",
			mk: func(t testing.TB) dashboardFactory {
				return func(args ...string) s3Target {
					return newClusterS3TargetWithExtraArgs(t, 4, args)
				}
			},
		},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var mk dashboardFactory

			ginkgo.BeforeEach(func() {
				mk = tc.mk(ginkgo.GinkgoTB())
			})

			runDashboardCases(func() dashboardFactory { return mk })
		})
	}
})

func runDashboardCases(mk func() dashboardFactory) {
	ginkgo.It("serves the UI", func() {
		tgt := mk()()
		resp, err := http.Get(tgt.endpoint(0) + "/ui/")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)

		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
		body, _ := io.ReadAll(resp.Body)
		s := string(body)
		gomega.Expect(s).To(gomega.ContainSubstring("GrainFS"))
		gomega.Expect(s).To(gomega.ContainSubstring("<!DOCTYPE html>"))
	})

	ginkgo.It("renders healing card markup", func() {
		tgt := mk()()
		resp, err := http.Get(tgt.endpoint(0) + "/ui/")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		html := string(body)

		gomega.Expect(html).To(gomega.ContainSubstring(`id="heal-section"`), "Self-Healing card section missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`id="heal-last-when"`), "Last Heal value element missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`id="heal-rate"`), "Heal Rate element missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`id="heal-restart-count"`), "Restart Recovery line missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`id="heal-events-table"`), "Live Heal Events table missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`/api/events/heal/stream`), "heal SSE endpoint not wired in JS")
		gomega.Expect(html).To(gomega.ContainSubstring(`id="incident-section"`), "Zero-ops incident section missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`id="incident-table"`), "Incident table missing")
		gomega.Expect(html).To(gomega.ContainSubstring(`/api/incidents`), "incident API not wired in JS")
		gomega.Expect(html).To(gomega.ContainSubstring(`FD exhaustion risk`), "FD incident label missing")
	})

	ginkgo.It("streams healing events as SSE", func() {
		tgt := mk()()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ginkgo.DeferCleanup(cancel)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, tgt.endpoint(0)+"/api/events/heal/stream", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)

		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
		ct := resp.Header.Get("Content-Type")
		gomega.Expect(ct).To(gomega.HavePrefix("text/event-stream"), "expected SSE content-type, got %q", ct)
		gomega.Expect(resp.Header.Get("Cache-Control")).To(gomega.Equal("no-cache"))
	})

	ginkgo.It("rotates dashboard tokens", func() {
		t := ginkgo.GinkgoTB()
		tgt := mk()()
		dataDir := dashboardDataDir(tgt)
		port := dashboardPort(tgt, 0)

		out1, code := runCLI(t, dataDir, "dashboard", "--format", "json")
		gomega.Expect(code).To(gomega.Equal(0), out1)
		var resp1 struct {
			Token string `json:"token"`
			URL   string `json:"url"`
		}
		gomega.Expect(json.Unmarshal([]byte(out1), &resp1)).To(gomega.Succeed())
		gomega.Expect(resp1.Token).NotTo(gomega.BeEmpty())
		gomega.Expect(resp1.URL).To(gomega.ContainSubstring("#token=" + resp1.Token))

		gomega.Expect(callUI(t, port, resp1.Token)).To(gomega.Equal(http.StatusOK), "old token must work")
		gomega.Expect(callUI(t, port, "")).To(gomega.Equal(http.StatusUnauthorized), "no token must 401")

		out2, code := runCLI(t, dataDir, "dashboard", "--rotate", "--format", "json")
		gomega.Expect(code).To(gomega.Equal(0), out2)
		var resp2 struct {
			Token string `json:"token"`
		}
		gomega.Expect(json.Unmarshal([]byte(out2), &resp2)).To(gomega.Succeed())
		gomega.Expect(resp2.Token).NotTo(gomega.Equal(resp1.Token))

		gomega.Expect(callUI(t, port, resp1.Token)).To(gomega.Equal(http.StatusUnauthorized), "rotated old token must be dead")
		gomega.Expect(callUI(t, port, resp2.Token)).To(gomega.Equal(http.StatusOK), "new token must work")
	})
}

// dashboardDataDir returns the dataDir to drive the admin CLI against tgt
// (single → unique server dir; cluster → leader).
func dashboardDataDir(tgt s3Target) string {
	return strings.TrimSuffix(tgt.adminSockPath(), "/admin.sock")
}

// dashboardPort parses the HTTP port out of tgt.endpoint(nodeIdx).
func dashboardPort(tgt s3Target, nodeIdx int) int {
	ep := tgt.endpoint(nodeIdx)
	i := strings.LastIndex(ep, ":")
	if i < 0 {
		panic(fmt.Sprintf("endpoint has no port: %s", ep))
	}
	var port int
	if _, err := fmt.Sscanf(ep[i+1:], "%d", &port); err != nil {
		panic(fmt.Sprintf("parse port from %s: %v", ep, err))
	}
	return port
}

func callUI(t testing.TB, port int, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/ui/api/volumes", port), nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}
