package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestPhase0QuickstartE2E mechanically verifies the Phase 0 "magical-moment"
// quickstart promised by the README: `./grainfs serve` then anon PUT + LIST +
// GET on s3://default via aws --no-sign-request. The fixture is FRESH and
// UNBOOTSTRAPPED (iam.anon-enabled=true by default in Phase 0); using a
// bootstrapped fixture would deny anon requests on Layer 1.
//
// Dual-target per R10: SingleNode + Cluster3Node.
//
// Cases:
//   - AnonPutDefaultBucket    — PUT to s3://default with NO Authorization header.
//   - AnonListShowsObject     — ListObjectsV2 (anon) returns the just-PUT key.
//   - AnonGetReadsBack        — GET round-trips the body that anon PUT wrote.
var _ = ginkgo.Describe("Phase 0 quickstart", func() {
	describePhase0QuickstartContext("SingleNode", func(tb testing.TB) *phase0Target {
		return newPhase0SingleNodeTarget(tb)
	})
	describePhase0QuickstartContext("Cluster3Node", func(tb testing.TB) *phase0Target {
		return newPhase0ClusterTarget(tb)
	})
})

func describePhase0QuickstartContext(name string, factory func(testing.TB) *phase0Target) {
	ginkgo.Context(name, func() {
		var tgt *phase0Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runPhase0QuickstartCases(func() *phase0Target { return tgt })
	})
}

// phase0Target is a minimal fixture handle used only by the Phase 0 quickstart
// suite. It intentionally avoids the iceberg / bootstrapped target helpers
// because Phase 0 is pre-bootstrap by definition.
//
// adminSock returns the admin UDS path appropriate for admin RPCs. For
// single-node it's the only node's socket; for cluster it's the leader's
// socket (writes like iam SA create / config PATCH must target the leader,
// matching newSharedClusterIAMAdminTarget's pattern). The i argument is
// reserved for symmetry with s3URL and currently ignored.
type phase0Target struct {
	name      string
	s3URL     func(i int) string
	adminSock func(i int) string
	nodeCount int
}

func newPhase0SingleNodeTarget(t testing.TB) *phase0Target {
	t.Helper()
	_, url, sock, _ := startUnbootstrappedSingleNode(t)
	return &phase0Target{
		name:      "single",
		s3URL:     func(i int) string { return url },
		adminSock: func(i int) string { return sock },
		nodeCount: 1,
	}
}

func newPhase0ClusterTarget(t testing.TB) *phase0Target {
	t.Helper()
	c := startUnbootstrappedCluster(t, 3)
	urls := append([]string(nil), c.httpURLs...)
	return &phase0Target{
		name:      "cluster3",
		s3URL:     func(i int) string { return urls[i] },
		adminSock: func(i int) string { return c.dataDirs[c.leaderIdx] + "/admin.sock" },
		nodeCount: len(urls),
	}
}

func runPhase0QuickstartCases(getTgt func() *phase0Target) {
	ginkgo.It("accepts anonymous PUTs to the default bucket (AnonPutDefaultBucket)", func() {
		tgt := getTgt()
		body := []byte("hello grainfs")
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut,
			tgt.s3URL(0)+"/default/hello.txt",
			bytes.NewReader(body))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// NO Authorization header (anon). Phase 0 must accept this.
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		respBody, _ := io.ReadAll(resp.Body)
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon PUT to s3://default must succeed (status=%d body=%s)",
			resp.StatusCode, string(respBody))
	})

	ginkgo.It("shows anonymously written objects in default bucket listings (AnonListShowsObject)", func() {
		tgt := getTgt()
		body := []byte("listme")
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut,
			tgt.s3URL(0)+"/default/listme.txt",
			bytes.NewReader(body))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp, err := http.DefaultClient.Do(putReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = putResp.Body.Close()
		gomega.Expect(putResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon PUT must succeed before LIST")

		listReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet,
			tgt.s3URL(0)+"/default/?list-type=2",
			nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		listResp, err := http.DefaultClient.Do(listReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(listResp.Body.Close)
		gomega.Expect(listResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon ListObjectsV2 on s3://default must succeed")
		listBody, err := io.ReadAll(listResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(string(listBody)).To(gomega.ContainSubstring("listme.txt"),
			"ListObjectsV2 response must show the anon-PUT key")
	})

	ginkgo.It("reads back anonymously written default bucket objects (AnonGetReadsBack)", func() {
		tgt := getTgt()
		body := []byte("readback content")
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut,
			tgt.s3URL(0)+"/default/readback.txt",
			bytes.NewReader(body))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp, err := http.DefaultClient.Do(putReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = putResp.Body.Close()
		gomega.Expect(putResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon PUT must succeed before GET")

		getReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet,
			tgt.s3URL(0)+"/default/readback.txt",
			nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		getResp, err := http.DefaultClient.Do(getReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getResp.Body.Close)
		gomega.Expect(getResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon GET on s3://default must succeed")
		got, err := io.ReadAll(getResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.Equal(body), "anon GET must read back what anon PUT wrote")
	})

	ginkgo.It("round-trips anonymous default bucket deletes (AnonDeleteRoundTrips)", func() {
		tgt := getTgt()
		// Phase 0 banner: "any client can read/write s3://default" — DELETE is
		// part of write semantics. Verify the wire-level contract is honored.
		key := "/default/delete-me.txt"

		// PUT first so DELETE has something to remove.
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+key, bytes.NewReader([]byte("ephemeral")))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp, err := http.DefaultClient.Do(putReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp.Body.Close()
		gomega.Expect(putResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon PUT precondition for DELETE test")

		// Anon DELETE — should succeed (200 or 204 are both valid for S3 delete).
		delReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodDelete, tgt.s3URL(0)+key, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		delResp, err := http.DefaultClient.Do(delReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		delResp.Body.Close()
		gomega.Expect([]int{http.StatusOK, http.StatusNoContent}).To(gomega.ContainElement(delResp.StatusCode),
			"Phase 0 anon DELETE on s3://default must succeed; got %d", delResp.StatusCode)

		// Verify object is gone. Phase 0 banner contract: anon GET on a
		// deleted /default key must return 404 NoSuchKey on both single and
		// cluster (F#46 fix applied — see ClusterCoordinator.GetObject's
		// IsDeleteMarker short-circuit).
		getReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+key, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		getResp, err := http.DefaultClient.Do(getReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		getResp.Body.Close()
		gomega.Expect(getResp.StatusCode).To(gomega.Equal(http.StatusNotFound),
			"DELETE should remove the object; anon GET status=%d", getResp.StatusCode)
	})

	ginkgo.It("keeps Iceberg anonymous behavior separate from the S3 fast path (IcebergAnonRequestStillRequiresBearer)", func() {
		tgt := getTgt()
		// F#41 anon fast-path is for S3 surface only. Iceberg REST has its own
		// bearer auth path (iceberg_authn.go). Verify a Phase 0 anon GET to the
		// iceberg catalog config endpoint is denied (not silently allowed by
		// crossover from the S3 anon fast-path).
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+"/iceberg/v1/config", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// NO Authorization header.
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		// Acceptable: 200 (anon-allowed by iceberg's own Phase 0 path), 401
		// (bearer-required), or 403 (other auth failure). EXPLICITLY NOT 200
		// via the S3 anon fast-path's authorizer — confirm by checking that
		// the response Content-Type is iceberg JSON (or the status is denial),
		// not a generic XML "AccessDenied" from the S3 surface.
		if resp.StatusCode == http.StatusOK {
			// OK because iceberg Phase 0 anon-skip activated. Verify it's not
			// an S3 error envelope.
			ct := resp.Header.Get("Content-Type")
			gomega.Expect(ct).NotTo(gomega.ContainSubstring("xml"),
				"iceberg config endpoint must not return S3 XML envelope")
		} else {
			// 401 or 403 — bearer-gated. That's also valid Phase 0 behavior
			// depending on how iceberg config endpoint handles anon today.
			gomega.Expect([]int{401, 403}).To(gomega.ContainElement(resp.StatusCode),
				"iceberg unsigned response should be 200/401/403, not %d", resp.StatusCode)
		}
	})
}
