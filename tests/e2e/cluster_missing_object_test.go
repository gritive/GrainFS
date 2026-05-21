package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestClusterMissingObjectE2E pins the S3 contract for anon GET/HEAD on a key
// that does not exist on the data plane, across SingleNode + Cluster3Node.
//
// Contract (single + cluster parity, per memory rule):
//   - GET    on never-existed key → 404 NoSuchKey
//   - HEAD   on never-existed key → 404 (no body)
//   - GET    after PUT+DELETE     → 404 NoSuchKey (the delete marker is a
//     not-found-for-the-latest, not a method-routing failure)
//
// Bugs this test covers:
//   - F#46           — cluster DELETE-then-GET previously returned 405
//     MethodNotAllowed instead of 404. The local-EC branch of
//     ClusterCoordinator.GetObject/HeadObject routes a versioned read at
//     the delete-marker version because the object-index entry carries the
//     marker's versionID; GetObjectVersion on a delete marker correctly
//     returns ErrMethodNotAllowed, but the unversioned caller must observe
//     ErrObjectNotFound → 404.
//   - missing-object-500 — cluster GET on a never-existed key previously
//     returned 500 "forward: no reachable peer" because no peer owned the
//     key. Absence-of-owner-for-key-that-never-existed must surface as
//     404 NoSuchKey, not as a transport "no reachable peer".
//
// Dual-target pattern per project memory:
//
//	Describe + SingleNode + Cluster3Node + shared case helper.
var _ = ginkgo.Describe("Cluster missing object", func() {
	describeClusterMissingObjectContext("SingleNode", newPhase0SingleNodeTarget)
	describeClusterMissingObjectContext("Cluster3Node", newPhase0ClusterTarget)
})

func describeClusterMissingObjectContext(name string, factory func(testing.TB) *phase0Target) {
	ginkgo.Context(name, func() {
		var tgt *phase0Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runClusterMissingObjectCases(func() *phase0Target { return tgt })
	})
}

func runClusterMissingObjectCases(getTgt func() *phase0Target) {
	ginkgo.It("returns 404 for GET on a never-existed key", func() {
		tgt := getTgt()
		key := "/default/never-existed-" + uuid.NewString() + ".txt"
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+key, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusNotFound),
			"anon GET on never-existed key must be 404 NoSuchKey (got %d body=%s)",
			resp.StatusCode, string(body))
		gomega.Expect(string(body)).To(gomega.ContainSubstring("NoSuchKey"),
			"404 body must carry NoSuchKey error code (got %s)", string(body))
	})

	ginkgo.It("returns 404 for HEAD on a never-existed key", func() {
		tgt := getTgt()
		key := "/default/never-existed-head-" + uuid.NewString() + ".txt"
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodHead, tgt.s3URL(0)+key, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close()
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusNotFound),
			"anon HEAD on never-existed key must be 404 (got %d)", resp.StatusCode)
	})

	ginkgo.It("returns 404 for GET after deleting a key", func() {
		tgt := getTgt()
		key := "/default/deleted-" + uuid.NewString() + ".txt"

		// PUT — must succeed under Phase 0 anon contract.
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+key, bytes.NewReader([]byte("transient")))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp, err := http.DefaultClient.Do(putReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = putResp.Body.Close()
		gomega.Expect(putResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"anon PUT precondition for delete test (got %d)", putResp.StatusCode)

		// DELETE — should succeed (200 or 204).
		delReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodDelete, tgt.s3URL(0)+key, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		delResp, err := http.DefaultClient.Do(delReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = delResp.Body.Close()
		gomega.Expect([]int{http.StatusOK, http.StatusNoContent}).To(gomega.ContainElement(delResp.StatusCode),
			"anon DELETE must succeed (got %d)", delResp.StatusCode)

		// GET — must observe the latest as "not found", not a 405.
		getReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+key, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		getResp, err := http.DefaultClient.Do(getReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer getResp.Body.Close()
		body, _ := io.ReadAll(getResp.Body)
		gomega.Expect(getResp.StatusCode).To(gomega.Equal(http.StatusNotFound),
			"anon GET on deleted key must be 404 NoSuchKey (got %d body=%s)",
			getResp.StatusCode, summarizeBody(body))
		gomega.Expect(string(body)).To(gomega.ContainSubstring("NoSuchKey"),
			"404 body must carry NoSuchKey error code (got %s)", summarizeBody(body))
	})
}

func summarizeBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 256 {
		return s[:256] + "..."
	}
	return fmt.Sprintf("%q", s)
}
