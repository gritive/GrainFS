package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

// TestPhaseTransitionE2E proves first-SA bootstrap does not disturb the default
// bucket's implicit anonymous policy, while non-default buckets remain denied
// unless policy explicitly allows anonymous access.
//
// Dual-target per R10: SingleNode + Cluster3Node, both Phase 0 (unbootstrapped).
//
// Each sub-case spins up its own fresh fixture because first-SA bootstrap is a
// one-way IAM state transition.
var _ = ginkgo.Describe("Phase transition", func() {
	describePhaseTransitionContext("SingleNode", "single", func(tb testing.TB) *phase0Target {
		return newPhase0SingleNodeTarget(tb)
	})
	describePhaseTransitionContext("Cluster3Node", "cluster3", func(tb testing.TB) *phase0Target {
		return newPhase0ClusterTarget(tb)
	})
})

func describePhaseTransitionContext(name, tgtName string, factory func(testing.TB) *phase0Target) {
	ginkgo.Context(name, func() {
		var tgt *phase0Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runPhaseTransitionCases(tgtName, func() *phase0Target { return tgt })
	})
}

func runPhaseTransitionCases(tgtName string, getTgt func() *phase0Target) {
	ginkgo.It("keeps default bucket anonymous access alive after the Phase 2 flip (DefaultBucketAnonSurvivesFlip)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		// Phase 0 anon PUT to /default — success.
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/default/before-flip.txt",
			bytes.NewReader([]byte("before")))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp, err := http.DefaultClient.Do(putReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = putResp.Body.Close()
		gomega.Expect(putResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon PUT to /default must succeed before flip")

		// Create the first SA. This no longer flips a global anonymous config.
		flipToPhase2(t, tgt.adminSock(0))

		// Default-bucket anonymous access is owned by the implicit bucket policy.

		// GET — reads back the Phase 0 PUT.
		getReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+"/default/before-flip.txt", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		getResp, err := http.DefaultClient.Do(getReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getResp.Body.Close)
		gomega.Expect(getResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"anon GET on /default must succeed after first SA create; status=%d",
			getResp.StatusCode)
		body, err := io.ReadAll(getResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(body).To(gomega.Equal([]byte("before")))

		// PUT — writes after first-SA bootstrap.
		putReq2, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/default/after-flip.txt",
			bytes.NewReader([]byte("after")))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		putResp2, err := http.DefaultClient.Do(putReq2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = putResp2.Body.Close()
		gomega.Expect(putResp2.StatusCode).To(gomega.Equal(http.StatusOK),
			"anon PUT on /default must succeed after first SA create; status=%d",
			putResp2.StatusCode)

		// LIST — subresource GET.
		listReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+"/default/?list-type=2", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		listResp, err := http.DefaultClient.Do(listReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(listResp.Body.Close)
		gomega.Expect(listResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"anon LIST on /default must succeed after first SA create; status=%d",
			listResp.StatusCode)
		listBody, err := io.ReadAll(listResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(string(listBody)).To(gomega.ContainSubstring("after-flip.txt"),
			"LIST response must show the post-flip anon PUT key")
	})

	ginkgo.It("denies anonymous access to non-default buckets without policy (NonDefaultBucketAnonDenied)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()

		flipToPhase2(t, tgt.adminSock(0))

		// Create a non-default bucket via admin UDS (no anon policy attached).
		bucket := bucketNameFor(tgtName, "phase-tx", "nondef")
		gomega.Expect(adminCreateBucket(t, tgt.adminSock(0), bucket)).To(gomega.Succeed(),
			"admin UDS bucket create must succeed in Phase 2")

		// Anon PUT to non-default bucket must be denied (401 or 403).
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/"+bucket+"/anon-try.txt",
			bytes.NewReader([]byte("x")))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		gomega.Expect([]int{http.StatusUnauthorized, http.StatusForbidden}).To(gomega.ContainElement(resp.StatusCode),
			"anon PUT to non-default bucket %q without policy must be denied (got %d)",
			bucket, resp.StatusCode)
	})

	ginkgo.It("does not expose torn anonymous auth state during the flip (NoTornStateDuringFlip)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		// Seed a probe object on /default so cluster GET-routing has a real
		// owner to resolve. Without this, cluster anon GETs on a never-written
		// key return 500 "forward: no reachable peer" (cluster data plane
		// behavior on missing objects — orthogonal to F#26).
		probeKey := "phase-tx-probe.txt"
		seedReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/default/"+probeKey,
			bytes.NewReader([]byte("torn-state-probe")))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		seedResp, err := http.DefaultClient.Do(seedReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = seedResp.Body.Close()
		gomega.Expect(seedResp.StatusCode).To(gomega.Equal(http.StatusOK),
			"Phase 0 anon PUT seed must succeed")

		// Pummel anon GETs against /default/<probeKey> while flipping. The
		// default-bucket guarantee says ZERO 401/403 — auth must pass throughout.
		var defaultOK, defaultDeny, defaultOther, errCount int32
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli := &http.Client{Timeout: 2 * time.Second}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				req, err := http.NewRequestWithContext(context.Background(),
					http.MethodGet, tgt.s3URL(0)+"/default/"+probeKey, nil)
				if err != nil {
					atomic.AddInt32(&errCount, 1)
					continue
				}
				resp, err := cli.Do(req)
				if err != nil {
					atomic.AddInt32(&errCount, 1)
					continue
				}
				switch resp.StatusCode {
				case http.StatusOK, http.StatusNotFound:
					atomic.AddInt32(&defaultOK, 1)
				case http.StatusUnauthorized, http.StatusForbidden:
					atomic.AddInt32(&defaultDeny, 1)
				default:
					n := atomic.AddInt32(&defaultOther, 1)
					if n <= 3 {
						body, _ := io.ReadAll(resp.Body)
						t.Logf("unexpected status %d during flip probe: body=%s",
							resp.StatusCode, string(body))
					}
				}
				_ = resp.Body.Close()
			}
		}()

		// Let some anon hits land, then create the first SA mid-flight.
		time.Sleep(200 * time.Millisecond)
		flipToPhase2(t, tgt.adminSock(0))
		wg.Wait()

		t.Logf("flip-probe counts: ok=%d deny=%d other=%d err=%d",
			atomic.LoadInt32(&defaultOK), atomic.LoadInt32(&defaultDeny),
			atomic.LoadInt32(&defaultOther), atomic.LoadInt32(&errCount))
		gomega.Expect(atomic.LoadInt32(&defaultOK)).To(gomega.BeNumerically(">", int32(0)),
			"expected at least some anon-to-/default to be allowed through during the flip window")
		gomega.Expect(atomic.LoadInt32(&defaultDeny)).To(gomega.Equal(int32(0)),
			"anon to /default MUST NEVER be denied during or after first-SA bootstrap; deny count=%d",
			atomic.LoadInt32(&defaultDeny))
	})
}

// flipToPhase2 creates the first SA via admin UDS.
// Returns the new SA's id and key pair; callers typically discard them.
func flipToPhase2(t testing.TB, sock string) (saID, ak, sk string) {
	t.Helper()
	out := iamCreateSA(t, sock, "phase-tx-"+strconv.FormatInt(time.Now().UnixNano(), 36))
	return out.SAID, out.AccessKey, out.SecretKey
}

// adminCreateBucket creates a bucket via admin UDS with no SA/policy attach.
// Anon access to the resulting bucket is governed by IAM/bucket policy.
func adminCreateBucket(t testing.TB, sock, bucket string) error {
	t.Helper()
	cli := iamadmin.NewClientForURL(sock)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return cli.BucketCreate(ctx, bucket, "", "")
}
