package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

// TestPhaseTransitionE2E proves F#26: when the first SA is created on a Phase 0
// fixture, iam.anon-enabled atomically flips to false (Phase 2). Anon traffic
// to s3://default MUST keep working across the flip (the default bucket carries
// an implicit anon-allow that survives), while anon traffic to other buckets
// is denied after the flip. No torn intermediate state is observable.
//
// Dual-target per R10: SingleNode + Cluster3Node, both Phase 0 (unbootstrapped).
//
// Each sub-case spins up its own fresh Phase 0 fixture via newFixture(t) because
// the first-SA-create flip is one-way (anon-enabled can't roll back through the
// same hook), and every case needs a fresh Phase 0 start.
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
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		// Phase 0 anon PUT to /default — success.
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/default/before-flip.txt",
			bytes.NewReader([]byte("before")))
		require.NoError(t, err)
		putResp, err := http.DefaultClient.Do(putReq)
		require.NoError(t, err)
		_ = putResp.Body.Close()
		require.Equal(t, http.StatusOK, putResp.StatusCode,
			"Phase 0 anon PUT to /default must succeed before flip")

		// Trigger Phase 2 flip by creating the first SA.
		flipToPhase2(t, tgt.adminSock(0))

		// Wait for iam.anon-enabled to read false. Single-node flips on the
		// same apply round; cluster needs Raft propagation to the queried node.
		require.Eventually(t, func() bool {
			return !isAnonEnabled(t, tgt.adminSock(0))
		}, 2*time.Second, 50*time.Millisecond,
			"iam.anon-enabled must flip to false after first SA create")

		// F#41-extension: default-bucket anon access is independent of
		// iam.anon-enabled (banner guarantee — "default remains public"). All
		// of GET/PUT/LIST on /default must survive the flip. The middleware
		// anon fast-path in authn_middleware.go pipes anon requests through to
		// the authorizer when bucket=="default", and the authorizer's D#2
		// implicit-anon path (authorizer.go:73-81) returns Allow with reason
		// ReasonDefaultBucketImplicitAnon.

		// GET — reads back the Phase 0 PUT.
		getReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+"/default/before-flip.txt", nil)
		require.NoError(t, err)
		getResp, err := http.DefaultClient.Do(getReq)
		require.NoError(t, err)
		defer getResp.Body.Close()
		require.Equalf(t, http.StatusOK, getResp.StatusCode,
			"Phase 2 anon GET on /default must succeed (banner promise); status=%d",
			getResp.StatusCode)
		body, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		require.Equal(t, []byte("before"), body)

		// PUT — writes after the flip. F#41-ext core assertion.
		putReq2, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/default/after-flip.txt",
			bytes.NewReader([]byte("after")))
		require.NoError(t, err)
		putResp2, err := http.DefaultClient.Do(putReq2)
		require.NoError(t, err)
		_ = putResp2.Body.Close()
		require.Equalf(t, http.StatusOK, putResp2.StatusCode,
			"Phase 2 anon PUT on /default must succeed (banner: 'default remains public'); status=%d",
			putResp2.StatusCode)

		// LIST — subresource GET. Must also pass authn in Phase 2.
		listReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+"/default/?list-type=2", nil)
		require.NoError(t, err)
		listResp, err := http.DefaultClient.Do(listReq)
		require.NoError(t, err)
		defer listResp.Body.Close()
		require.Equalf(t, http.StatusOK, listResp.StatusCode,
			"Phase 2 anon LIST on /default must succeed (banner: 'default remains public'); status=%d",
			listResp.StatusCode)
		listBody, err := io.ReadAll(listResp.Body)
		require.NoError(t, err)
		require.Contains(t, string(listBody), "after-flip.txt",
			"LIST response must show the post-flip anon PUT key")
	})

	ginkgo.It("denies anonymous access to non-default buckets after the Phase 2 flip (NonDefaultBucketAnonDeniedAfterFlip)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		// Trigger Phase 2 via first SA create.
		flipToPhase2(t, tgt.adminSock(0))
		require.Eventually(t, func() bool {
			return !isAnonEnabled(t, tgt.adminSock(0))
		}, 2*time.Second, 50*time.Millisecond,
			"iam.anon-enabled must flip to false after first SA create")

		// Create a non-default bucket via admin UDS (no anon policy attached).
		bucket := bucketNameFor(tgtName, "phase-tx", "nondef")
		require.NoError(t, adminCreateBucket(t, tgt.adminSock(0), bucket),
			"admin UDS bucket create must succeed in Phase 2")

		// Anon PUT to non-default bucket must be denied (401 or 403).
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/"+bucket+"/anon-try.txt",
			bytes.NewReader([]byte("x")))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Containsf(t, []int{http.StatusUnauthorized, http.StatusForbidden},
			resp.StatusCode,
			"anon PUT to non-default bucket %q in Phase 2 must be denied (got %d)",
			bucket, resp.StatusCode)
	})

	ginkgo.It("does not expose torn anonymous auth state during the flip (NoTornStateDuringFlip)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		seedTrustedProxyForFlip(t, tgt.adminSock(0))

		// Seed a probe object on /default so cluster GET-routing has a real
		// owner to resolve. Without this, cluster anon GETs on a never-written
		// key return 500 "forward: no reachable peer" (cluster data plane
		// behavior on missing objects — orthogonal to F#26).
		probeKey := "phase-tx-probe.txt"
		seedReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+"/default/"+probeKey,
			bytes.NewReader([]byte("torn-state-probe")))
		require.NoError(t, err)
		seedResp, err := http.DefaultClient.Do(seedReq)
		require.NoError(t, err)
		_ = seedResp.Body.Close()
		require.Equal(t, http.StatusOK, seedResp.StatusCode,
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

		// Let some Phase 0 anon hits land, then flip mid-flight.
		time.Sleep(200 * time.Millisecond)
		flipToPhase2(t, tgt.adminSock(0))
		wg.Wait()

		t.Logf("flip-probe counts: ok=%d deny=%d other=%d err=%d",
			atomic.LoadInt32(&defaultOK), atomic.LoadInt32(&defaultDeny),
			atomic.LoadInt32(&defaultOther), atomic.LoadInt32(&errCount))
		require.Greater(t, atomic.LoadInt32(&defaultOK), int32(0),
			"expected at least some anon-to-/default to be allowed through during the flip window")
		require.Equalf(t, int32(0), atomic.LoadInt32(&defaultDeny),
			"anon to /default MUST NEVER be denied during or after the Phase 0→2 flip (F#26 atomic guarantee); deny count=%d",
			atomic.LoadInt32(&defaultDeny))
	})
}

// seedTrustedProxyForFlip sets trusted-proxy.cidr to a benign loopback value
// so the TLS posture reload hook accepts the subsequent iam.anon-enabled→false
// flip triggered by first-SA-create. Without this, the FSM's auto-flip in
// MetaCmdTypeIAMSACreate logs a warning and silently keeps anon enabled (see
// meta_fsm.go applyIAMSACreate and serveruntime/tls_posture.go
// reloadHookPostureCheck).
//
// Defensive finding (flag only — out of scope for T73): F#26's atomic flip
// is silently dropped on first-SA-create when TLS posture is unsafe. The
// Phase 0 → Phase 2 "magical moment" has an unstated precondition (TLS cert OR
// trusted-proxy.cidr must be set).
func seedTrustedProxyForFlip(t testing.TB, sock string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"value": "127.0.0.1/32"})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPut, "http://unix/v1/config/trusted-proxy.cidr",
		bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := iamUDSClient(sock).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Truef(t,
		resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent,
		"PUT /v1/config/trusted-proxy.cidr → %d", resp.StatusCode)
}

// flipToPhase2 creates the first SA via admin UDS, which triggers the
// Phase 0 → Phase 2 atomic flip server-side (iam.anon-enabled set to false).
// Returns the new SA's id and key pair; callers typically discard them.
func flipToPhase2(t testing.TB, sock string) (saID, ak, sk string) {
	t.Helper()
	out := iamCreateSA(t, sock, "phase-tx-"+strconv.FormatInt(time.Now().UnixNano(), 36))
	return out.SAID, out.AccessKey, out.SecretKey
}

// isAnonEnabled reads iam.anon-enabled via admin UDS GET /v1/config/<key>
// and returns the parsed bool value. Fatals on transport/decode errors.
func isAnonEnabled(t testing.TB, sock string) bool {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodGet, "http://unix/v1/config/iam.anon-enabled", nil)
	require.NoError(t, err)
	resp, err := iamUDSClient(sock).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"GET /v1/config/iam.anon-enabled → %d", resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var entry struct {
		Value string `json:"value"`
	}
	require.NoError(t, json.Unmarshal(body, &entry))
	v, err := strconv.ParseBool(entry.Value)
	require.NoErrorf(t, err, "parse iam.anon-enabled value %q", entry.Value)
	return v
}

// adminCreateBucket creates a bucket via admin UDS with no SA/policy attach.
// Anon access to the resulting bucket is then governed solely by the global
// iam.anon-enabled config.
func adminCreateBucket(t testing.TB, sock, bucket string) error {
	t.Helper()
	cli := iamadmin.NewClientForURL(sock)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return cli.BucketCreate(ctx, bucket, "", "")
}
