package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/onsi/gomega"
)

// lifecycleFixture drives the in-process lifecycle worker(s) deterministically
// from an e2e test. The test process cannot reach the spawned binary's
// in-process *lifecycle.Service directly, so the fixture POSTs SigV4-signed
// requests to the test-control endpoints (registered in
// internal/server/lifecycle_testctl_api.go). On single-node fixtures the
// single URL is the leader; on cluster fixtures every node is poked because
// only the leader's object-side worker is loaded (followers no-op naturally),
// while every node's MPU worker is per-node and always loaded.
type lifecycleFixture struct {
	t        testing.TB
	urls     []string
	signer   *v4.Signer
	creds    aws.Credentials
	now      time.Time
	startSet bool
}

// newLifecycleFixture seeds the fixture at the current real-time clock and
// captures the list of node URLs from tgt. The caller is responsible for
// boot-time admin grants — the fixture only signs and POSTs.
func newLifecycleFixture(t testing.TB, tgt s3Target) *lifecycleFixture {
	t.Helper()
	urls := make([]string, 0, tgt.nodes)
	for i := 0; i < tgt.nodes; i++ {
		urls = append(urls, tgt.endpoint(i))
	}
	return &lifecycleFixture{
		t:    t,
		urls: urls,
		signer: v4.NewSigner(func(o *v4.SignerOptions) {
			o.DisableURIPathEscaping = true
		}),
		creds: aws.Credentials{AccessKeyID: tgt.accessKey, SecretAccessKey: tgt.secretKey},
		now:   time.Now(),
	}
}

// AdvanceLifecycleClock moves the fixture's deterministic clock forward by d
// and pushes the new value to every captured node. The next RunLifecycleCycle
// observes the advanced time.
func (f *lifecycleFixture) AdvanceLifecycleClock(d time.Duration) {
	f.t.Helper()
	f.now = f.now.Add(d)
	f.startSet = true
	payload, err := json.Marshal(map[string]int64{"unix_nano": f.now.UnixNano()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, url := range f.urls {
		f.postSigned(url+routePathLifecycleTestSetNow, payload)
	}
}

// ResetClock returns the fixture's clock to real-time now and pushes that value
// to the server-side lifecycle worker on every captured node. Use in per-spec
// setup when a shared fixture is reused across specs (Ordered Container +
// BeforeAll pattern) to avoid cumulative clock drift polluting later specs.
func (f *lifecycleFixture) ResetClock() {
	f.t.Helper()
	f.now = time.Now()
	f.startSet = true
	payload, err := json.Marshal(map[string]int64{"unix_nano": f.now.UnixNano()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, url := range f.urls {
		f.postSigned(url+routePathLifecycleTestSetNow, payload)
	}
}

// RunLifecycleCycle synchronously runs one object-side + MPU cycle on every
// captured node. Followers without a leader-side worker no-op naturally
// (Service.RunCycleForTest is nil-safe on the worker handle).
func (f *lifecycleFixture) RunLifecycleCycle(ctx context.Context) {
	_ = ctx // SigV4 path uses its own context
	f.t.Helper()
	// If AdvanceLifecycleClock was not called, push the current fixture clock
	// once so SetNowForTest is applied on at least one cycle.
	if !f.startSet {
		f.AdvanceLifecycleClock(0)
	}
	for _, url := range f.urls {
		f.postSigned(url+routePathLifecycleTestRunCycle, nil)
	}
}

// postSigned wraps SigV4-signed POST. Body may be nil for empty payloads.
// Logs the response body on non-2xx so failing tests surface server messages.
func (f *lifecycleFixture) postSigned(url string, body []byte) {
	f.t.Helper()
	ctx := context.Background()
	if body == nil {
		body = []byte{}
	}
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = f.signer.SignHTTP(ctx, f.creds, req, payloadHash, "s3", "us-east-1", time.Now())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		f.t.Logf("POST %s -> %d: %s", url, resp.StatusCode, string(respBody))
	}
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
		"lifecycle test-ctl endpoint must succeed (route requires lifecycle service enabled — pass --lifecycle-interval=24h to the fixture)")
}

// Route path constants kept aligned with internal/server/route_paths.go.
// Duplicated here intentionally — tests cannot import the internal package's
// unexported identifiers.
const (
	routePathLifecycleTestRunCycle = "/api/cluster/lifecycle/test/run-cycle"
	routePathLifecycleTestSetNow   = "/api/cluster/lifecycle/test/set-now"
)
