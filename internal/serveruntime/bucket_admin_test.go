package serveruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// inProcessProposeFull extends inProcessPropose with bucket-upstream commands so
// tests in this file can drive HandleBucketUpstreamPut end-to-end without a cluster.
func inProcessProposeFull(applier *iam.Applier) iam.ProposeFunc {
	base := inProcessPropose(applier)
	return func(ctx context.Context, t clusterpb.MetaCmdType, payload []byte) error {
		switch t {
		case clusterpb.MetaCmdTypeIAMBucketUpstreamPut:
			return applier.ApplyBucketUpstreamPut(payload)
		case clusterpb.MetaCmdTypeIAMBucketUpstreamDelete:
			return applier.ApplyBucketUpstreamDelete(payload)
		default:
			return base(ctx, t, payload)
		}
	}
}

// newAdminAPIWithBucketUpstream builds an AdminAPI wired for bucket-upstream
// proposals, suitable for TestBucketAdminRoutes_* tests.
func newAdminAPIWithBucketUpstream(t *testing.T) *iam.AdminAPI {
	t.Helper()
	enc := newTestEncryptor(t)
	store := iam.NewStore()
	applier := iam.NewApplier(store, enc)
	proposer := &iam.MetaProposer{Propose: inProcessProposeFull(applier)}
	return iam.NewAdminAPI(store, proposer, enc)
}

// startBucketAdminTestServer spins up a Hertz UDS server with BOTH
// RegisterIAMAdminRoutes and RegisterBucketAdminRoutes registered, so we can
// assert that the new paths respond and the legacy ones are gone.
func startBucketAdminTestServer(t *testing.T, api *iam.AdminAPI) *http.Client {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "gs-bucket-uds-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "a.sock")

	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}

	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
	)
	admin.RegisterIAMOnly(h, &admin.Deps{IAM: api})

	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.Dial("unix", sock)
		if dialErr == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 5 * time.Second,
	}
}

// TestBucketAdminRoutes_NewPathsRespond ensures the relocated routes are wired
// and the legacy /v1/iam/bucket-upstream* routes are gone (regression for
// ADR 0010 surface relocation).
func TestBucketAdminRoutes_NewPathsRespond(t *testing.T) {
	api := newAdminAPIWithBucketUpstream(t)
	cli := startBucketAdminTestServer(t, api)

	// New path: PUT /v1/buckets/upstream — create a record.
	body, _ := json.Marshal(map[string]string{
		"bucket":       "alpha",
		"upstream_url": "http://up:9000",
		"access_key":   "AKIAIOSFODNN7EXAMPLE",
		"secret_key":   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	req, err := http.NewRequest("PUT", "http://unix/v1/buckets/upstream", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("PUT /v1/buckets/upstream: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT new path: expected 204, got %d", resp.StatusCode)
	}

	// New path: GET /v1/buckets/upstream — list.
	resp2, err := cli.Get("http://unix/v1/buckets/upstream")
	if err != nil {
		t.Fatalf("GET /v1/buckets/upstream: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET list new path: expected 200, got %d", resp2.StatusCode)
	}

	// New path: GET /v1/buckets/:bucket/upstream — single record.
	resp3, err := cli.Get("http://unix/v1/buckets/alpha/upstream")
	if err != nil {
		t.Fatalf("GET /v1/buckets/alpha/upstream: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GET single new path: expected 200, got %d", resp3.StatusCode)
	}

	// Legacy path is gone: GET /v1/iam/bucket-upstream must 404.
	resp4, err := cli.Get("http://unix/v1/iam/bucket-upstream")
	if err != nil {
		t.Fatalf("GET legacy: %v", err)
	}
	resp4.Body.Close()
	if resp4.StatusCode != http.StatusNotFound {
		t.Fatalf("legacy /v1/iam/bucket-upstream must be 404, got %d", resp4.StatusCode)
	}
}

// TestBucketAdminRoutes_DeleteAndGet verifies DELETE /v1/buckets/:bucket/upstream
// removes the record and GET /v1/buckets/:bucket/upstream returns 404 after deletion.
func TestBucketAdminRoutes_DeleteAndGet(t *testing.T) {
	api := newAdminAPIWithBucketUpstream(t)
	cli := startBucketAdminTestServer(t, api)

	// Create.
	body, _ := json.Marshal(map[string]string{
		"bucket":       "beta",
		"upstream_url": "https://s3.example.com",
		"access_key":   "AKIAIOSFODNN7EXAMPLE",
		"secret_key":   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	req, _ := http.NewRequest("PUT", "http://unix/v1/buckets/upstream", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT expected 204, got %d", resp.StatusCode)
	}

	// Delete.
	delReq, _ := http.NewRequest("DELETE", "http://unix/v1/buckets/beta/upstream", nil)
	resp2, err := cli.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE expected 204, got %d", resp2.StatusCode)
	}

	// Get after delete: must 404.
	resp3, err := cli.Get("http://unix/v1/buckets/beta/upstream")
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete expected 404, got %d", resp3.StatusCode)
	}
}
