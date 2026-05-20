package admin_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server/admin"
)

type fakeCutoverIAM struct {
	*iam.AdminAPI
	err error
}

func (f *fakeCutoverIAM) CutoverBucketUpstream(context.Context, string) error {
	if gateErr, ok := f.err.(*compat.GateRejectError); ok {
		return &adminapi.Error{Code: "conflict", Message: gateErr.PublicMessage()}
	}
	return f.err
}

func startCutoverRouteTestServer(t *testing.T, iamSvc admin.IAMService) *http.Client {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "gs-cutover-uds-")
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
		server.WithExitWaitTime(10*time.Millisecond),
	)
	admin.RegisterIAMOnly(h, &admin.Deps{IAM: iamSvc})
	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
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
		time.Sleep(25 * time.Millisecond)
	}
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

func TestBucketAdminRoutes_CutoverGateRejectIsConflict(t *testing.T) {
	iamSvc := &fakeCutoverIAM{err: &compat.GateRejectError{Plan: compat.GatePlan{
		Capability: compat.CapabilityMigrationCutoverV1,
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  compat.OperationMigrationCutover,
		Missing:    []compat.NodeID{"node-old"},
	}}}
	cli := startCutoverRouteTestServer(t, iamSvc)

	cutover, _ := http.NewRequest("POST", "http://unix/v1/migration/cutover", strings.NewReader(`{"bucket":"gamma"}`))
	cutover.Header.Set("Content-Type", "application/json")
	resp2, err := cli.Do(cutover)
	if err != nil {
		t.Fatalf("POST /v1/migration/cutover: %v", err)
	}
	raw, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("cutover expected 409, got %d body=%s", resp2.StatusCode, raw)
	}
	if bytes.Contains(raw, []byte("node-old")) {
		t.Fatalf("cutover response leaked node ID: %s", raw)
	}
	if !bytes.Contains(raw, []byte("migration_cutover_v1")) {
		t.Fatalf("cutover response missing capability: %s", raw)
	}
}
