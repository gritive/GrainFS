//go:build linux || darwin

package clusteradmin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/clusteradmin"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// auditFakeProposer applies the patch directly to the FSM (no raft).
type auditFakeProposer struct{ fsm *cluster.MetaFSM }

func (p *auditFakeProposer) ProposeClusterConfigPatch(patch cluster.ClusterConfigPatch) error {
	return p.fsm.ApplyClusterConfigPatchForTest(patch)
}

// wrapStdlibForTest bridges a stdlib http.HandlerFunc onto Hertz. Mirrors
// serveruntime.wrapStdlibNoParam but is local to this test so we don't pull
// the serveruntime dependency cone.
func wrapStdlibForTest(fn http.HandlerFunc) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		method := string(c.Method())
		uri := c.Request.URI()
		target := string(uri.Path())
		if qs := uri.QueryString(); len(qs) > 0 {
			target += "?" + string(qs)
		}
		body := c.Request.Body()
		req, _ := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
		c.Request.Header.VisitAll(func(k, v []byte) { req.Header.Add(string(k), string(v)) })

		rw := newRecResp()
		fn(rw, req)
		c.SetStatusCode(rw.status)
		for k, vs := range rw.header {
			for _, v := range vs {
				c.Response.Header.Add(k, v)
			}
		}
		if rw.body.Len() > 0 {
			c.Write(rw.body.Bytes())
		}
	}
}

type recResp struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newRecResp() *recResp                     { return &recResp{header: http.Header{}, status: http.StatusOK} }
func (r *recResp) Header() http.Header         { return r.header }
func (r *recResp) WriteHeader(status int)      { r.status = status }
func (r *recResp) Write(b []byte) (int, error) { return r.body.Write(b) }

// shortAdminSockPath returns a UDS path under /tmp short enough to fit
// macOS's 104-char sun_path limit. t.TempDir() on darwin returns paths
// under /var/folders/... that exceed this limit.
func shortAdminSockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "auditsock-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "admin.sock")
}

func TestClusterConfigPatch_AuditLog_CarriesPeerUID(t *testing.T) {
	// Redirect zerolog to a capture buffer.
	var buf bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&buf).With().Timestamp().Logger()
	t.Cleanup(func() { log.Logger = prevLogger })

	sockPath := shortAdminSockPath(t)

	fsm := cluster.NewMetaFSM()
	handler := clusteradmin.NewClusterConfigHandler(fsm, &auditFakeProposer{fsm: fsm}, nil)

	srv, err := admin.Start(admin.Config{
		SocketPath: sockPath,
		Deps:       &admin.Deps{NodeID: "audit-test"}, // most fields nil; we never hit those routes
		ExtraRoutes: func(h *server.Hertz) {
			h.PATCH("/v1/cluster/config", wrapStdlibForTest(handler.ServeHTTP))
			h.GET("/v1/cluster/config", wrapStdlibForTest(handler.ServeHTTP))
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})

	// Dial admin UDS as the test process uid.
	httpc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sockPath)
			},
		},
		Timeout: 5 * time.Second,
	}

	patchBody, err := json.Marshal(map[string]any{"balancer_cb_threshold": 0.91})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPatch, "http://admin/v1/cluster/config", bytes.NewReader(patchBody))
	require.NoError(t, err)
	resp, err := httpc.Do(req)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "PATCH failed: status=%d body=%q", resp.StatusCode, string(respBody))

	// Wait briefly for log emission (synchronous in the handler, but the
	// zerolog write may race the test read by microseconds).
	require.Eventually(t, func() bool {
		return bytes.Contains(buf.Bytes(), []byte(`"event":"cluster_config_patch_received"`))
	}, 2*time.Second, 25*time.Millisecond, "audit log line not seen; buf=%q", buf.String())

	// Find the audit line and assert it carries our uid + Resolved=true.
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	found := false
	for dec.More() {
		var ev map[string]any
		if err := dec.Decode(&ev); err != nil {
			continue
		}
		if ev["event"] == "cluster_config_patch_received" {
			require.Equal(t, float64(os.Getuid()), ev["actor_uid"], "actor_uid mismatch")
			require.Equal(t, true, ev["actor_uid_resolved"], "actor_uid_resolved must be true")
			found = true
			break
		}
	}
	require.True(t, found, "no cluster_config_patch_received line found in log buf:\n%s", buf.String())
}
