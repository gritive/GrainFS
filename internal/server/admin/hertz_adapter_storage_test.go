package admin_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestRegisterUIStorageRoutesExposeSafeSurface(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	d := newServerDeps(t, t.TempDir())
	d.Buckets = newFakeBucketOps()
	d.Protocols = adminapi.StorageProtocolStatusResp{
		P9: adminapi.ProtocolEndpointStatus{Enabled: true, Bind: "127.0.0.1", Port: 564},
	}
	admin.RegisterUI(h, d)
	start()

	resp := doRouteTestRequest(t, http.MethodGet, base+"/ui/api/storage/protocols", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRouteTestRequest(t, http.MethodGet, base+"/ui/api/storage/buckets", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRouteTestRequest(t, http.MethodPost, base+"/ui/api/storage/buckets", bytes.NewBufferString(`{"name":"logs"}`))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()
}

func TestRegisterUIStorageRoutesDoNotExposeDestructiveBucketDelete(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	d := newServerDeps(t, t.TempDir())
	d.Buckets = newFakeBucketOps()
	admin.RegisterUI(h, d)
	start()

	resp := doRouteTestRequest(t, http.MethodDelete, base+"/ui/api/storage/buckets/logs", nil)
	defer resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)
}

func TestRegisterUIStorageRoutesDoNotExposeNfsDebug(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	d := newServerDeps(t, t.TempDir())
	d.Buckets = newFakeBucketOps()
	d.NfsExports = &fakeStorageNfsExports{
		exports: map[string]admin.NfsExportInfo{
			"logs": {Bucket: "logs", Generation: 1},
		},
	}
	admin.RegisterUI(h, d)
	start()

	resp := doRouteTestRequest(t, http.MethodGet, base+"/ui/api/storage/nfs/exports/logs/debug", nil)
	defer resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)
}

func TestRegisterUIStorageRoutesDoNotExposeNfsMutations(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	d := newServerDeps(t, t.TempDir())
	d.Buckets = newFakeBucketOps()
	d.NfsExports = &fakeStorageNfsExports{
		exports: map[string]admin.NfsExportInfo{
			"logs": {Bucket: "logs", Generation: 1},
		},
	}
	admin.RegisterUI(h, d)
	start()

	resp := doRouteTestRequest(t, http.MethodPost, base+"/ui/api/storage/nfs/exports", bytes.NewBufferString(`{"bucket":"logs"}`))
	resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)

	resp = doRouteTestRequest(t, http.MethodPatch, base+"/ui/api/storage/nfs/exports/logs", bytes.NewBufferString(`{"read_only":true}`))
	resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)
}

func TestRegisterUIDoesNotExposeDestructiveVolumeRoutes(t *testing.T) {
	h, base, start := newUIRouteTestServer(t)
	d := newServerDeps(t, t.TempDir())
	_, err := d.Manager.Create("ui-vol", 1<<20)
	require.NoError(t, err)
	_, err = d.Manager.WriteAt("ui-vol", []byte("x"), 0)
	require.NoError(t, err)
	snapID, err := d.Manager.CreateSnapshot("ui-vol")
	require.NoError(t, err)
	admin.RegisterUI(h, d)
	start()

	resp := doRouteTestRequest(t, http.MethodDelete, base+"/ui/api/volumes/ui-vol/snapshots/"+snapID, nil)
	resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)

	resp = doRouteTestRequest(t, http.MethodPost, base+"/ui/api/volumes/ui-vol/snapshots/"+snapID+"/rollback", nil)
	resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)

	resp = doRouteTestRequest(t, http.MethodDelete, base+"/ui/api/volumes/ui-vol?force=true", nil)
	resp.Body.Close()
	require.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode)

	_, err = d.Manager.Get("ui-vol")
	require.NoError(t, err)
	snaps, err := d.Manager.ListSnapshots("ui-vol")
	require.NoError(t, err)
	require.Len(t, snaps, 1)
}

func TestBucketPolicyRoute_InvalidStoredPolicyDoesNotPanic(t *testing.T) {
	cli := startBucketRouteTestServer(t, []byte{0xae, '{', '}'})

	resp := doUnixRouteTestRequest(t, cli, http.MethodGet, "http://unix/v1/buckets/logs/policy", nil)
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, string(raw))
	require.Contains(t, string(raw), "stored bucket policy is invalid JSON")

	alive := doUnixRouteTestRequest(t, cli, http.MethodGet, "http://unix/v1/buckets/logs", nil)
	alive.Body.Close()
	require.NotEqual(t, http.StatusServiceUnavailable, alive.StatusCode)
}

func TestBucketPolicyRoute_WritesPolicyEnvelope(t *testing.T) {
	cli := startBucketRouteTestServer(t, []byte(`{"Version":"2012-10-17","Statement":[]}`))

	resp := doUnixRouteTestRequest(t, cli, http.MethodGet, "http://unix/v1/buckets/logs/policy", nil)
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.JSONEq(t, `{"policy":{"Version":"2012-10-17","Statement":[]}}`, string(raw))
}

func newUIRouteTestServer(t *testing.T) (*server.Hertz, string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	h := server.New(server.WithListener(ln), server.WithHostPorts(""), server.WithExitWaitTime(10*time.Millisecond))
	start := func() {
		t.Helper()
		go h.Spin() //nolint:errcheck
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = h.Shutdown(ctx)
	})
	return h, "http://" + ln.Addr().String(), start
}

func startBucketRouteTestServer(t *testing.T, policy []byte) *http.Client {
	t.Helper()
	d, err := os.MkdirTemp("", "grainfs-admin-bucket-route-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })

	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
		server.WithExitWaitTime(10*time.Millisecond),
	)
	buckets := newFakeBucketOpsWithPolicy()
	buckets.buckets["logs"] = true
	buckets.policy["logs"] = policy
	admin.RegisterAdmin(h, &admin.Deps{Buckets: buckets})
	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = h.Shutdown(ctx)
	})
	waitForUnixSocket(t, sock)
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

func waitForUnixSocket(t *testing.T, sock string) {
	t.Helper()
	var lastErr error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", sock)
		if err == nil {
			conn.Close()
			return
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("unix socket not ready: %v", lastErr)
}

func doRouteTestRequest(t *testing.T, method, url string, body *bytes.Buffer) *http.Response {
	t.Helper()
	var lastErr error
	for i := 0; i < 20; i++ {
		var reqBody io.Reader
		if body != nil {
			reqBody = bytes.NewBuffer(body.Bytes())
		}
		req, err := http.NewRequest(method, url, reqBody)
		require.NoError(t, err)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			return resp
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s %s: %v", method, url, lastErr)
	return nil
}

func doUnixRouteTestRequest(t *testing.T, cli *http.Client, method, url string, body *bytes.Buffer) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewBuffer(body.Bytes())
	}
	req, err := http.NewRequest(method, url, reqBody)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := cli.Do(req)
	require.NoError(t, err)
	return resp
}
