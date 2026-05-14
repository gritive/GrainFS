package admin_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
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

func newUIRouteTestServer(t *testing.T) (*server.Hertz, string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	h := server.New(server.WithListener(ln), server.WithHostPorts(""))
	start := func() {
		t.Helper()
		go h.Spin() //nolint:errcheck
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})
	return h, "http://" + ln.Addr().String(), start
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
