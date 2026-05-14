package nfsadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type fakeRoute struct {
	method  string
	path    string
	status  int
	body    any
	errResp *Error
	verify  func(*http.Request)
}

func newFakeServer(t *testing.T, routes []fakeRoute) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	registered := map[string]bool{}
	for _, r := range routes {
		if registered[r.path] {
			continue
		}
		registered[r.path] = true
		mux.HandleFunc(r.path, func(w http.ResponseWriter, req *http.Request) {
			for _, rt := range routes {
				if rt.path != req.URL.Path || rt.method != req.Method {
					continue
				}
				if rt.verify != nil {
					rt.verify(req)
				}
				w.Header().Set("Content-Type", "application/json")
				status := rt.status
				if status == 0 {
					status = 200
				}
				w.WriteHeader(status)
				if rt.errResp != nil {
					_ = json.NewEncoder(w).Encode(rt.errResp)
					return
				}
				if rt.body != nil {
					_ = json.NewEncoder(w).Encode(rt.body)
				}
				return
			}
			http.NotFound(w, req)
		})
	}
	return httptest.NewServer(mux)
}

func optsForServer(srv *httptest.Server) (BaseOptions, *bytes.Buffer, *bytes.Buffer) {
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	return BaseOptions{Endpoint: srv.URL, Stdout: out, Stderr: errBuf}, out, errBuf
}

func TestRunAddHappyDryRunAndQuiet(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/nfs/exports",
		body: NfsExportInfo{Bucket: "b1", FsidMajor: 1, FsidMinor: 2, Generation: 1},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	require.NoError(t, RunAdd(context.Background(), AddExportOptions{BaseOptions: base, Bucket: "b1"}))
	require.Contains(t, out.String(), `added NFS export "b1"`)

	out.Reset()
	require.NoError(t, RunAdd(context.Background(), AddExportOptions{BaseOptions: BaseOptions{Stdout: out}, Bucket: "b1", DryRun: true}))
	require.Contains(t, out.String(), "Would add export 'b1' (rw)")

	out.Reset()
	quietBase := base
	quietBase.Quiet = true
	require.NoError(t, RunAdd(context.Background(), AddExportOptions{BaseOptions: quietBase, Bucket: "b1"}))
	require.Empty(t, out.String())
}

func TestRunAddBucketNotFound(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/nfs/exports", status: 404,
		errResp: &Error{Code: "bucket_not_found", Message: "missing", Help: "create it", DocsURL: "https://example.test"},
	}})
	defer srv.Close()
	base, _, errBuf := optsForServer(srv)
	err := RunAdd(context.Background(), AddExportOptions{BaseOptions: base, Bucket: "missing"})
	require.Truef(t, IsBucketNotFound(err), "expected bucket_not_found, got %v", err)
	require.Contains(t, errBuf.String(), "Hint:")
	require.Contains(t, errBuf.String(), "Docs:")
}

func TestRunListJSONAndText(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/nfs/exports",
		body: ListNfsExportsResp{Exports: []NfsExportInfo{{Bucket: "b1", FsidMajor: 1, FsidMinor: 2, Generation: 3}}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	require.NoError(t, RunList(context.Background(), ListExportOptions{BaseOptions: base}))
	require.Contains(t, out.String(), "BUCKET")
	require.Contains(t, out.String(), "1.2")

	out.Reset()
	base.JSONOut = true
	require.NoError(t, RunList(context.Background(), ListExportOptions{BaseOptions: base}))
	require.Contains(t, out.String(), `"bucket": "b1"`)
}

func TestClientExportDebug(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET",
		path:   "/v1/nfs/exports/my-data/debug",
		body: ExportDebugResp{
			Bucket:     "my-data",
			Registered: true,
			BackendBucket: adminapi.ExportDebugBackend{
				Exists:      true,
				ObjectCount: 7,
			},
		},
	}})
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	resp, err := c.ExportDebug(context.Background(), "my-data")
	require.NoError(t, err)
	require.True(t, resp.Registered)
	require.EqualValues(t, 7, resp.BackendBucket.ObjectCount)
}

func TestRunRemove204(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{method: "DELETE", path: "/v1/nfs/exports/b1", status: 204}})
	defer srv.Close()
	base, _, _ := optsForServer(srv)
	require.NoError(t, RunRemove(context.Background(), RemoveExportOptions{BaseOptions: base, Bucket: "b1"}))
}
