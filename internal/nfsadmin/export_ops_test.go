package nfsadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	if err := RunAdd(context.Background(), AddExportOptions{BaseOptions: base, Bucket: "b1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), `added NFS export "b1"`) {
		t.Fatalf("unexpected output: %s", out.String())
	}

	out.Reset()
	if err := RunAdd(context.Background(), AddExportOptions{BaseOptions: BaseOptions{Stdout: out}, Bucket: "b1", DryRun: true}); err != nil {
		t.Fatalf("dry-run err: %v", err)
	}
	if !strings.Contains(out.String(), "would add") {
		t.Fatalf("unexpected dry-run output: %s", out.String())
	}

	out.Reset()
	quietBase := base
	quietBase.Quiet = true
	if err := RunAdd(context.Background(), AddExportOptions{BaseOptions: quietBase, Bucket: "b1"}); err != nil {
		t.Fatalf("quiet err: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("quiet wrote output: %s", out.String())
	}
}

func TestRunAddBucketNotFound(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "POST", path: "/v1/nfs/exports", status: 404,
		errResp: &Error{Code: "bucket_not_found", Message: "missing", Help: "create it", DocsURL: "https://example.test"},
	}})
	defer srv.Close()
	base, _, errBuf := optsForServer(srv)
	err := RunAdd(context.Background(), AddExportOptions{BaseOptions: base, Bucket: "missing"})
	if !IsBucketNotFound(err) {
		t.Fatalf("expected bucket_not_found, got %v", err)
	}
	if !strings.Contains(errBuf.String(), "Hint:") || !strings.Contains(errBuf.String(), "Docs:") {
		t.Fatalf("expected rendered help/docs, got %s", errBuf.String())
	}
}

func TestRunListJSONAndText(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{
		method: "GET", path: "/v1/nfs/exports",
		body: ListNfsExportsResp{Exports: []NfsExportInfo{{Bucket: "b1", FsidMajor: 1, FsidMinor: 2, Generation: 3}}},
	}})
	defer srv.Close()
	base, out, _ := optsForServer(srv)
	if err := RunList(context.Background(), ListExportOptions{BaseOptions: base}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.String(), "BUCKET") || !strings.Contains(out.String(), "1.2") {
		t.Fatalf("unexpected text: %s", out.String())
	}

	out.Reset()
	base.JSONOut = true
	if err := RunList(context.Background(), ListExportOptions{BaseOptions: base}); err != nil {
		t.Fatalf("json err: %v", err)
	}
	if !strings.Contains(out.String(), `"bucket": "b1"`) {
		t.Fatalf("unexpected json: %s", out.String())
	}
}

func TestRunRemove204(t *testing.T) {
	srv := newFakeServer(t, []fakeRoute{{method: "DELETE", path: "/v1/nfs/exports/b1", status: 204}})
	defer srv.Close()
	base, _, _ := optsForServer(srv)
	if err := RunRemove(context.Background(), RemoveExportOptions{BaseOptions: base, Bucket: "b1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
}
