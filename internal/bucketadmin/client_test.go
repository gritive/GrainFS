package bucketadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClient_Create_NoAttach(t *testing.T) {
	var got map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b","created_at":"2026-05-20T12:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	resp, err := c.Create(context.Background(), "b", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Name != "b" {
		t.Errorf("resp = %+v", resp)
	}
	if got["name"] != "b" {
		t.Errorf("body name = %v", got["name"])
	}
	if _, ok := got["attach"]; ok {
		t.Errorf("attach field present when both attach args empty; body = %v", got)
	}
}

func TestClient_Create_WithAttach(t *testing.T) {
	var got map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b","created_at":"2026-05-20T12:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if _, err := c.Create(context.Background(), "b", "sa-x", "Admin"); err != nil {
		t.Fatal(err)
	}
	attach, ok := got["attach"].(map[string]any)
	if !ok {
		t.Fatalf("attach missing or wrong type; body = %v", got)
	}
	if attach["sa"] != "sa-x" || attach["policy"] != "Admin" {
		t.Errorf("attach = %v", attach)
	}
}

func TestClient_List(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		// Server returns the {"buckets":[...]} envelope; client unwraps it.
		_, _ = w.Write([]byte(`{"buckets":[{"name":"b1","has_upstream":true},{"name":"b2","has_upstream":false}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	items, err := c.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Name != "b1" || items[1].Name != "b2" {
		t.Errorf("items = %+v", items)
	}
	if !items[0].HasUpstream || items[1].HasUpstream {
		t.Errorf("HasUpstream mismatch: %+v", items)
	}
}

func TestClient_Info_PathEscape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b%2Fx", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"b/x","object_count":42,"has_upstream":true,"versioning":"Enabled"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	info, err := c.Info(context.Background(), "b/x")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "b/x" {
		t.Errorf("name = %q", info.Name)
	}
	if info.ObjectCount == nil || *info.ObjectCount != 42 {
		t.Errorf("object_count = %v", info.ObjectCount)
	}
	if !info.HasUpstream {
		t.Errorf("has_upstream = false")
	}
	if info.Versioning != "Enabled" {
		t.Errorf("versioning = %q", info.Versioning)
	}
}

func TestClient_Delete_QueryParams(t *testing.T) {
	var sawURL, sawMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b", func(w http.ResponseWriter, r *http.Request) {
		sawURL = r.URL.String()
		sawMethod = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.Delete(context.Background(), "b", true, true); err != nil {
		t.Fatal(err)
	}
	if sawMethod != http.MethodDelete {
		t.Errorf("method = %s", sawMethod)
	}
	if !strings.Contains(sawURL, "force=true") || !strings.Contains(sawURL, "recursive=true") {
		t.Errorf("url = %q", sawURL)
	}
}

func TestClient_Delete_NoQueryWhenFlagsUnset(t *testing.T) {
	var sawURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b", func(w http.ResponseWriter, r *http.Request) {
		sawURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.Delete(context.Background(), "b", false, false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sawURL, "?") {
		t.Errorf("expected no query string; url = %q", sawURL)
	}
}

func TestClient_UpstreamPut(t *testing.T) {
	var got map[string]string
	var method string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	err := c.UpstreamPut(context.Background(), UpstreamPutOptions{
		Bucket:       "b",
		Scheme:       "s3",
		Endpoint:     "https://s3.example.com",
		AccessKey:    "AK",
		SecretKey:    "SK",
		Region:       "us-east-1",
		RemoteBucket: "remote-b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut {
		t.Errorf("method = %s", method)
	}
	if got["bucket"] != "b" || got["scheme"] != "s3" || got["upstream_url"] != "https://s3.example.com" ||
		got["access_key"] != "AK" || got["secret_key"] != "SK" || got["region"] != "us-east-1" ||
		got["remote_bucket"] != "remote-b" {
		t.Errorf("got = %v", got)
	}
}

func TestClient_UpstreamGetRaw(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/upstream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bucket":"b","scheme":"s3"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	raw, err := c.UpstreamGetRaw(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"bucket":"b","scheme":"s3"}` {
		t.Errorf("raw = %s", raw)
	}
}

func TestClient_UpstreamListRaw(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"bucket":"b1"},{"bucket":"b2"}]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	raw, err := c.UpstreamListRaw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `[{"bucket":"b1"},{"bucket":"b2"}]` {
		t.Errorf("raw = %s", raw)
	}
}

func TestClient_UpstreamDelete(t *testing.T) {
	var method string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/upstream", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.UpstreamDelete(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodDelete {
		t.Errorf("method = %s", method)
	}
}

func TestClient_PolicyGetRaw(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"policy":{"Version":"2012-10-17","Statement":[]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	raw, err := c.PolicyGetRaw(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"Version":"2012-10-17","Statement":[]}` {
		t.Errorf("raw = %s", raw)
	}
}

func TestClient_PolicySet_Verbatim(t *testing.T) {
	// Critical: the policy field the server receives must preserve the
	// policy bytes we sent (no re-marshal that would reorder keys).
	policy := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*"}]}`)
	var got struct {
		Policy json.RawMessage `json:"policy"`
	}
	var method string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/policy", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.PolicySet(context.Background(), "b", policy); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut {
		t.Errorf("method = %s", method)
	}
	if string(got.Policy) != string(policy) {
		t.Errorf("policy mismatch:\n got: %s\nwant: %s", got.Policy, policy)
	}
}

func TestClient_PolicyDelete(t *testing.T) {
	var method string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/policy", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.PolicyDelete(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodDelete {
		t.Errorf("method = %s", method)
	}
}

func TestClient_VersioningGet(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"Enabled"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	resp, err := c.VersioningGet(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "Enabled" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestClient_VersioningEnable(t *testing.T) {
	var got map[string]string
	var method string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.VersioningEnable(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut {
		t.Errorf("method = %s", method)
	}
	if got["status"] != "Enabled" {
		t.Errorf("got = %v", got)
	}
}

func TestClient_VersioningSuspend(t *testing.T) {
	var got map[string]string
	var method string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/versioning", func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.VersioningSuspend(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut {
		t.Errorf("method = %s", method)
	}
	if got["status"] != "Suspended" {
		t.Errorf("got = %v", got)
	}
}
