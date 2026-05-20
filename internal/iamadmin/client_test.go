package iamadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClient_SACreate(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "alice" || body["description"] != "data team" {
			t.Errorf("body = %v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sa_id":"sa-x","name":"alice","access_key":"AK","secret_key":"SK"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	resp, err := c.SACreate(context.Background(), "alice", "data team")
	if err != nil {
		t.Fatal(err)
	}
	if resp.SAID != "sa-x" || resp.AccessKey != "AK" || resp.SecretKey != "SK" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestClient_SAList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"sa_id":"sa-1","name":"alice","description":"d","created_at":"2026-05-20T12:00:00Z","num_keys":2,"num_grants":3}]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	resp, err := c.SAList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 1 || resp[0].SAID != "sa-1" || resp[0].NumKeys != 2 {
		t.Errorf("resp = %+v", resp)
	}
}

func TestClient_SAGet_PathEscape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa%2Fx", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sa_id":"sa/x","name":"n","description":"","created_at":"2026-05-20T12:00:00Z","created_by":""}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	resp, err := c.SAGet(context.Background(), "sa/x")
	if err != nil {
		t.Fatal(err)
	}
	if resp.SAID != "sa/x" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestClient_SADelete(t *testing.T) {
	var saw string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x", func(w http.ResponseWriter, r *http.Request) {
		saw = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.SADelete(context.Background(), "sa-x"); err != nil {
		t.Fatal(err)
	}
	if saw != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", saw)
	}
}

func TestClient_KeyCreate_WithBuckets(t *testing.T) {
	var got map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_key":"AK","secret_key":"SK"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	raw, err := c.KeyCreateRaw(context.Background(), "sa-x", []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"access_key":"AK"`) {
		t.Errorf("raw = %s", raw)
	}
	bs, _ := got["buckets"].([]any)
	if len(bs) != 2 || bs[0] != "a" || bs[1] != "b" {
		t.Errorf("buckets = %v", got["buckets"])
	}
}

func TestClient_KeyCreate_NoBuckets_OmitsField(t *testing.T) {
	var raw map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&raw)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if _, err := c.KeyCreateRaw(context.Background(), "sa-x", nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["buckets"]; ok {
		t.Errorf("buckets field present in body when nil slice passed; body = %v", raw)
	}
}

func TestClient_KeyRevoke(t *testing.T) {
	var saw string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/sa/sa-x/key/AK", func(w http.ResponseWriter, r *http.Request) {
		saw = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClientForURL(srv.URL)
	if err := c.KeyRevoke(context.Background(), "sa-x", "AK"); err != nil {
		t.Fatal(err)
	}
	if saw != http.MethodDelete {
		t.Errorf("method = %q", saw)
	}
}
