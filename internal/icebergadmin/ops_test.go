package icebergadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func newTestServer(t *testing.T, mux *http.ServeMux) (url string, stop func()) {
	t.Helper()
	srv := httptest.NewServer(mux)
	return srv.URL, srv.Close
}

// sampleIcebergResp returns a believable IcebergConfigResponse.
func sampleIcebergResp() adminapi.IcebergConfigResponse {
	return adminapi.IcebergConfigResponse{
		CatalogURI:    "http://localhost:9000/iceberg",
		OAuthTokenURI: "http://localhost:9000/oauth/token",
		Warehouse:     "s3://my-warehouse",
		ClientID:      "sa-x",
		ClientSecret:  "super-secret",
	}
}

// TestRunIcebergConfig_HappyPath verifies table output contains all key fields.
func TestRunIcebergConfig_HappyPath(t *testing.T) {
	resp := sampleIcebergResp()
	body, _ := json.Marshal(resp)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iceberg/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		var req adminapi.IcebergConfigRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Warehouse != "s3://my-warehouse" {
			t.Errorf("warehouse = %q", req.Warehouse)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunIcebergConfig(context.Background(), IcebergConfigOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Warehouse:   "s3://my-warehouse",
		SAID:        "sa-x",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := out.String()
	for _, want := range []string{"catalog_uri:", "oauth_token_uri:", "warehouse:", "client_id:", "client_secret:", "super-secret"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, got)
		}
	}
}

// TestRunIcebergConfig_NoReveal verifies --no-reveal hides the secret in table output.
func TestRunIcebergConfig_NoReveal(t *testing.T) {
	// Server returns empty ClientSecret when NoReveal=true (mirrors real server behaviour).
	resp := sampleIcebergResp()
	resp.ClientSecret = ""
	body, _ := json.Marshal(resp)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iceberg/config", func(w http.ResponseWriter, r *http.Request) {
		var req adminapi.IcebergConfigRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if !req.NoReveal {
			t.Error("NoReveal should be true")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunIcebergConfig(context.Background(), IcebergConfigOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out},
		Warehouse:   "s3://my-warehouse",
		NoReveal:    true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := out.String()
	// The client-side redaction message must appear.
	if !strings.Contains(got, "hidden") {
		t.Errorf("expected hidden-secret message; output:\n%s", got)
	}
	// The actual secret must NOT appear.
	if strings.Contains(got, "super-secret") {
		t.Errorf("secret leaked into output with --no-reveal; output:\n%s", got)
	}
}

// TestRunIcebergConfig_JSONOut verifies --json mode emits parseable JSON with expected fields.
func TestRunIcebergConfig_JSONOut(t *testing.T) {
	resp := sampleIcebergResp()
	body, _ := json.Marshal(resp)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iceberg/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	var out bytes.Buffer
	err := RunIcebergConfig(context.Background(), IcebergConfigOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &out, JSONOut: true},
		Warehouse:   "s3://my-warehouse",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got adminapi.IcebergConfigResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out.String())
	}
	if got.ClientID != resp.ClientID {
		t.Errorf("client_id = %q, want %q", got.ClientID, resp.ClientID)
	}
	if got.ClientSecret != resp.ClientSecret {
		t.Errorf("client_secret = %q, want %q", got.ClientSecret, resp.ClientSecret)
	}
}

// TestRunIcebergConfig_ServerError verifies a 400 error from the server is surfaced.
func TestRunIcebergConfig_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iceberg/config", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"invalid_warehouse","error":"warehouse not found"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunIcebergConfig(context.Background(), IcebergConfigOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
		Warehouse:   "s3://nonexistent",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRunIcebergConfig_NotFound verifies a 404 is surfaced as an error.
func TestRunIcebergConfig_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iceberg/config", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"not_found","error":"iceberg endpoint disabled"}`))
	})
	url, stop := newTestServer(t, mux)
	defer stop()

	err := RunIcebergConfig(context.Background(), IcebergConfigOptions{
		BaseOptions: BaseOptions{Endpoint: url, Stdout: &bytes.Buffer{}},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
