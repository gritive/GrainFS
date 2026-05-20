package bucketadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunPolicyGet_Passthrough(t *testing.T) {
	body := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"policy":` + body + `}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunPolicyGet(context.Background(), PolicyGetOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != body+"\n" {
		t.Errorf("got %q want %q", out.String(), body+"\n")
	}
}

func TestRunPolicySet_BodyVerbatim(t *testing.T) {
	// CRITICAL: server must receive the policy field byte-for-byte equal
	// to what we sent. No re-marshal, no key reorder.
	inputPolicy := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	var got struct {
		Policy json.RawMessage `json:"policy"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunPolicySet(context.Background(), PolicySetOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
		Policy:      inputPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Policy, inputPolicy) {
		t.Errorf("policy mismatch\ngot:  %q\nwant: %q", got.Policy, inputPolicy)
	}
	if out.String() != "" {
		t.Errorf("expected no stdout, got %q", out.String())
	}
}

func TestRunPolicyDelete_NoStdout(t *testing.T) {
	var sawMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/b/policy", func(w http.ResponseWriter, r *http.Request) {
		sawMethod = r.Method
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var out bytes.Buffer
	err := RunPolicyDelete(context.Background(), PolicyDeleteOptions{
		BaseOptions: BaseOptions{Endpoint: srv.URL, Stdout: &out},
		Bucket:      "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sawMethod != http.MethodDelete {
		t.Errorf("method = %s", sawMethod)
	}
	if out.String() != "" {
		t.Errorf("expected no stdout, got %q", out.String())
	}
}
