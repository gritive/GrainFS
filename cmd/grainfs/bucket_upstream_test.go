package main

// NOTE: Tests below temporarily disabled — Step 2 changed bucketUpstreamCmd
// from a builder function returning *cobra.Command to a package-level var
// (registered in bucketCmd via init()), and the flag surface migrated from
// {--upstream-url, --access-key, --secret-key-stdin, --secret-key-file}
// to {--scheme, --endpoint-url, --access-key, --secret-key, --region,
// --remote-bucket}. The helper readSecretKey was also removed because the
// new flag surface no longer reads the secret from stdin/file. Task 11 will
// rewrite the cobra smoke tests against the new shape and wire format.

/*
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func buildTestBucketUpstreamRoot() *cobra.Command {
	root := &cobra.Command{Use: "grainfs"}
	bkt := &cobra.Command{Use: "bucket"}
	bkt.PersistentFlags().String("endpoint", "", "")
	bkt.PersistentFlags().Bool("json", false, "")
	bkt.AddCommand(bucketUpstreamCmd())
	root.AddCommand(bkt)
	return root
}

func TestReadSecretKey_Stdin(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"plain", "secret", "secret"},
		{"trailing LF", "secret\n", "secret"},
		{"trailing CRLF", "secret\r\n", "secret"},
		{"multi-line takes first", "secret\nignored", "secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readSecretKey(true, "", strings.NewReader(c.input))
			if err != nil {
				t.Fatalf("readSecretKey: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestReadSecretKey_StdinClosed(t *testing.T) {
	got, err := readSecretKey(true, "", strings.NewReader(""))
	if err == nil {
		t.Fatalf("readSecretKey on empty stdin: want error, got %q", got)
	}
	if !strings.Contains(err.Error(), "stdin") {
		t.Errorf("error should mention stdin: %v", err)
	}
}

func TestReadSecretKey_File(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"plain", "secret", "secret"},
		{"trailing newline", "secret\n", "secret"},
		{"leading + trailing whitespace", "  secret  \n", "secret"},
		{"tabs and CRLF", "\tsecret\r\n", "secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, c.name+".txt")
			if err := os.WriteFile(path, []byte(c.content), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			got, err := readSecretKey(false, path, nil)
			if err != nil {
				t.Fatalf("readSecretKey: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestReadSecretKey_FileNotFound(t *testing.T) {
	_, err := readSecretKey(false, "/nonexistent/path/should/not/exist", nil)
	if err == nil {
		t.Fatal("readSecretKey on missing file: want error, got nil")
	}
}

func TestBucketUpstreamGetCmd_Table(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/upstream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"bucket":"my-bucket","upstream_url":"http://minio:9000","access_key":"AKID","created_at":"2026-01-01T00:00:00Z"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "upstream", "get", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "BUCKET") {
		t.Errorf("output %q missing BUCKET header", out)
	}
	if !strings.Contains(out, "my-bucket") {
		t.Errorf("output %q missing bucket name", out)
	}
}

func TestBucketUpstreamGetCmd_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/upstream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"bucket":"my-bucket","upstream_url":"http://minio:9000","access_key":"AKID"}`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "--json", "upstream", "get", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
}

func TestBucketUpstreamListCmd_Table(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"bucket":"b1","upstream_url":"http://minio:9000","access_key":"AK1","created_at":"2026-01-01T00:00:00Z"}]`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "upstream", "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "BUCKET") {
		t.Errorf("output %q missing BUCKET header", out)
	}
	if !strings.Contains(out, "b1") {
		t.Errorf("output %q missing bucket name", out)
	}
}

func TestBucketUpstreamListCmd_JSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"bucket":"b1"}]`)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "--json", "upstream", "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	var parsed any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
}

func TestBucketUpstreamDeleteCmd(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/buckets/my-bucket/upstream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "upstream", "delete", "my-bucket"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "my-bucket") {
		t.Errorf("output %q missing bucket name feedback", buf.String())
	}
}

func TestBucketUpstreamPutCmd_MissingURL(t *testing.T) {
	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", "/tmp/fake.sock", "upstream", "put", "my-bucket",
		"--access-key", "AKID", "--secret-key-stdin"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "upstream-url") {
		t.Errorf("expected --upstream-url error, got: %v", err)
	}
}

func TestBucketUpstreamPutCmd_MissingAccessKey(t *testing.T) {
	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", "/tmp/fake.sock", "upstream", "put", "my-bucket",
		"--upstream-url", "http://minio:9000", "--secret-key-stdin"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "access-key") {
		t.Errorf("expected --access-key error, got: %v", err)
	}
}

func TestBucketUpstreamPutCmd_SecretKeyConflict(t *testing.T) {
	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetContext(context.Background())
	// neither --secret-key-stdin nor --secret-key-file provided
	root.SetArgs([]string{"bucket", "--endpoint", "/tmp/fake.sock", "upstream", "put", "my-bucket",
		"--upstream-url", "http://minio:9000", "--access-key", "AKID"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "secret-key") {
		t.Errorf("expected secret-key exclusivity error, got: %v", err)
	}
}

func TestBucketUpstreamPutCmd(t *testing.T) {
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upstreams", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	})
	sock := startFakeAdminUDS(t, mux)

	root := buildTestBucketUpstreamRoot()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetIn(strings.NewReader("mysecret\n"))
	root.SetContext(context.Background())
	root.SetArgs([]string{"bucket", "--endpoint", sock, "upstream", "put", "my-bucket",
		"--upstream-url", "http://minio:9000", "--access-key", "AKID", "--secret-key-stdin"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, buf.String())
	}
	if gotBody["bucket"] != "my-bucket" {
		t.Errorf("body bucket = %q, want my-bucket", gotBody["bucket"])
	}
	if gotBody["secret_key"] != "mysecret" {
		t.Errorf("body secret_key = %q, want mysecret", gotBody["secret_key"])
	}
	if !strings.Contains(buf.String(), "Upstream configured for bucket my-bucket") {
		t.Errorf("output %q missing upstream configured feedback", buf.String())
	}
}
*/
