package server

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func setupAuthServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	creds := []s3auth.Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}}
	srv := New(addr, backend, WithAuth(creds))
	go srv.Run()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr
}

func TestAuthRejectsUnsigned(t *testing.T) {
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestAuthAcceptsValidSignature(t *testing.T) {
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthRejectsWrongKey(t *testing.T) {
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "wrongsecret", "us-east-1")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestNoAuthServerAllowsAll(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 without auth, got %d", resp.StatusCode)
	}
}
