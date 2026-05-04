package dashboard_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/internal/dashboard"
)

func TestTokenStore_OpenCreatesFileWith0600(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dashboard.token")
	s, err := dashboard.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Get() == "" {
		t.Fatal("token empty after Open")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("mode = %o, want 0600", mode)
	}
}

func TestTokenStore_OpenLoadsExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dashboard.token")
	s1, err := dashboard.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := s1.Get()

	s2, err := dashboard.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.Get(); got != want {
		t.Fatalf("token reload mismatch: got %q, want %q", got, want)
	}
}

func TestTokenStore_OpenEmptyFileGetsRotated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dashboard.token")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := dashboard.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Get() == "" {
		t.Fatal("empty file should have been rotated to a fresh token")
	}
}

func TestTokenStore_Rotate_OverwritesAndReturns(t *testing.T) {
	dir := t.TempDir()
	s, err := dashboard.Open(filepath.Join(dir, "dashboard.token"))
	if err != nil {
		t.Fatal(err)
	}
	old := s.Get()
	new1, err := s.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	if new1 == old {
		t.Fatal("Rotate did not change token")
	}
	if s.Get() != new1 {
		t.Fatal("Get after Rotate mismatch")
	}
}

func TestTokenStore_Verify_RejectsBadAndEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := dashboard.Open(filepath.Join(dir, "dashboard.token"))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Verify(s.Get()) {
		t.Fatal("Verify rejected current token")
	}
	if s.Verify("") {
		t.Fatal("Verify accepted empty")
	}
	if s.Verify("not-the-right-length") {
		t.Fatal("Verify accepted wrong token")
	}
}

func TestTokenStore_ConcurrentRotateAndGet(t *testing.T) {
	dir := t.TempDir()
	s, err := dashboard.Open(filepath.Join(dir, "dashboard.token"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.Get() }()
		go func() { defer wg.Done(); _, _ = s.Rotate() }()
	}
	wg.Wait()
	if s.Get() == "" {
		t.Fatal("token lost under concurrent access")
	}
}
