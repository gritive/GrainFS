package iamadmin

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRunPDPSetToken_TokenFile(t *testing.T) {
	ctx := context.Background()

	t.Run("missing file errors", func(t *testing.T) {
		err := RunPDPSetToken(ctx, PDPSetTokenOptions{
			BaseOptions: BaseOptions{Endpoint: "http://127.0.0.1:0"},
			TokenFile:   filepath.Join(t.TempDir(), "does-not-exist"),
		})
		if err == nil {
			t.Fatal("expected error for missing token file")
		}
	})

	t.Run("empty file errors", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "empty")
		if err := os.WriteFile(p, []byte("\n\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		err := RunPDPSetToken(ctx, PDPSetTokenOptions{
			BaseOptions: BaseOptions{Endpoint: "http://127.0.0.1:0"},
			TokenFile:   p,
		})
		if err == nil {
			t.Fatal("expected error for empty token file")
		}
	})
}
