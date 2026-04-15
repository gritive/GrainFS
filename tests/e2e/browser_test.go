package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestObjectBrowser_ServesUI(t *testing.T) {
	resp, err := http.Get(testServerURL + "/ui/")
	if err != nil {
		t.Fatalf("get ui: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	html := string(body)

	// Verify it's the object browser, not just a status dashboard
	if !strings.Contains(html, "Object Browser") {
		t.Fatal("expected 'Object Browser' in UI response")
	}

	// Verify volume management tab exists
	if !strings.Contains(html, "Volumes") {
		t.Fatal("expected 'Volumes' tab in UI response")
	}
}
