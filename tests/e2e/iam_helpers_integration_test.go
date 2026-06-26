//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
)

// bootstrapAdminViaUDS performs the post-serve admin SA bootstrap via the
// admin UDS. Returns the access_key/secret_key pair for use in subsequent
// S3 sigv4 requests. Replaces the legacy --access-key/--secret-key flag
// pattern. Caller must have started `grainfs serve` and waited for the
// admin socket to exist at <dataDir>/admin.sock.
func bootstrapAdminViaUDS(t testing.TB, dataDir string) (accessKey, secretKey string) {
	t.Helper()
	sock := filepath.Join(dataDir, "admin.sock")

	// Wait up to 10s for socket to appear (cluster bootstrap may need time).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admin socket %s did not appear within 10s", sock)
		}
		time.Sleep(50 * time.Millisecond)
	}

	client := iamUDSClient(sock)
	body := strings.NewReader(`{"name":"admin","description":"e2e bootstrap"}`)
	req, err := http.NewRequestWithContext(context.Background(), "POST",
		"http://unix/v1/iam/sa", body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	gomega.Expect(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated).To(gomega.BeTrue(),
		"bootstrap via %s: got %d", sock, resp.StatusCode)

	var out struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&out)).To(gomega.Succeed())
	gomega.Expect(out.AccessKey).NotTo(gomega.BeEmpty())
	gomega.Expect(out.SecretKey).NotTo(gomega.BeEmpty())
	return out.AccessKey, out.SecretKey
}
