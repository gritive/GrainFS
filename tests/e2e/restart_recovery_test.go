package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Restart recovery orphan sweep", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("sweeps startup orphan artifacts and persists heal events", func() {
			runRestartRecoveryOrphanSweepCases(ginkgo.GinkgoTB())
		})
	})
})

func runRestartRecoveryOrphanSweepCases(t testing.TB) {
	t.Helper()
	binary := getBinary()
	dir, err := os.MkdirTemp("", "grainfs-restart-recovery-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(os.RemoveAll, dir)

	// Plant a stale .tmp file (backdated past the 5-min in-flight guard).
	staleTmp := filepath.Join(dir, "shards", "b", "k", "0.tmp")
	gomega.Expect(os.MkdirAll(filepath.Dir(staleTmp), 0o755)).To(gomega.Succeed())
	gomega.Expect(os.WriteFile(staleTmp, []byte("partial"), 0o644)).To(gomega.Succeed())
	past := time.Now().Add(-30 * time.Minute)
	gomega.Expect(os.Chtimes(staleTmp, past, past)).To(gomega.Succeed())

	// Plant an abandoned multipart upload (older than 24h).
	staleUpload := filepath.Join(dir, "parts", "abandoned-id")
	gomega.Expect(os.MkdirAll(staleUpload, 0o755)).To(gomega.Succeed())
	gomega.Expect(os.WriteFile(filepath.Join(staleUpload, "00001"), []byte("p"), 0o644)).To(gomega.Succeed())
	veryPast := time.Now().Add(-25 * time.Hour)
	gomega.Expect(os.Chtimes(staleUpload, veryPast, veryPast)).To(gomega.Succeed())

	port := freePort()
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() { _ = cmd.Process.Kill() })

	waitForPort(t, port, 5*time.Second)

	// Sanity: orphan artifacts must be gone now that startup recovery ran.
	_, statErr := os.Stat(staleTmp)
	gomega.Expect(os.IsNotExist(statErr)).To(gomega.BeTrue(), "stale .tmp should have been removed by startup recovery")
	_, statErr = os.Stat(staleUpload)
	gomega.Expect(os.IsNotExist(statErr)).To(gomega.BeTrue(), "abandoned multipart dir should have been removed")

	// Eventstore writes are async (bounded queue + worker) so retry briefly.
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	var (
		healEvents     []map[string]any
		fetchErr       error
		hasOrphanTmp   bool
		hasOrphanMulti bool
	)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		healEvents, fetchErr = fetchHealEvents(endpoint)
		if fetchErr == nil {
			hasOrphanTmp, hasOrphanMulti = scanForRestartCleanups(healEvents)
			if hasOrphanTmp && hasOrphanMulti {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
	}

	gomega.Expect(fetchErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(hasOrphanTmp).To(gomega.BeTrue(), "expected an orphan_tmp startup HealEvent in eventstore, got: %v", healEvents)
	gomega.Expect(hasOrphanMulti).To(gomega.BeTrue(), "expected an orphan_multipart startup HealEvent in eventstore, got: %v", healEvents)
}

func fetchHealEvents(endpoint string) ([]map[string]any, error) {
	resp, err := http.Get(endpoint + "/api/eventlog?type=heal&since=3600&limit=200")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("eventlog status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var events []map[string]any
	if err := json.Unmarshal(body, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func scanForRestartCleanups(events []map[string]any) (orphanTmp, orphanMulti bool) {
	for _, e := range events {
		if e["phase"] != "startup" {
			continue
		}
		switch e["err_code"] {
		case "orphan_tmp":
			orphanTmp = true
		case "orphan_multipart":
			orphanMulti = true
		}
	}
	return
}
