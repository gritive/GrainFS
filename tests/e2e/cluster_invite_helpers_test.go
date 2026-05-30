package e2e

import (
	"bufio"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
)

// inviteBundleEnvKey is the env var a joining node reads its sealed invite
// bundle from at boot. The bundle is a secret (sealed KEK+PSK+cluster.id), so it
// is delivered via env, never argv.
const inviteBundleEnvKey = "GRAINFS_INVITE_BUNDLE"

// mintInvite runs `grainfs cluster invite create` against the leader's admin UDS
// and returns the bundle token. The admin UDS + meta-raft leadership + join
// listener take a moment to settle after the HTTP port opens, so it retries.
func mintInvite(t testing.TB, leaderDataDir string) string {
	t.Helper()
	sock := filepath.Join(leaderDataDir, "admin.sock")
	var out []byte
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command(getBinary(), "cluster", "invite", "create", "--endpoint", sock)
		out, lastErr = cmd.CombinedOutput()
		if lastErr == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	gomega.Expect(lastErr).NotTo(gomega.HaveOccurred(), "invite create must succeed; out:\n%s", string(out))
	return parseBundleToken(t, string(out))
}

// parseBundleToken extracts the bundle token printed after the
// "GRAINFS_INVITE_BUNDLE" prompt line of `cluster invite create`.
func parseBundleToken(t testing.TB, output string) string {
	t.Helper()
	sc := bufio.NewScanner(strings.NewReader(output))
	seenPrompt := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if seenPrompt && line != "" {
			return line
		}
		if strings.Contains(line, "GRAINFS_INVITE_BUNDLE") {
			seenPrompt = true
		}
	}
	t.Fatalf("could not parse invite bundle from output:\n%s", output)
	return ""
}
