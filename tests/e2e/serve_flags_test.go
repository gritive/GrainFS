package e2e

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServeFlagsRejectionE2E is a regression gate locking the removal of
// --upstream / --upstream-access-key / --upstream-secret-key. cobra must
// reject each as "unknown flag" with non-zero exit. Same regression class
// as PR #258's incomplete --access-key removal — without this test, a
// future commit could silently reintroduce one of the flags. The rejection
// is binary CLI behavior and identical on both branches by design, but the
// SingleNode/Cluster4Node shape is kept for grep/inventory consistency.
func TestServeFlagsRejectionE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		_ = newSingleNodeS3Target()
		runServeFlagsRejectionCases(t)
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		_ = newSharedClusterS3Target(t)
		runServeFlagsRejectionCases(t)
	})
}

func runServeFlagsRejectionCases(t *testing.T) {
	t.Helper()
	binary := getBinary()

	cases := []struct {
		flag string
	}{
		{"--upstream"},
		{"--upstream-access-key"},
		{"--upstream-secret-key"},
	}
	for _, c := range cases {
		t.Run(c.flag, func(t *testing.T) {
			cmd := exec.Command(binary, "serve", c.flag, "value", "--data", "/tmp/_unused_serve_flags_test", "--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
			out, err := cmd.CombinedOutput()
			require.Error(t, err, "binary must exit non-zero when %s is present", c.flag)
			assert.Contains(t, string(out), "unknown flag", "stderr must mention 'unknown flag' for %s; got: %s", c.flag, string(out))
			assert.Contains(t, string(out), c.flag, "stderr should reference the rejected flag name; got: %s", string(out))
		})
	}
}
