package e2e

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServe_RejectsRemovedUpstreamFlags is a regression gate locking the
// removal of --upstream / --upstream-access-key / --upstream-secret-key.
// cobra must reject each as "unknown flag" with non-zero exit.
//
// Same regression class as PR #258's incomplete --access-key removal —
// without this test, a future commit could silently reintroduce one of the
// flags (e.g., from a copy-pasted ops example) and we wouldn't notice until
// users hit a confusing security exposure.
func TestServe_RejectsRemovedUpstreamFlags(t *testing.T) {
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
