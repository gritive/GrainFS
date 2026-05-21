package e2e

import (
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServeFlagsRejectionE2E is a regression gate locking the removal of
// --upstream / --upstream-access-key / --upstream-secret-key. cobra must
// reject each as "unknown flag" with non-zero exit. Same regression class
// as PR #258's incomplete --access-key removal — without this test, a
// future commit could silently reintroduce one of the flags. The rejection
// is binary CLI behavior and does not require a running fixture.
var _ = ginkgo.Describe("Serve flags rejection", func() {
	runServeFlagsRejectionCases()
})

func runServeFlagsRejectionCases() {
	binary := getBinary()

	cases := []struct {
		flag string
	}{
		{"--upstream"},
		{"--upstream-access-key"},
		{"--upstream-secret-key"},
	}
	for _, c := range cases {
		c := c
		ginkgo.It("rejects "+c.flag, func() {
			t := ginkgo.GinkgoTB()
			cmd := exec.Command(binary, "serve", c.flag, "value", "--data", "/tmp/_unused_serve_flags_test", "--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
			out, err := cmd.CombinedOutput()
			require.Error(t, err, "binary must exit non-zero when %s is present", c.flag)
			assert.Contains(t, string(out), "unknown flag", "stderr must mention 'unknown flag' for %s; got: %s", c.flag, string(out))
			assert.Contains(t, string(out), c.flag, "stderr should reference the rejected flag name; got: %s", string(out))
		})
	}
}
