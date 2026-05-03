package nbd_interop

import (
	"os/exec"
	"runtime"
	"testing"
)

func TestNBDInteropToolsAvailable(t *testing.T) {
	requireNBDInteropTools(t)
}

func TestNBDModernInteropSmoke(t *testing.T) {
	requireNBDInteropTools(t)
	if runtime.GOOS != "linux" {
		t.Skip("NBD qemu/libnbd smoke requires Linux TCP NBD client tooling")
	}
	t.Skip("qemu/libnbd command smoke is gated until the test harness owns a disposable GrainFS NBD port")
}

func requireNBDInteropTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"qemu-io", "qemu-nbd", "nbdinfo"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found; install qemu and libnbd to run NBD interop smoke", tool)
		}
	}
}
