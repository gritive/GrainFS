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
	}
}

func requireNBDInteropTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"qemu-io", "qemu-nbd", "nbdinfo"} {
		if _, err := exec.LookPath(tool); err != nil {
		}
	}
}
