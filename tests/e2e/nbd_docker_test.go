package e2e

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// runNBDDockerTest builds (if needed) and runs the grainfs-nbd-test image
// with the given shell script. It handles SKIP and PASS/FAIL detection.
func runNBDDockerTest(t *testing.T, script, passMarker string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found, skipping NBD Docker test")
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker not running: %s", out)
	}

	projectRoot := "../.."

	t.Log("Building NBD test Docker image...")
	build := exec.Command("docker", "build", "-t", "grainfs-nbd-test", "-f", "docker/nbd-test.Dockerfile", ".")
	build.Dir = projectRoot
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("docker build failed: %v", err)
	}

	run := exec.Command("docker", "run", "--rm", "--privileged",
		"-v", "/lib/modules:/lib/modules:ro",
		"grainfs-nbd-test",
		"bash", script)
	run.Dir = projectRoot
	out, err := run.CombinedOutput()
	output := string(out)
	t.Log(output)

	if strings.Contains(output, "SKIP:") {
		t.Skip("NBD kernel module not available in this Docker environment")
	}
	if err != nil {
		t.Fatalf("docker run failed: %v", err)
	}
	if !strings.Contains(output, passMarker) {
		t.Fatalf("NBD Docker test did not report %q", passMarker)
	}
}

func TestNBD_Docker(t *testing.T) {
	runNBDDockerTest(t, "docker/nbd-test.sh", "Docker NBD Test: PASS")
}

// TestNBD_CoW_SnapshotRollback verifies CoW snapshot+rollback via the NBD block device:
// write pattern → snapshot → overwrite → rollback → original pattern restored.
func TestNBD_CoW_SnapshotRollback(t *testing.T) {
	runNBDDockerTest(t, "docker/nbd-cow-test.sh", "Docker NBD CoW Test: PASS")
}

// TestNBD_Dedup runs three dedup scenarios inside a Docker container via NBD:
// SavingsRatio, ReadConsistency, OverwriteRefcount.
func TestNBD_Dedup(t *testing.T) {
	runNBDDockerTest(t, "docker/nbd-dedup-test.sh", "Docker NBD Dedup Test: PASS")
}
