package e2e

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestNBD_Docker(t *testing.T) {
	// Skip if docker is not available
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found, skipping NBD Docker test")
	}

	// Check docker is running
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker not running: %s", out)
	}

	// Find project root (relative to tests/e2e/)
	projectRoot := "../.."

	// Build Docker image
	t.Log("Building NBD test Docker image...")
	build := exec.Command("docker", "build", "-t", "grainfs-nbd-test", "-f", "docker/nbd-test.Dockerfile", ".")
	build.Dir = projectRoot
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("docker build failed: %v", err)
	}

	// Run NBD test container
	t.Log("Running NBD E2E test in Docker...")
	run := exec.Command("docker", "run", "--rm", "--privileged",
		"-v", "/lib/modules:/lib/modules:ro",
		"grainfs-nbd-test")
	run.Dir = projectRoot
	out, err := run.CombinedOutput()
	output := string(out)
	t.Log(output)

	// SKIP is acceptable (NBD module not available in Docker Desktop)
	if strings.Contains(output, "SKIP:") {
		t.Skip("NBD kernel module not available in this Docker environment")
	}

	if err != nil {
		t.Fatalf("docker run failed: %v", err)
	}

	if !strings.Contains(output, "Docker NBD Test: PASS") {
		t.Fatal("NBD Docker test did not report PASS")
	}
}
