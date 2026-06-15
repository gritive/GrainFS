package e2e

import (
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// runCLI runs the grainfs binary against the admin Unix socket under dataDir,
// returning combined stdout/stderr and the process exit code. When the caller
// supplies its own --endpoint flag it is left untouched.
func runCLI(t testing.TB, dataDir string, args ...string) (stdout string, exitCode int) {
	t.Helper()
	full := append([]string{}, args...)
	if !containsFlag(full, "--endpoint") {
		full = append(full, "--endpoint", filepath.Join(dataDir, "admin.sock"))
	}
	cmd := exec.Command(getBinary(), full...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	t.Fatalf("CLI run failed unexpectedly: %v\n%s", err, out)
	return "", 0
}

func containsFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}
