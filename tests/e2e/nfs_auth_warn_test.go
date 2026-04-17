package e2e

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNFS_NullAuthWarning verifies that starting the server with NFS enabled
// prints a security warning to stdout about null (unauthenticated) NFS access.
func TestNFS_NullAuthWarning(t *testing.T) {
	binary := getBinary()
	dir, err := os.MkdirTemp("", "grainfs-nfsauth-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	port := freePort()
	nfsPort := freePort()

	var stdout bytes.Buffer
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs-port", fmt.Sprintf("%d", nfsPort),
	)
	cmd.Stdout = &stdout
	cmd.Stderr = &stdout

	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	waitForPort(port, 5*time.Second)

	cmd.Process.Kill()
	cmd.Wait()

	output := stdout.String()
	assert.Contains(t, output, "WARNING",
		"Server stdout must contain a null auth warning when NFS is started without authentication")
	assert.Contains(t, output, "null auth",
		"Warning must mention 'null auth' so operators know NFS access is unauthenticated")
}

// TestNFS_NoWarnWhenNFSDisabled verifies that no NFS null auth warning appears
// when NFS is not enabled (--nfs-port 0 or not specified).
func TestNFS_NoWarnWhenNFSDisabled(t *testing.T) {
	binary := getBinary()
	dir, err := os.MkdirTemp("", "grainfs-nonfs-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	port := freePort()

	var stdout bytes.Buffer
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs-port", "0",
	)
	cmd.Stdout = &stdout
	cmd.Stderr = &stdout

	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	waitForPort(port, 5*time.Second)

	cmd.Process.Kill()
	cmd.Wait()

	output := stdout.String()
	assert.NotContains(t, output, "null auth",
		"No null auth warning when NFS is disabled")
}
