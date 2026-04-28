package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNFSWriteAmplification_dd200MB verifies Phase 1 acceptance:
// dd if=/dev/zero of=/mnt/g/test.dat bs=1M count=200 leaves the
// grainfs data dir well under 250 MB (vs ~17 GB without the fix).
//
// Requires Linux + ability to mount NFS + sudo. Skips otherwise.
func TestNFSWriteAmplification_dd200MB(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux for NFS mount")
	}
	if os.Getuid() != 0 {
		t.Skip("requires root for NFS mount")
	}

	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	mntDir := filepath.Join(tmp, "mnt")
	require.NoError(t, os.MkdirAll(mntDir, 0o755))

	bin := os.Getenv("GRAINFS_BIN")
	if bin == "" {
		bin = "./bin/grainfs"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("GRAINFS_BIN not found at %s; run `make build` first", bin)
	}
	abs, _ := filepath.Abs(bin)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpPort := "19100"
	nfsPort := "19102"

	cmd := exec.CommandContext(ctx, abs, "serve",
		"--data", dataDir,
		"--port", httpPort,
		"--nfs-port", nfsPort,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--no-encryption",
		"--nbd-volume-size", "1073741824",
		"--rate-limit-ip-rps", "1000000", "--rate-limit-ip-burst", "1000000",
		"--rate-limit-user-rps", "1000000", "--rate-limit-user-burst", "1000000",
	)
	logFile, err := os.Create(filepath.Join(tmp, "grainfs.log"))
	require.NoError(t, err)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())
	defer func() {
		cancel()
		_ = cmd.Wait()
	}()

	// Wait for HTTP ready.
	deadline := time.Now().Add(20 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/healthz", httpPort))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.True(t, ready, "grainfs did not become ready; see %s", logFile.Name())

	// Mount NFS.
	mountOpts := fmt.Sprintf("vers=3,proto=tcp,mountproto=tcp,port=%s,mountport=%s,nolock,noacl,rsize=1048576,wsize=1048576,hard", nfsPort, nfsPort)
	out, err := exec.Command("mount", "-t", "nfs", "-o", mountOpts, "127.0.0.1:/default", mntDir).CombinedOutput()
	require.NoError(t, err, "mount failed: %s", string(out))
	defer func() {
		_ = exec.Command("umount", "-f", mntDir).Run()
	}()

	// dd 200 MB.
	dd := exec.Command("dd", "if=/dev/zero", "of="+filepath.Join(mntDir, "test.dat"),
		"bs=1M", "count=200")
	out, err = dd.CombinedOutput()
	require.NoError(t, err, "dd failed: %s", string(out))

	// Force any deferred writes to land. NFS COMMIT is currently a no-op in
	// upstream go-nfs but documents intent for Phase 2 (cache layer).
	require.NoError(t, exec.Command("sync").Run())

	// Measure backend disk usage.
	usage := dirSizeBytes(t, dataDir)
	t.Logf("backend data dir size after 200 MB dd: %d bytes (%d MB)", usage, usage>>20)

	// Acceptance: under 250 MB. Without Phase 1 this was 15-20 GB.
	require.Less(t, usage, int64(250*1024*1024),
		"backend disk usage %d MB exceeds 250 MB cap — Phase 1 fix not effective",
		usage>>20)
}

// dirSizeBytes recursively sums regular file sizes under root.
func dirSizeBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	require.NoError(t, filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	}))
	return total
}
