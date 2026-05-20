//go:build colima

package fuse_s3_colima

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Benchmark payload defaults to 64 MiB so startup costs do not dominate the
// wall clock. Tests can set FUSE_S3_BENCH_MB lower for quick Colima smoke runs.
func benchPayloadMB(b *testing.B) int {
	b.Helper()
	v := os.Getenv("FUSE_S3_BENCH_MB")
	if v == "" {
		return 64
	}
	n, err := strconv.Atoi(v)
	require.NoErrorf(b, err, "parse FUSE_S3_BENCH_MB=%q", v)
	require.Positive(b, n, "FUSE_S3_BENCH_MB")
	return n
}

// throughput measures wall-clock seconds elapsed for a payload of N bytes and
// logs MB/s in a uniform format.
func throughput(b *testing.B, label string, payloadBytes int64, fn func()) {
	b.Helper()
	start := time.Now()
	fn()
	elapsed := time.Since(start)
	mbs := float64(payloadBytes) / (1 << 20) / elapsed.Seconds()
	b.Logf("%-32s %5.1f MB/s   (%5.2fs for %d MiB)",
		label, mbs, elapsed.Seconds(), payloadBytes>>20)
}

// withRcloneMountB is the Benchmark variant of withRcloneMount.
func withRcloneMountB(b *testing.B, fn func(mnt string)) {
	b.Helper()
	if out, err := colimaSSH("which", "rclone").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) == "" {
		require.NoErrorf(b, err, "rclone not installed in colima VM\n%s", out)
		require.NotEmptyf(b, strings.TrimSpace(string(out)), "rclone not installed in colima VM\n%s", out)
	}
	if err := colimaSSH("test", "-e", "/dev/fuse").Run(); err != nil {
		require.NoError(b, err, "/dev/fuse not present in colima VM")
	}

	name := strings.ReplaceAll(b.Name(), "/", "-")
	mnt := "/tmp/grainfs-fuse-s3-bench-" + name
	cfgPath := "/tmp/rclone-fuse-s3-bench-" + name + ".conf"
	cfg := fmt.Sprintf(`[grainfs]
type = s3
provider = Other
access_key_id = %s
secret_access_key = %s
endpoint = http://%s:%s
region = us-east-1
force_path_style = true
s3_upload_cutoff = 128M
s3_disable_checksum = true
`, accessKey, secretKey, colimaHostIP, colimaHTTPPort)

	out, err := colimaSSH("bash", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", cfgPath, cfg)).CombinedOutput()
	require.NoErrorf(b, err, "write rclone config\n%s", out)
	if out, err := colimaSSH("rclone", "--config", cfgPath, "mkdir", "grainfs:"+bucket).CombinedOutput(); err != nil {
		require.NoErrorf(b, err, "rclone mkdir\n%s", out)
	}
	if out, err := colimaSSH("mkdir", "-p", mnt).CombinedOutput(); err != nil {
		require.NoErrorf(b, err, "mkdir mountpoint\n%s", out)
	}

	mountArgs := []string{"--config", cfgPath, "mount",
		"grainfs:" + bucket, mnt,
		"--daemon",
		"--s3-upload-cutoff", "128M",
		"--vfs-cache-mode", "writes",
		"--dir-cache-time", "1s",
	}
	if out, err := colimaSSH(append([]string{"rclone"}, mountArgs...)...).CombinedOutput(); err != nil {
		require.NoErrorf(b, err, "rclone mount\n%s", out)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := colimaSSH("mountpoint", "-q", mnt).Run(); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := colimaSSH("mountpoint", "-q", mnt).Run(); err != nil {
		require.NoError(b, err, "rclone mount should become ready within 15s")
	}

	b.Cleanup(func() {
		colimaSSH("fusermount3", "-u", mnt).Run()    //nolint:errcheck
		colimaSSH("fusermount", "-u", mnt).Run()     //nolint:errcheck
		colimaSSH("sudo", "umount", "-l", mnt).Run() //nolint:errcheck
		colimaSSH("rmdir", mnt).Run()                //nolint:errcheck
		colimaSSH("rm", "-f", cfgPath).Run()         //nolint:errcheck
	})

	fn(mnt)
}

// BenchmarkFUSE_S3_Throughput compares sequential write/read throughput across
// three paths against the same GrainFS server, all from the Colima VM:
//
//  1. rclone mount + dd  — FUSE-over-S3 path (this is what users get)
//  2. rclone copyto/copy — direct S3 PUT/GET, no FUSE involved
//
// Numbers are logged to test output (b.Logf), not reported as ns/op since the
// payload size is fixed.
func BenchmarkFUSE_S3_Throughput(b *testing.B) {
	payloadMB := benchPayloadMB(b)
	payloadBytes := int64(payloadMB) << 20

	// Pre-stage a payload file inside the VM. Reused across sub-benchmarks so
	// the source data (and disk cache) is identical between FUSE and direct.
	srcPath := "/tmp/grainfs-bench-src.bin"
	out, err := colimaSSH("bash", "-c", fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=1M count=%d 2>/dev/null && sha256sum %s | awk '{print $1}'",
		srcPath, payloadMB, srcPath)).CombinedOutput()
	require.NoErrorf(b, err, "stage payload\n%s", out)
	srcSum := strings.TrimSpace(string(out))
	b.Logf("payload: %d MiB at %s, sha256=%s", payloadMB, srcPath, srcSum)
	b.Cleanup(func() { colimaSSH("rm", "-f", srcPath).Run() }) //nolint:errcheck

	// rclone config for direct (non-FUSE) S3 operations.
	directCfg := "/tmp/rclone-direct.conf"
	cfg := fmt.Sprintf(`[grainfs]
type = s3
provider = Other
access_key_id = %s
secret_access_key = %s
endpoint = http://%s:%s
region = us-east-1
force_path_style = true
s3_upload_cutoff = 128M
s3_disable_checksum = true
`, accessKey, secretKey, colimaHostIP, colimaHTTPPort)
	if out, err := colimaSSH("bash", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", directCfg, cfg)).CombinedOutput(); err != nil {
		require.NoErrorf(b, err, "write direct config\n%s", out)
	}
	if out, err := colimaSSH("rclone", "--config", directCfg, "mkdir", "grainfs:"+bucket).CombinedOutput(); err != nil {
		require.NoErrorf(b, err, "mkdir bucket\n%s", out)
	}
	b.Cleanup(func() { colimaSSH("rm", "-f", directCfg).Run() }) //nolint:errcheck

	b.Run("Direct_S3_Write", func(b *testing.B) {
		key := fmt.Sprintf("grainfs:%s/bench-direct.bin", bucket)
		throughput(b, "direct S3 PUT", payloadBytes, func() {
			out, err := colimaSSH("rclone", "--config", directCfg,
				"copyto", srcPath, key).CombinedOutput()
			require.NoErrorf(b, err, "direct PUT\n%s", out)
		})
		b.Cleanup(func() {
			colimaSSH("rclone", "--config", directCfg, "deletefile", key).Run() //nolint:errcheck
		})
	})

	b.Run("Direct_S3_Read", func(b *testing.B) {
		key := fmt.Sprintf("grainfs:%s/bench-direct-read.bin", bucket)
		dstPath := "/tmp/grainfs-bench-direct-read.bin"
		// Stage object once.
		if out, err := colimaSSH("rclone", "--config", directCfg,
			"copyto", srcPath, key).CombinedOutput(); err != nil {
			require.NoErrorf(b, err, "stage read object\n%s", out)
		}
		b.Cleanup(func() {
			colimaSSH("rclone", "--config", directCfg, "deletefile", key).Run() //nolint:errcheck
			colimaSSH("rm", "-f", dstPath).Run()                                //nolint:errcheck
		})
		throughput(b, "direct S3 GET", payloadBytes, func() {
			out, err := colimaSSH("bash", "-c", fmt.Sprintf(
				"rclone --config %s cat %s > %s",
				directCfg, key, dstPath)).CombinedOutput()
			require.NoErrorf(b, err, "direct GET\n%s", out)
		})
	})

	b.Run("FUSE_Mount_Write", func(b *testing.B) {
		withRcloneMountB(b, func(mnt string) {
			dst := mnt + "/bench-fuse.bin"
			throughput(b, "FUSE write (dd via rclone mount)", payloadBytes, func() {
				// `cp` triggers open(O_WRONLY|O_CREAT) + write loop + close.
				// With vfs-cache-mode=writes, rclone may finish the remote upload
				// after close; functional coverage verifies the round trip.
				out, err := colimaSSH("bash", "-c", fmt.Sprintf("timeout 30s cp %s %s", srcPath, dst)).CombinedOutput()
				require.NoErrorf(b, err, "FUSE write\n%s", out)
			})
			colimaSSH("rm", "-f", dst).Run() //nolint:errcheck
		})
	})

	b.Run("FUSE_Mount_Read", func(b *testing.B) {
		// Stage the read object via direct S3 PUT (not through the FUSE mount)
		// so it's not in any rclone-side cache when we open the new mount.
		key := fmt.Sprintf("grainfs:%s/bench-fuse-read.bin", bucket)
		if out, err := colimaSSH("rclone", "--config", directCfg,
			"copyto", srcPath, key).CombinedOutput(); err != nil {
			require.NoErrorf(b, err, "stage read object via direct S3\n%s", out)
		}
		b.Cleanup(func() {
			colimaSSH("rclone", "--config", directCfg, "deletefile", key).Run() //nolint:errcheck
		})

		withRcloneMountB(b, func(mnt string) {
			src := mnt + "/bench-fuse-read.bin"
			throughput(b, "FUSE read (dd via rclone mount)", payloadBytes, func() {
				// /dev/null sink. With --vfs-cache-mode off, rclone streams via
				// range GETs from S3 (no local cache hit).
				out, err := colimaSSH("bash", "-c", fmt.Sprintf(
					"dd if=%s of=/dev/null bs=1M 2>/dev/null", src)).CombinedOutput()
				require.NoErrorf(b, err, "FUSE read\n%s", out)
			})
		})
	})
}
