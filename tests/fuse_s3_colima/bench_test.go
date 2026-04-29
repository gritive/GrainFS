//go:build colima

package fuse_s3_colima

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Benchmark sizes. Kept modest so the suite finishes in tens of seconds, but
// large enough that startup costs (rclone mount, dir cache warm-up) don't
// dominate the wall clock.
const (
	benchPayloadMB    = 64    // size of the test object in MiB
	benchPayloadBytes = benchPayloadMB << 20
)

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
		b.Skip("rclone not installed in colima VM")
	}
	if err := colimaSSH("test", "-e", "/dev/fuse").Run(); err != nil {
		b.Skip("/dev/fuse not present in colima VM")
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
`, accessKey, secretKey, colimaHostIP, colimaHTTPPort)

	out, err := colimaSSH("bash", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", cfgPath, cfg)).CombinedOutput()
	if err != nil {
		b.Fatalf("write rclone config: %v\n%s", err, out)
	}
	if out, err := colimaSSH("rclone", "--config", cfgPath, "mkdir", "grainfs:"+bucket).CombinedOutput(); err != nil {
		b.Fatalf("rclone mkdir: %v\n%s", err, out)
	}
	if out, err := colimaSSH("mkdir", "-p", mnt).CombinedOutput(); err != nil {
		b.Fatalf("mkdir mountpoint: %v\n%s", err, out)
	}

	// vfs-cache-mode=off makes close(2) on a write fd block until the S3 PUT
	// completes, and reads stream from S3 via range GET (no local cache hit).
	// This is what we want for benchmarking — `--vfs-cache-mode writes` would
	// give us inflated numbers (close returns when local cache write is done,
	// uploads happen async; subsequent reads hit the local cache).
	mountArgs := []string{"--config", cfgPath, "mount",
		"grainfs:" + bucket, mnt,
		"--daemon",
		"--vfs-cache-mode", "off",
		"--dir-cache-time", "1s",
	}
	if out, err := colimaSSH(append([]string{"rclone"}, mountArgs...)...).CombinedOutput(); err != nil {
		b.Fatalf("rclone mount: %v\n%s", err, out)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := colimaSSH("mountpoint", "-q", mnt).Run(); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := colimaSSH("mountpoint", "-q", mnt).Run(); err != nil {
		b.Fatalf("rclone mount did not become ready within 15s")
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
	// Pre-stage a payload file inside the VM. Reused across sub-benchmarks so
	// the source data (and disk cache) is identical between FUSE and direct.
	srcPath := "/tmp/grainfs-bench-src.bin"
	out, err := colimaSSH("bash", "-c", fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=1M count=%d 2>/dev/null && sha256sum %s | awk '{print $1}'",
		srcPath, benchPayloadMB, srcPath)).CombinedOutput()
	if err != nil {
		b.Fatalf("stage payload: %v\n%s", err, out)
	}
	srcSum := strings.TrimSpace(string(out))
	b.Logf("payload: %d MiB at %s, sha256=%s", benchPayloadMB, srcPath, srcSum)
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
`, accessKey, secretKey, colimaHostIP, colimaHTTPPort)
	if out, err := colimaSSH("bash", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", directCfg, cfg)).CombinedOutput(); err != nil {
		b.Fatalf("write direct config: %v\n%s", err, out)
	}
	if out, err := colimaSSH("rclone", "--config", directCfg, "mkdir", "grainfs:"+bucket).CombinedOutput(); err != nil {
		b.Fatalf("mkdir bucket: %v\n%s", err, out)
	}
	b.Cleanup(func() { colimaSSH("rm", "-f", directCfg).Run() }) //nolint:errcheck

	b.Run("Direct_S3_Write", func(b *testing.B) {
		key := fmt.Sprintf("grainfs:%s/bench-direct.bin", bucket)
		throughput(b, "direct S3 PUT", benchPayloadBytes, func() {
			out, err := colimaSSH("rclone", "--config", directCfg,
				"--s3-disable-checksum", "copyto", srcPath, key).CombinedOutput()
			if err != nil {
				b.Fatalf("direct PUT: %v\n%s", err, out)
			}
		})
		b.Cleanup(func() {
			colimaSSH("rclone", "--config", directCfg, "deletefile", key).Run() //nolint:errcheck
		})
	})

	b.Run("Direct_S3_Read", func(b *testing.B) {
		key := fmt.Sprintf("grainfs:%s/bench-direct-read.bin", bucket)
		dstPath := "/tmp/grainfs-bench-direct-read.bin"
		// Stage object once.
		if out, err := colimaSSH("rclone", "--config", directCfg, "copyto", srcPath, key).CombinedOutput(); err != nil {
			b.Fatalf("stage read object: %v\n%s", err, out)
		}
		b.Cleanup(func() {
			colimaSSH("rclone", "--config", directCfg, "deletefile", key).Run() //nolint:errcheck
			colimaSSH("rm", "-f", dstPath).Run()                                 //nolint:errcheck
		})
		throughput(b, "direct S3 GET", benchPayloadBytes, func() {
			out, err := colimaSSH("rclone", "--config", directCfg, "copyto", key, dstPath).CombinedOutput()
			if err != nil {
				b.Fatalf("direct GET: %v\n%s", err, out)
			}
		})
	})

	b.Run("FUSE_Mount_Write", func(b *testing.B) {
		withRcloneMountB(b, func(mnt string) {
			dst := mnt + "/bench-fuse.bin"
			throughput(b, "FUSE write (dd via rclone mount)", benchPayloadBytes, func() {
				// `cp` triggers open(O_WRONLY|O_CREAT) + write loop + close.
				// rclone --vfs-cache-mode writes flushes on close, so the timer
				// MUST include the close. Use bash to ensure that.
				out, err := colimaSSH("bash", "-c", fmt.Sprintf("cp %s %s && sync", srcPath, dst)).CombinedOutput()
				if err != nil {
					b.Fatalf("FUSE write: %v\n%s", err, out)
				}
			})
			colimaSSH("rm", "-f", dst).Run() //nolint:errcheck
		})
	})

	b.Run("FUSE_Mount_Read", func(b *testing.B) {
		// Stage the read object via direct S3 PUT (not through the FUSE mount)
		// so it's not in any rclone-side cache when we open the new mount.
		key := fmt.Sprintf("grainfs:%s/bench-fuse-read.bin", bucket)
		if out, err := colimaSSH("rclone", "--config", directCfg, "copyto", srcPath, key).CombinedOutput(); err != nil {
			b.Fatalf("stage read object via direct S3: %v\n%s", err, out)
		}
		b.Cleanup(func() {
			colimaSSH("rclone", "--config", directCfg, "deletefile", key).Run() //nolint:errcheck
		})

		withRcloneMountB(b, func(mnt string) {
			src := mnt + "/bench-fuse-read.bin"
			throughput(b, "FUSE read (dd via rclone mount)", benchPayloadBytes, func() {
				// /dev/null sink. With --vfs-cache-mode off, rclone streams via
				// range GETs from S3 (no local cache hit).
				out, err := colimaSSH("bash", "-c", fmt.Sprintf(
					"dd if=%s of=/dev/null bs=1M 2>/dev/null", src)).CombinedOutput()
				if err != nil {
					b.Fatalf("FUSE read: %v\n%s", err, out)
				}
			})
		})
	})
}
