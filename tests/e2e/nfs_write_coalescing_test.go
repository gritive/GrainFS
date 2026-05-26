package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// NFS write coalescing e2e: exercises WriteBuffer semantics (opWrite, opCommit,
// opSetAttr truncate) through a real colima kernel NFS4 mount. All assertions
// are behaviour-only via S3 GET/HEAD — no internal coalescing metric is tested
// here (that is Task 12's bench job).
//
// Default --nfs-write-buffer-idle is 30 s; tests rely on explicit COMMIT (dd
// conv=fdatasync) or SETATTR (truncate) rather than the idle flusher.
var _ = ginkgo.Describe("NFS write coalescing", func() {
	for _, tc := range []struct {
		name string
		mk   func() *nfsTarget
	}{
		{name: "SingleNode", mk: func() *nfsTarget { return newSingleNodeNFSTarget(ginkgo.GinkgoTB()) }},
		// Cluster path deferred per plan §"Cluster parity" out-of-scope for Task 11.
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt *nfsTarget
			ginkgo.BeforeEach(func() { tgt = tc.mk() })
			runNFSWriteCoalescingCases(func() *nfsTarget { return tgt })
		})
	}
})

func runNFSWriteCoalescingCases(getTgt func() *nfsTarget) {
	// Case 1: sequential writes in 8 × 128 KiB chunks followed by fsync
	// (dd conv=fdatasync triggers NFS COMMIT), then S3 GET must match.
	ginkgo.It("round-trips 1 MiB written in 8×128 KiB chunks after COMMIT", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket, _ := tgt.uniqueExport(t, "coalesce-seq")
		key := "seq.bin"

		mountDir, ok := mountNFSExportColima(t, tgt, bucket)
		if !ok {
			return // mountNFSExportColima calls ginkgo.Skip internally
		}
		nfsFilePath := mountDir + "/" + bucket + "/" + key

		const chunkSize = 128 * 1024
		const numChunks = 8
		original := makeDeterministicBytes("seq", chunkSize*numChunks)

		for i := range numChunks {
			chunk := original[i*chunkSize : (i+1)*chunkSize]
			gomega.Expect(nfsWriteAt(t, nfsFilePath, int64(i*chunkSize), chunk)).To(gomega.Succeed())
		}
		gomega.Expect(nfsFsync(t, nfsFilePath)).To(gomega.Succeed())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ginkgo.DeferCleanup(cancel)

		// FILE_SYNC ack makes Linux NFS skip COMMIT on fdatasync, so the
		// buffer flushes via the idle timer (fixture sets it to 1s).
		// Poll until backend reflects the buffered content.
		gomega.Eventually(func() string {
			return sha256Hex(s3GetObject(ctx, t, tgt.s3Client(tgt.leaderIdx), bucket, key))
		}, 10*time.Second, 200*time.Millisecond).Should(gomega.Equal(sha256Hex(original)),
			"S3 GET body sha256 must match original 1 MiB after idle flush")
	})

	// Case 2: SETATTR truncate-to-0 after pending writes → S3 HEAD must
	// report ContentLength == 0.  setattr is synchronous (opSetAttr discards
	// the write buffer immediately) so no idle-flush wait is needed.
	ginkgo.It("SETATTR truncate-to-0 after pending writes yields empty S3 object", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket, _ := tgt.uniqueExport(t, "coalesce-trunc")
		key := "trunc.bin"

		mountDir, ok := mountNFSExportColima(t, tgt, bucket)
		if !ok {
			return
		}
		nfsFilePath := mountDir + "/" + bucket + "/" + key

		data := makeDeterministicBytes("trunc", 1*1024*1024)
		gomega.Expect(nfsWriteAt(t, nfsFilePath, 0, data)).To(gomega.Succeed())

		// Truncate to zero via SETATTR — triggers opSetAttr which discards the
		// write buffer synchronously.
		gomega.Expect(nfsTruncate(t, nfsFilePath, 0)).To(gomega.Succeed())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ginkgo.DeferCleanup(cancel)

		gomega.Eventually(func() int64 {
			out, err := tgt.s3Client(tgt.leaderIdx).HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return -1
			}
			return aws.ToInt64(out.ContentLength)
		}, 10*time.Second, 200*time.Millisecond).Should(gomega.Equal(int64(0)),
			"S3 HEAD ContentLength must be 0 after truncate-to-0")
	})

	// Case 3: full-overwrite shortcut + buffered tail.
	// Write first (1 MiB), then second (512 KiB) at offset 0, then fsync.
	// S3 GET must return 1 MiB total: [0..512K] == second, [512K..1M] == first[512K:].
	ginkgo.It("partial overwrite at offset 0 preserves non-overwritten tail", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket, _ := tgt.uniqueExport(t, "coalesce-overwrite")
		key := "overwrite.bin"

		mountDir, ok := mountNFSExportColima(t, tgt, bucket)
		if !ok {
			return
		}
		nfsFilePath := mountDir + "/" + bucket + "/" + key

		const oneMiB = 1 * 1024 * 1024
		const halfMiB = 512 * 1024

		first := makeDeterministicBytes("first", oneMiB)
		second := makeDeterministicBytes("second", halfMiB)

		// Write full 1 MiB then fsync so server has a committed baseline.
		gomega.Expect(nfsWriteAt(t, nfsFilePath, 0, first)).To(gomega.Succeed())
		gomega.Expect(nfsFsync(t, nfsFilePath)).To(gomega.Succeed())

		// Overwrite first 512 KiB via the WriteBuffer path.
		gomega.Expect(nfsWriteAt(t, nfsFilePath, 0, second)).To(gomega.Succeed())
		gomega.Expect(nfsFsync(t, nfsFilePath)).To(gomega.Succeed())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ginkgo.DeferCleanup(cancel)

		// Poll until backend reflects the second write (1s idle flush).
		var got []byte
		gomega.Eventually(func() bool {
			got = s3GetObject(ctx, t, tgt.s3Client(tgt.leaderIdx), bucket, key)
			if len(got) != oneMiB {
				return false
			}
			return bytes.Equal(got[:halfMiB], second) && bytes.Equal(got[halfMiB:], first[halfMiB:])
		}, 10*time.Second, 200*time.Millisecond).Should(gomega.BeTrue(),
			"backend must reflect partial overwrite + preserved tail after idle flush")
		gomega.Expect(got).To(gomega.HaveLen(oneMiB))
		gomega.Expect(got[:halfMiB]).To(gomega.Equal(second), "[0..512K] must equal second write")
		gomega.Expect(got[halfMiB:]).To(gomega.Equal(first[halfMiB:]), "[512K..1M] must equal first write tail")
	})

	// Case 4: crash recovery — requires killing and restarting the shared
	// TestMain server which would break the rest of the suite.
	ginkgo.It("crash recovery preserves flushed data", func() {
		ginkgo.Skip("requires harness KillAndRestart — shared TestMain server cannot be killed; see docs/operators/runbook.md")
	})
}

// mountNFSExportColima mounts the server's NFS root inside the colima VM and
// waits until the export's bucket directory is visible. Returns (mountDir, true)
// on success or ("", false) after calling ginkgo.Skip when colima is unavailable.
// DeferCleanup is registered for umount+rmdir.
func mountNFSExportColima(t testing.TB, tgt *nfsTarget, bucket string) (string, bool) {
	t.Helper()

	if _, err := exec.LookPath("colima"); err != nil {
		ginkgo.Skip(fmt.Sprintf("colima not in PATH: %v", err))
		return "", false
	}
	if out, err := exec.Command("colima", "status").CombinedOutput(); err != nil {
		ginkgo.Skip(fmt.Sprintf("colima not running: %v\n%s", err, out))
		return "", false
	}

	hostIP := os.Getenv("HOST_IP")
	if hostIP == "" {
		hostIP = "192.168.5.2"
	}

	name := fmt.Sprintf("grainfs-nfs-coalesce-%d", time.Now().UnixNano())
	mountDir := "/mnt/" + name

	runColimaSSH(t, "sudo", "mkdir", "-p", mountDir)
	ginkgo.DeferCleanup(func() {
		_ = colimaSSH("sudo", "umount", "-l", mountDir).Run()
		_ = colimaSSH("sudo", "rmdir", mountDir).Run()
	})

	nfsAddr := tgt.nfsAddr(tgt.leaderIdx)
	// nfsAddr returns "host:port"; extract the port part.
	port := nfsAddr
	for i := len(nfsAddr) - 1; i >= 0; i-- {
		if nfsAddr[i] == ':' {
			port = nfsAddr[i+1:]
			break
		}
	}

	// Re-enable anon access so the NFS session (which has no MountSA binding)
	// passes the anonRejected() gate.  TestMain's bootstrap SA creation
	// auto-flips iam.anon-enabled → false; setConfigViaUDS is documented
	// specifically for this per-fixture re-enable case.
	sock := tgt.dataDir(tgt.leaderIdx) + "/admin.sock"
	gomega.Expect(setConfigViaUDS(sock, "iam.anon-enabled", "true")).
		To(gomega.Succeed(), "re-enable iam.anon-enabled for NFS anon session")
	ginkgo.DeferCleanup(func() {
		_ = setConfigViaUDS(sock, "iam.anon-enabled", "false")
	})

	if out, err := colimaSSHCombinedOutput(15*time.Second, "sudo", "mount", "-t", "nfs4",
		"-o", fmt.Sprintf("vers=4.1,port=%s,rw,hard,intr,timeo=600,retrans=2", port),
		fmt.Sprintf("%s:/", hostIP),
		mountDir,
	); err != nil {
		ginkgo.Skip(fmt.Sprintf("colima nfs4 mount failed: %v\n%s", err, out))
		return "", false
	}

	// Wait until the export's bucket directory is visible in the mount.
	// The NFS server exports bucket directories under the mount root.
	bucketDir := mountDir + "/" + bucket
	gomega.Eventually(func() bool {
		_, err := colimaSSHCombinedOutput(2*time.Second, "sudo", "ls", bucketDir)
		return err == nil
	}, 15*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(),
		"bucket directory %s not visible in NFS mount", bucketDir)

	return mountDir, true
}

// nfsWriteAt writes data to filePath at byte offset off via dd inside the
// colima VM. Uses stdin pipe to avoid ARG_MAX limits on large payloads
// (macOS 256 KiB limit). dd conv=fdatasync triggers an NFS COMMIT on close.
// filePath must be the full path inside the VM, e.g. mountDir/bucket/key.
func nfsWriteAt(t testing.TB, filePath string, off int64, data []byte) error {
	t.Helper()
	if len(data)%1024 != 0 {
		return fmt.Errorf("nfsWriteAt: data length %d must be a multiple of 1024 for dd bs=1024", len(data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seekBlocks := off / 1024
	countBlocks := len(data) / 1024
	cmd := exec.CommandContext(ctx, "colima", "ssh", "--",
		"sudo", "bash", "-c",
		fmt.Sprintf("dd of=%s seek=%d bs=1024 count=%d iflag=fullblock conv=notrunc,fdatasync status=none",
			filePath, seekBlocks, countBlocks),
	)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nfsWriteAt dd seek=%d count=%d: %w\n%s", seekBlocks, countBlocks, err, out)
	}
	return nil
}

// nfsFsync issues "sync <filePath>" inside the colima VM. For deterministic
// COMMIT semantics the dd in nfsWriteAt already uses conv=fdatasync; this
// helper provides an additional explicit flush barrier when needed.
// filePath must be the full path inside the VM.
func nfsFsync(t testing.TB, filePath string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "colima", "ssh", "--", "sudo", "sync", filePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nfsFsync sync %s: %w\n%s", filePath, err, out)
	}
	return nil
}

// nfsTruncate issues "truncate -s <size>" inside the colima VM, triggering an
// NFS SETATTR op which causes the server to discard the write buffer.
// filePath must be the full path inside the VM.
func nfsTruncate(t testing.TB, filePath string, size int64) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "colima", "ssh", "--",
		"sudo", "truncate", fmt.Sprintf("-s%d", size), filePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nfsTruncate -s%d %s: %w\n%s", size, filePath, err, out)
	}
	return nil
}

// s3GetObject fetches the full body of bucket/key and asserts no error.
func s3GetObject(ctx context.Context, t testing.TB, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return data
}

// makeDeterministicBytes returns n bytes derived deterministically from seed
// using a sha256 chain. Pure Go, no I/O.
func makeDeterministicBytes(seed string, n int) []byte {
	out := make([]byte, 0, n)
	h := sha256.Sum256([]byte(seed))
	for len(out) < n {
		out = append(out, h[:]...)
		h = sha256.Sum256(h[:])
	}
	return out[:n]
}

// sha256Hex returns the hex-encoded sha256 digest of data.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}
