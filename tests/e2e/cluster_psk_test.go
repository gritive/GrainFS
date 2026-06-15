package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster PSK", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("refuses a non-genesis node with no cluster key or invite bundle", func() {
			t := ginkgo.GinkgoTB()

			dir := t.TempDir()
			port := freePort()
			raft := freePort()

			// Plant prior raft state so the node is NOT genesis-eligible. With no
			// staged keys.d/current.key and no GRAINFS_INVITE_BUNDLE, genesis
			// self-seed is refused (priorState), so the cluster-transport-key gate
			// must trip. This replaces the retired .join-pending join-mode trigger.
			// The wrong-PSK rejection path (a node holding a different cluster
			// secret cannot join) is now covered by the invite-join cross-cluster
			// and channel-binding rejection tests in cluster_invite_join_test.go.
			gomega.Expect(os.MkdirAll(filepath.Join(dir, "raft"), 0o700)).To(gomega.Succeed())
			gomega.Expect(os.WriteFile(filepath.Join(dir, "raft", "prior-state"), []byte("x"), 0o600)).To(gomega.Succeed())

			cmd := exec.Command(getBinary(), "serve",
				"--data", dir,
				"--port", fmt.Sprintf("%d", port),
				"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raft),
				"--node-id", "n-no-key",
				"--nfs4-port", "0",
			)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			out, err := cmd.CombinedOutput()
			gomega.Expect(err).To(gomega.HaveOccurred(), "process must exit non-zero without a cluster transport key")
			if !strings.Contains(string(out), "cluster transport key missing: stage keys.d/current.key, set GRAINFS_INVITE_BUNDLE, or start a fresh genesis node") {
				t.Fatalf("expected the cluster-transport-key-missing gate message in output, got:\n%s", string(out))
			}
		})
	})
})
