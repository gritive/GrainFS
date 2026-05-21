package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/volumeadmin"
)

// nbdTarget abstracts an NBD-capable grainfs fixture. Cluster variant reuses
// the shared 4-node e2eCluster (S3 generic fixture, NBD enabled).
type nbdTarget struct {
	name       string
	nbdAddr    func(i int) string
	s3Endpoint func(i int) string
	dataDir    func(i int) string
	nodeCount  int
	leaderIdx  int
	caseSeq    atomic.Int64
	isCluster  bool
	cluster    *e2eCluster // non-nil for cluster fixtures
}

// uniqueDevice ensures the boot-time NBD volume exists in metadata and returns
// its name. The NBD server exports a single fixed volume name (see
// internal/nbd/handshake.go: exportNameMatches), so we cannot create per-case
// unique exports. The atomic seq is preserved for future per-case offset
// isolation if additional matrix cases need it.
func (tgt *nbdTarget) uniqueDevice(t testing.TB, caseName string, sizeBytes int64) string {
	t.Helper()
	_ = tgt.caseSeq.Add(1) // reserved for future per-case offset isolation
	_ = caseName
	const device = "default" // NBD server is bound to a single export

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if tgt.cluster != nil {
		ensureE2ENBDVolume(t, ctx, tgt.cluster, device, sizeBytes)
	} else {
		ensureSingleNodeNBDVolume(t, ctx, device, sizeBytes)
	}
	return device
}

// ensureSingleNodeNBDVolume mirrors ensureE2ENBDVolume but uses the single-node
// fixture's admin socket (testServerDataDir + /admin.sock).
func ensureSingleNodeNBDVolume(t testing.TB, ctx context.Context, name string, size int64) {
	t.Helper()
	cli, err := volumeadmin.NewClient(filepath.Join(testServerDataDir, "admin.sock"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = cli.CreateVolume(ctx, volumeadmin.CreateVolumeReq{Name: name, Size: size})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func newSingleNodeNBDTarget(t testing.TB) *nbdTarget {
	t.Helper()
	return &nbdTarget{
		name:       "single",
		nbdAddr:    func(i int) string { return fmt.Sprintf("127.0.0.1:%d", testServerNBDPort) },
		s3Endpoint: func(i int) string { return testServerURL },
		dataDir:    func(i int) string { return testServerDataDir },
		nodeCount:  1,
		leaderIdx:  0,
		isCluster:  false,
	}
}

func newSharedClusterNBDTarget(t testing.TB) *nbdTarget {
	t.Helper()
	c := getOrInitSharedCluster(t) // 4-node *e2eCluster, NBD now enabled
	n := len(c.httpURLs)
	return &nbdTarget{
		name:       "cluster4",
		nbdAddr:    func(i int) string { return fmt.Sprintf("127.0.0.1:%d", c.nbdPorts[i%n]) },
		s3Endpoint: func(i int) string { return c.httpURLs[i%n] },
		dataDir:    func(i int) string { return c.dataDirs[i%n] },
		nodeCount:  n,
		leaderIdx:  c.leaderIdx,
		isCluster:  true,
		cluster:    c,
	}
}
