package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

// uniqueDevice creates a per-case NBD volume (via volumeadmin) and returns the
// device/volume name.
func (tgt *nbdTarget) uniqueDevice(t *testing.T, caseName string, sizeBytes int64) string {
	t.Helper()
	id := tgt.caseSeq.Add(1)
	device := fmt.Sprintf("nbd-%s-%s-%d", tgt.name, sanitizeForBucket(caseName), id)

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
func ensureSingleNodeNBDVolume(t *testing.T, ctx context.Context, name string, size int64) {
	t.Helper()
	cli, err := volumeadmin.NewClient(filepath.Join(testServerDataDir, "admin.sock"))
	require.NoError(t, err)
	_, err = cli.CreateVolume(ctx, volumeadmin.CreateVolumeReq{Name: name, Size: size})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		require.NoError(t, err)
	}
}

func newSingleNodeNBDTarget(t *testing.T) *nbdTarget {
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

func newSharedClusterNBDTarget(t *testing.T) *nbdTarget {
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
