package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/credentialadmin"
	"github.com/gritive/GrainFS/internal/iamadmin"
	"github.com/gritive/GrainFS/internal/volumeadmin"
)

// nbdTarget abstracts an NBD-capable grainfs fixture. Cluster variant reuses
// the shared 4-node e2eCluster (S3 generic fixture, NBD enabled).
type nbdTarget struct {
	name       string
	nbdAddr    func(i int) string
	s3Endpoint func(i int) string
	dataDir    func(i int) string
	saID       string
	nodeCount  int
	leaderIdx  int
	caseSeq    atomic.Int64
	isCluster  bool
	cluster    *e2eCluster // non-nil for cluster fixtures
}

// uniqueDevice ensures the boot-time NBD volume exists in metadata and returns
// an authorized export name for it. The NBD server exports a single fixed
// volume, so per-case names remain unavailable; the atomic seq is preserved
// for future per-case offset isolation if additional matrix cases need it.
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
		ensureSingleNodeNBDVolume(t, ctx, tgt.dataDir(0), device, sizeBytes)
	}
	return ensureE2ENBDCredential(t, ctx, filepath.Join(tgt.dataDir(tgt.leaderIdx), "admin.sock"), tgt.saID, device)
}

func ensureSingleNodeNBDVolume(t testing.TB, ctx context.Context, dataDir, name string, size int64) {
	t.Helper()
	cli, err := volumeadmin.NewClient(filepath.Join(dataDir, "admin.sock"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = cli.CreateVolume(ctx, volumeadmin.CreateVolumeReq{Name: name, Size: size})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func ensureE2ENBDCredential(t testing.TB, ctx context.Context, adminSock, saID, volumeName string) string {
	t.Helper()
	attachE2ENBDCredentialPolicy(t, ctx, adminSock, saID, volumeName)
	cli, err := credentialadmin.NewClient(adminSock)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cred, err := cli.Create(ctx, credentialadmin.CreateReq{
		SAID:     saID,
		Protocol: "nbd",
		Resource: "volume/" + volumeName,
		Mode:     "rw",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	exportName := cred.ConnectionHint["export_name"]
	gomega.Expect(exportName).NotTo(gomega.BeEmpty())
	return exportName
}

func attachE2ENBDCredentialPolicy(t testing.TB, ctx context.Context, adminSock, saID, volumeName string) {
	t.Helper()
	sum := sha256.Sum256([]byte(t.Name() + "|" + saID + "|" + volumeName))
	polName := "test-nbd-cred-" + hex.EncodeToString(sum[:8])
	doc := buildPolicyDocJSON(
		[]string{"grainfs:CredentialCreate"},
		[]string{"protocol-credential/nbd/volume/" + volumeName},
	)
	cli := iamadmin.NewClientForURL(adminSock)
	gomega.Expect(cli.PolicyPut(ctx, polName, doc)).To(gomega.Succeed(), "PolicyPut %s", polName)
	gomega.Expect(cli.PolicyAttachToSA(ctx, polName, saID)).To(gomega.Succeed(), "PolicyAttachToSA %s->%s", polName, saID)
}

func newSingleNodeNBDTarget(t testing.TB) *nbdTarget {
	t.Helper()
	tgt := newDedicatedSingleNodeS3Target(t, nil)
	return &nbdTarget{
		name:       "single",
		nbdAddr:    func(i int) string { return fmt.Sprintf("127.0.0.1:%d", tgt.nbdPort) },
		s3Endpoint: func(i int) string { return tgt.endpoint(i) },
		dataDir:    func(i int) string { return tgt.dataDir },
		saID:       tgt.saID,
		nodeCount:  1,
		leaderIdx:  0,
		isCluster:  false,
	}
}

func newSharedClusterNBDTarget(t testing.TB) *nbdTarget {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      4,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-NBD-KEY",
		LogPrefix:  "grainfs-nbd",
		DisableNFS: true,
	})
	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 30*time.Second)
	}
	n := len(c.httpURLs)
	return &nbdTarget{
		name:       "cluster4",
		nbdAddr:    func(i int) string { return fmt.Sprintf("127.0.0.1:%d", c.nbdPorts[i%n]) },
		s3Endpoint: func(i int) string { return c.httpURLs[i%n] },
		dataDir:    func(i int) string { return c.dataDirs[i%n] },
		saID:       c.saID,
		nodeCount:  n,
		leaderIdx:  c.leaderIdx,
		isCluster:  true,
		cluster:    c,
	}
}
