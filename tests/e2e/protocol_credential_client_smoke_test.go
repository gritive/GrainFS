package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/credentialadmin"
	"github.com/gritive/GrainFS/internal/iamadmin"
)

var _ = ginkgo.Describe("Protocol credential client smoke", ginkgo.Label("protocred"), func() {
	describeProtocolCredentialS3ClientSmokeContext("S3 SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})
	describeProtocolCredentialS3ClientSmokeContext("S3 Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeProtocolCredentialS3ClientSmokeContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt s3Target

		ginkgo.BeforeAll(func() {
			tgt = factory()
		})

		ginkgo.It("round-trips an object through MinIO mc using a bucket-scoped protocol credential", func(ctx context.Context) {
			testS3ProtocolCredentialMinIOMC(ginkgo.GinkgoTB(), tgt)
		}, ginkgo.NodeTimeout(60*time.Second))
	})
}

func testS3ProtocolCredentialMinIOMC(t testing.TB, tgt s3Target) {
	t.Helper()
	_, err := exec.LookPath("mc")
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "mc is required for protocol credential S3 client smoke")

	bucket := tgt.uniqueBucket(t, "pc-mc")
	cred := createProtocolCredential(t, tgt.adminSockPath(), tgt.saID, "s3", "bucket/"+bucket, "rw")
	endpoint := protocolCredentialS3Endpoint(t, tgt)
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "mc-config")
	gomega.Expect(os.MkdirAll(configDir, 0o700)).To(gomega.Succeed())
	srcPath := filepath.Join(tmpDir, "src.txt")
	gomega.Expect(os.WriteFile(srcPath, []byte("protocol credential mc smoke"), 0o600)).To(gomega.Succeed())

	runClientCommand(nil, "mc", "--config-dir", configDir, "alias", "set", "grainfs-pc", endpoint, cred.ID, cred.Secret, "--api", "S3v4", "--path", "on")
	runClientCommand(nil, "mc", "--config-dir", configDir, "cp", srcPath, "grainfs-pc/"+bucket+"/pc-smoke.txt")
	out := runClientCommand(nil, "mc", "--config-dir", configDir, "cat", "grainfs-pc/"+bucket+"/pc-smoke.txt")
	gomega.Expect(string(out)).To(gomega.Equal("protocol credential mc smoke"))
	out = runClientCommand(nil, "mc", "--config-dir", configDir, "ls", "grainfs-pc/"+bucket)
	gomega.Expect(string(out)).To(gomega.ContainSubstring("pc-smoke.txt"))
	runClientCommand(nil, "mc", "--config-dir", configDir, "rm", "grainfs-pc/"+bucket+"/pc-smoke.txt")
	requireObjectDeleted(tgt, bucket, "pc-smoke.txt")
}

func protocolCredentialS3Endpoint(t testing.TB, tgt s3Target) string {
	t.Helper()
	if tgt.isCluster && tgt.cluster != nil {
		return tgt.endpoint(currentE2EClusterLeaderIdx(t, tgt.cluster))
	}
	return tgt.endpoint(0)
}

func createProtocolCredential(t testing.TB, adminSock, saID, protocol, resource, mode string) credentialadmin.Credential {
	t.Helper()
	attachProtocolCredentialPolicy(t, adminSock, saID, protocol, resource)
	tp, err := adminapi.NewTransport(adminSock)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create admin transport for protocol credential")
	cli := &credentialadmin.Client{Transport: tp}
	cred, err := cli.Create(context.Background(), credentialadmin.CreateReq{
		SAID:     saID,
		Protocol: protocol,
		Resource: resource,
		Mode:     mode,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create %s protocol credential for %s", protocol, resource)
	gomega.Expect(cred.ID).NotTo(gomega.BeEmpty(), "protocol credential id")
	gomega.Expect(cred.Secret).NotTo(gomega.BeEmpty(), "one-time protocol credential secret")
	ginkgo.DeferCleanup(func() {
		_, _ = cli.Revoke(context.Background(), cred.ID)
	})
	return cred
}

func attachProtocolCredentialPolicy(t testing.TB, adminSock, saID, protocol, resource string) {
	t.Helper()
	cli := iamadmin.NewClientForURL(adminSock)
	ctx := context.Background()
	policyName := fmt.Sprintf("test-pc-%d", time.Now().UnixNano())
	doc := buildPolicyDocJSON(
		[]string{"grainfs:CredentialCreate", "grainfs:CredentialRevoke"},
		[]string{"protocol-credential/" + protocol + "/" + resource},
	)
	gomega.Expect(cli.PolicyPut(ctx, policyName, doc)).To(gomega.Succeed(), "PolicyPut %s", policyName)
	gomega.Expect(cli.PolicyAttachToSA(ctx, policyName, saID)).To(gomega.Succeed(), "PolicyAttachToSA %s -> %s", policyName, saID)
	ginkgo.DeferCleanup(func() {
		_ = cli.PolicyDetachFromSA(ctx, policyName, saID)
		_ = cli.PolicyDelete(ctx, policyName)
	})
}
