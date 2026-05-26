package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Degraded mode writes", func() {
	ginkgo.Context("Cluster5Node", func() {
		// Starts a 5-node cluster with EC 3+2, kills 3 nodes, and verifies
		// that PUT requests return 503 once the degraded monitor detects the
		// shortage.
		ginkgo.It("blocks writes after live nodes fall below MinECNodes", func() {
			t := ginkgo.GinkgoTB()

			binary := getBinary()
			if _, err := os.Stat(binary); err != nil {
			}

			const (
				clusterKey = "E2E-DEGRADED-KEY"
				bucketName = "degraded-test"
				numNodes   = 5
			)
			var accessKey, secretKey, saID string

			httpPorts := make([]int, numNodes)
			raftPorts := make([]int, numNodes)
			ports := uniqueFreePorts(numNodes * 2)
			for i := range numNodes {
				httpPorts[i] = ports[i]
				raftPorts[i] = ports[numNodes+i]
			}

			raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
			httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }

			dataDirs := make([]string, numNodes)
			for i := range dataDirs {
				d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-degraded-%d-*", i))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				dataDirs[i] = d
				ginkgo.DeferCleanup(removeE2EDir, d)
			}
			encKeyFile := makeSharedEncryptionKeyFile(t)

			startNode := func(i int) *exec.Cmd {
				cmd := exec.Command(binary, "serve",
					"--data", dataDirs[i],
					"--port", fmt.Sprintf("%d", httpPorts[i]),
					"--node-id", raftAddr(i),
					"--raft-addr", raftAddr(i),
					"--cluster-key", clusterKey,
					"--encryption-key-file", encKeyFile,
					"--nfs4-port", "0",
					"--nbd-port", "0",
					"--scrub-interval", "0",
					"--lifecycle-interval", "0",
					"--degraded-check-interval", "1s",
				)
				if testing.Verbose() {
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
				}
				gomega.Expect(cmd.Start()).To(gomega.Succeed(), "start node %d", i)
				return cmd
			}

			procs := make([]*exec.Cmd, numNodes)
			killAll := func() {
				for _, p := range procs {
					if p != nil && p.Process != nil {
						_ = p.Process.Kill()
						_, _ = p.Process.Wait()
					}
				}
			}
			ginkgo.DeferCleanup(killAll)

			// Start seed node first, then let followers join via .join-pending.
			procs[0] = startNode(0)
			waitForPortsParallel(t, httpPorts[:1], 60*time.Second)
			time.Sleep(2 * time.Second)

			bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, dataDirs[:1], 60*time.Second)
			accessKey, secretKey, saID = bootstrap.AccessKey, bootstrap.SecretKey, bootstrap.SAID

			for i := 1; i < numNodes; i++ {
				gomega.Expect(writeNodeJoinPending(dataDirs[i], dataDirs[0], raftAddr(0))).To(gomega.Succeed())
				procs[i] = startNode(i)
				time.Sleep(150 * time.Millisecond)
			}
			waitForPortsParallel(t, httpPorts, 60*time.Second)
			time.Sleep(4 * time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
			ginkgo.DeferCleanup(cancel)

			// Find a leader and create a bucket to confirm the cluster is healthy.
			endpoints := make([]string, numNodes)
			for i := range endpoints {
				endpoints[i] = httpURL(i)
			}
			leaderIdx, err := waitForAdminBucketWritable(ctx, dataDirs, endpoints, accessKey, secretKey, saID, bucketName, 180*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "no leader found or PutObject never succeeded")
			client := ecS3Client(httpURL(leaderIdx), accessKey, secretKey)
			t.Logf("degraded test: leader node %d at %s", leaderIdx, httpURL(leaderIdx))

			// Verify normal PUT works before killing nodes.
			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("before-kill"),
				Body:   bytes.NewReader([]byte("healthy")),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "PutObject should succeed with all nodes up")

			// Kill 3 nodes — leave only 2 alive (< MinECNodes=3 → degraded).
			// Pick 3 non-leader nodes to avoid forcing a re-election that could confuse
			// which nodes are "up" for the subsequent client requests.
			killed := 0
			for i := 0; i < numNodes && killed < 3; i++ {
				if i == leaderIdx {
					continue
				}
				t.Logf("degraded test: killing node %d at %s", i, httpURL(i))
				_ = procs[i].Process.Kill()
				_, _ = procs[i].Process.Wait()
				procs[i] = nil
				killed++
			}
			t.Logf("degraded test: %d nodes killed, 2 remaining", killed)

			// The monitor fires immediately on start — but since all nodes started
			// healthy, the immediate fire found live≥MinECNodes (not degraded). This
			// test uses a 1 s monitor interval so the post-kill transition is observed
			// quickly and does not depend on the production 30 s tick boundary.
			// Poll the surviving nodes until one returns 503 for a PUT.
			gomega.Eventually(func() bool {
				for i := 0; i < numNodes; i++ {
					if procs[i] == nil {
						continue
					}
					c := ecS3Client(httpURL(i), accessKey, secretKey)
					_, putErr := c.PutObject(ctx, &s3.PutObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String("after-kill"),
						Body:   bytes.NewReader([]byte("should-fail")),
					})
					if putErr != nil {
						errStr := putErr.Error()
						if strings.Contains(errStr, "503") || strings.Contains(errStr, "ServiceUnavailable") {
							t.Logf("degraded test: node %d correctly returned 503/ServiceUnavailable", i)
							return true
						}
					}
				}
				return false
			}, 120*time.Second, 500*time.Millisecond).Should(gomega.BeTrue(), "expected writes to be blocked (503) after degraded")
		})
	})
})
