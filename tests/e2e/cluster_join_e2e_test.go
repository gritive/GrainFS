package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster join", ginkgo.Label("bucket"), func() {
	ginkgo.Context("default bucket", func() {
		ginkgo.It("creates the default bucket only on the seed", func() {
			t := ginkgo.GinkgoTB()
			c := startE2ECluster(t, e2eClusterOptions{
				Nodes:      2,
				Mode:       ClusterModeDynamicJoin,
				ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
				AccessKey:  "join-ak",
				SecretKey:  "join-sk",
				LogPrefix:  "grainfs-dynamic-join",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			ginkgo.DeferCleanup(cancel)

			for i, endpoint := range c.httpURLs {
				client := c.S3Client(i)
				out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				var defaults int
				for _, bucket := range out.Buckets {
					if aws.ToString(bucket.Name) == "default" {
						defaults++
					}
				}
				gomega.Expect(defaults).To(gomega.Equal(1), "default bucket should exist once as shared cluster metadata at %s", endpoint)
			}
		})
	})

	ginkgo.Context("services", func() {
		ginkgo.It("makes all services available after dynamic join", func() {
			runClusterJoinAllServicesAvailable(ginkgo.GinkgoTB())
		})

		for _, nodes := range []int{2, 3} {
			nodes := nodes
			ginkgo.It(fmt.Sprintf("serves S3 and Iceberg across %d joined nodes", nodes), func() {
				runClusterJoinDynamicJoinServicesNodeCount(ginkgo.GinkgoTB(), nodes)
			})
		}

		ginkgo.It("forwards joined-node edge writes before data is locally ready", func() {
			runClusterJoinedNodeEdgeForwardsBeforeDataReady(ginkgo.GinkgoTB())
		})
	})
})

func runClusterJoinedNodeEdgeForwardsBeforeDataReady(t testing.TB) {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  "grainfs-dynamic-join",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	ginkgo.DeferCleanup(cancel)

	joinClient := c.S3Client(1)
	seedClient := c.S3Client(0)
	bucket := "dynamic-join-edge"
	key := "hello.txt"
	body := []byte("hello dynamic join")

	_, err := c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(tryPutObject(ctx, joinClient, bucket, key, body)).To(gomega.Succeed())

	gotSeed, err := getObjectBytes(ctx, seedClient, bucket, key)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(gotSeed).To(gomega.Equal(body))
	gotJoin, err := getObjectBytes(ctx, joinClient, bucket, key)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(gotJoin).To(gomega.Equal(body))

	_, err = joinClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func runClusterJoinAllServicesAvailable(t testing.TB) {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      2,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-DYNAMIC-JOIN-KEY",
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  "grainfs-dynamic-join",
	})
	waitForPortsParallel(t, c.nfs4Ports, 30*time.Second)
	waitForPortsParallel(t, c.nbdPorts, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ginkgo.DeferCleanup(cancel)
	client := newIcebergSigV4Client(t, c.accessKey, c.secretKey, "us-east-1")

	reqBody := []byte(`{"namespace":["join_ns"],"properties":{"owner":"e2e"}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURLs[1]+"/iceberg/v1/namespaces", bytes.NewReader(reqBody))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, c.httpURLs[0]+"/iceberg/v1/namespaces", nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	resp, err = client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	data, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	gomega.Expect(string(data)).To(gomega.ContainSubstring("join_ns"))
}

func runClusterJoinDynamicJoinServicesNodeCount(t testing.TB, nodes int) {
	t.Helper()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      nodes,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: fmt.Sprintf("E2E-DYNAMIC-JOIN-MATRIX-%d", nodes),
		AccessKey:  "join-ak",
		SecretKey:  "join-sk",
		LogPrefix:  fmt.Sprintf("grainfs-dynamic-join-%d", nodes),
	})

	waitForPortsParallel(t, c.httpPorts, 30*time.Second)
	waitForPortsParallel(t, c.nfs4Ports, 30*time.Second)
	waitForPortsParallel(t, c.nbdPorts, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	ginkgo.DeferCleanup(cancel)

	bucket := fmt.Sprintf("dynamic-join-services-%d", nodes)
	_, err := c.EnsureBucketWritable(ctx, bucket, 60*time.Second)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for i := range c.httpURLs {
		key := fmt.Sprintf("node-%d.txt", i)
		body := []byte(fmt.Sprintf("node %d through dynamic join", i))
		gomega.Expect(tryPutObject(ctx, c.S3Client(i), bucket, key, body)).To(gomega.Succeed())
		got, err := getObjectBytes(ctx, c.S3Client(i), bucket, key)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got).To(gomega.Equal(body))
	}

	client := newIcebergSigV4Client(t, c.accessKey, c.secretKey, "us-east-1")
	namespace := fmt.Sprintf("join_matrix_%d", nodes)
	reqBody := []byte(fmt.Sprintf(`{"namespace":["%s"],"properties":{"owner":"e2e"}}`, namespace))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURLs[nodes-1]+"/iceberg/v1/namespaces", bytes.NewReader(reqBody))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

	for i, endpoint := range c.httpURLs {
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/iceberg/v1/namespaces", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err = client.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK), "iceberg list namespaces on node %d", i)
		gomega.Expect(string(data)).To(gomega.ContainSubstring(namespace), "iceberg namespace should be visible on node %d", i)
	}
}
