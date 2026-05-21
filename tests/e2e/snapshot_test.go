package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// snapshotResponse mirrors internal/snapshot.Snapshot for JSON decoding.
type snapshotResponse struct {
	Seq         uint64 `json:"seq"`
	Timestamp   string `json:"timestamp"`
	ObjectCount int    `json:"object_count"`
	SizeBytes   int64  `json:"size_bytes"`
	Reason      string `json:"reason,omitempty"`
}

type snapshotListResponse struct {
	Snapshots []snapshotResponse `json:"snapshots"`
}

type restoreResponse struct {
	RestoredObjects int           `json:"restored_objects"`
	StaleBlobs      []interface{} `json:"stale_blobs"`
}

type errorResponse struct {
	Error string `json:"error"`
	Hint  string `json:"hint"`
}

func postJSON(url string, body interface{}) (*http.Response, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	client := &http.Client{Timeout: 10 * time.Second}
	return client.Post(url, "application/json", &buf) //nolint:noctx
}

func createSnapshotE2E(t testing.TB, serverURL, reason string) snapshotResponse {
	t.Helper()
	var lastErr error
	var lastStatus int
	var lastBody string

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var snap snapshotResponse
		resp, err := postJSON(serverURL+"/admin/snapshots", map[string]string{"reason": reason})
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastStatus = resp.StatusCode
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastBody = string(body)
		if resp.StatusCode != http.StatusOK {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := json.Unmarshal(body, &snap); err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return snap
	}

	ginkgo.Fail(fmt.Sprintf(
		"snapshot should become available after cluster data groups are ready: lastErr=%v status=%d body=%s",
		lastErr, lastStatus, lastBody))
	return snapshotResponse{}
}

// Snapshot specs exercise the /admin/snapshots HTTP surface (create / list
// / restore / 404) against both single-node and 4-node cluster fixtures.
// /admin/snapshots restore mutates global metadata state, so each spec starts
// a dedicated fixture instead of sharing one across cases.
var _ = ginkgo.Describe("Snapshots", func() {
	describeSnapshotContext("SingleNode", func() s3Target {
		return newDedicatedSingleNodeS3Target(ginkgo.GinkgoTB(), nil)
	})

	describeSnapshotContext("Cluster4Node", func() s3Target {
		return newClusterS3TargetWithExtraArgs(ginkgo.GinkgoTB(), 4, nil)
	})
})

func describeSnapshotContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var (
			tgt       s3Target
			serverURL string
			client    *s3.Client
		)

		ginkgo.BeforeEach(func() {
			tgt = factory()
			serverURL = tgt.endpoint(0)
			client = tgt.pickNode(0)
		})

		runSnapshotCases(func() s3Target { return tgt }, func() string { return serverURL }, func() *s3.Client { return client })
	})
}

func runSnapshotCases(getTgt func() s3Target, getServerURL func() string, getClient func() *s3.Client) {
	ginkgo.It("creates a snapshot and restores object state", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		serverURL := getServerURL()
		client := getClient()
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "create")

		objects := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
		for _, key := range objects {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("content-" + key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "put %s", key)
		}

		snap := createSnapshotE2E(t, serverURL, "e2e-test")
		gomega.Expect(snap.Seq).NotTo(gomega.BeZero(), "snapshot seq must be non-zero")
		gomega.Expect(snap.Timestamp).NotTo(gomega.BeEmpty())
		gomega.Expect(snap.ObjectCount).To(gomega.BeNumerically(">", 0), "snapshot must contain objects")

		extras := []string{"extra1.txt", "extra2.txt"}
		for _, key := range extras {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("extra-" + key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "put extra %s", key)
		}

		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut.Contents).To(gomega.HaveLen(7), "7 objects before restore")

		restoreURL := fmt.Sprintf("%s/admin/snapshots/%d/restore", serverURL, snap.Seq)
		restoreResp, err := postJSON(restoreURL, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(restoreResp.Body.Close)
		restoreBody, err := io.ReadAll(restoreResp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(restoreResp.StatusCode).To(gomega.Equal(http.StatusOK), "restore status: %s", restoreBody)

		var rr restoreResponse
		gomega.Expect(json.Unmarshal(restoreBody, &rr)).To(gomega.Succeed())
		gomega.Expect(rr.RestoredObjects).To(gomega.BeNumerically(">=", 5), "at least 5 objects restored")
		gomega.Expect(rr.StaleBlobs).To(gomega.BeEmpty(), "no stale blobs: blobs still exist")

		listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut2.Contents).To(gomega.HaveLen(5), "5 objects after restore (extras removed)")

		for _, key := range objects {
			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "get %s after restore", key)
			ginkgo.DeferCleanup(getResp.Body.Close)
			body, _ := io.ReadAll(getResp.Body)
			gomega.Expect(string(body)).To(gomega.Equal("content-" + key))
		}

		for _, key := range extras {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			gomega.Expect(err).To(gomega.HaveOccurred(), "extra object %s should be gone after restore", key)
		}
	})

	ginkgo.It("lists snapshots in sequence order", func() {
		t := ginkgo.GinkgoTB()
		serverURL := getServerURL()
		for i := 0; i < 2; i++ {
			createSnapshotE2E(t, serverURL, fmt.Sprintf("list-test-%d", i))
		}

		listResp, err := http.Get(serverURL + "/admin/snapshots") //nolint:noctx
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(listResp.Body.Close)
		gomega.Expect(listResp.StatusCode).To(gomega.Equal(http.StatusOK))

		var lr snapshotListResponse
		gomega.Expect(json.NewDecoder(listResp.Body).Decode(&lr)).To(gomega.Succeed())
		gomega.Expect(len(lr.Snapshots)).To(gomega.BeNumerically(">=", 2))

		for i := 1; i < len(lr.Snapshots); i++ {
			gomega.Expect(lr.Snapshots[i-1].Seq).To(gomega.BeNumerically("<", lr.Snapshots[i].Seq))
		}
	})

	ginkgo.It("returns not found for missing snapshot restores", func() {
		serverURL := getServerURL()
		restoreResp, err := postJSON(serverURL+"/admin/snapshots/999999/restore", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(restoreResp.Body.Close)
		gomega.Expect(restoreResp.StatusCode).To(gomega.Equal(http.StatusNotFound))

		var er errorResponse
		gomega.Expect(json.NewDecoder(restoreResp.Body).Decode(&er)).To(gomega.Succeed())
		gomega.Expect(er.Error).NotTo(gomega.BeEmpty())
		gomega.Expect(er.Hint).NotTo(gomega.BeEmpty())
	})
}
