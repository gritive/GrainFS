package e2e

import (
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

type pitrResponse struct {
	RestoredObjects    int              `json:"restored_objects"`
	WALEntriesReplayed int              `json:"wal_entries_replayed"`
	StaleBlobs         []map[string]any `json:"stale_blobs"`
}

func createPITRSnapshot(t testing.TB, serverURL, reason string) {
	t.Helper()
	var lastErr error
	var lastStatus int
	var lastBody string

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
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
		if resp.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	ginkgo.Fail(fmt.Sprintf(
		"snapshot should become available after cluster data groups are ready: lastErr=%v status=%d body=%s",
		lastErr, lastStatus, lastBody))
}

var _ = ginkgo.Describe("PITR", func() {
	describePITRContext("SingleNode", func(t testing.TB) s3Target {
		return newDedicatedSingleNodeS3Target(t, nil)
	})
	describePITRContext("Cluster4Node", func(t testing.TB) s3Target {
		return newClusterS3TargetWithExtraArgs(t, 4, nil)
	})
})

func describePITRContext(name string, factory func(testing.TB) s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		runPITRCases(func() s3Target { return tgt })
	})
}

func runPITRCases(getTgt func() s3Target) {
	ctx := context.Background()

	// Invalid-input cases first so they don't accumulate fixture state.
	ginkgo.It("rejects an invalid target time", func() {
		tgt := getTgt()
		serverURL := tgt.endpoint(0)

		resp, err := postJSON(serverURL+"/admin/pitr", map[string]string{"to": "not-a-time"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusBadRequest))
	})

	ginkgo.It("rejects a target before any snapshot", func() {
		tgt := getTgt()
		serverURL := tgt.endpoint(0)

		// epoch+1s is always before any snapshot this fixture will produce,
		// so the "no snapshot before target" check is stable regardless of
		// what other sub-tests have already run on this fixture.
		veryOldTime := time.Unix(1, 0).UTC().Format(time.RFC3339Nano)
		resp, err := postJSON(serverURL+"/admin/pitr", map[string]string{"to": veryOldTime})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		gomega.Expect(resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest).To(gomega.BeTrue(),
			"expected 404 or 400, got %d", resp.StatusCode)
	})

	ginkgo.It("replays WAL entries to add objects before the target time", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		serverURL := tgt.endpoint(0)
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "walreplay")
		createPITRSnapshot(t, serverURL, "pitr-wal-base")

		included := []string{"inc1.txt", "inc2.txt", "inc3.txt"}
		for _, key := range included {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("included-" + key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		pivotTime := time.Now().UTC()
		time.Sleep(150 * time.Millisecond)

		excluded := []string{"exc1.txt", "exc2.txt"}
		for _, key := range excluded {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("excluded-" + key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut.Contents).To(gomega.HaveLen(5), "5 objects before PITR")

		pitrResp, err := postJSON(serverURL+"/admin/pitr", map[string]string{
			"to": pivotTime.Format(time.RFC3339Nano),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(pitrResp.Body.Close)
		gomega.Expect(pitrResp.StatusCode).To(gomega.Equal(http.StatusOK), "PITR should succeed")

		var pr pitrResponse
		gomega.Expect(json.NewDecoder(pitrResp.Body).Decode(&pr)).To(gomega.Succeed())
		gomega.Expect(pr.WALEntriesReplayed).To(gomega.BeNumerically(">=", 3), "at least 3 WAL entries replayed")
		for _, b := range pr.StaleBlobs {
			gomega.Expect(b["bucket"]).NotTo(gomega.Equal(bucket), "unexpected stale blob in test bucket: %v", b)
		}

		listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut2.Contents).To(gomega.HaveLen(3), "only 3 included objects after PITR")

		for _, key := range included {
			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "get %s after PITR", key)
			ginkgo.DeferCleanup(getResp.Body.Close)
			body, _ := io.ReadAll(getResp.Body)
			gomega.Expect(string(body)).To(gomega.Equal("included-" + key))
		}

		for _, key := range excluded {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			gomega.Expect(err).To(gomega.HaveOccurred(), "excluded %s should not exist after PITR", key)
		}
	})

	ginkgo.It("excludes objects added after the target time", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		serverURL := tgt.endpoint(0)
		client := tgt.pickNode(0)
		bucket := tgt.uniqueBucket(t, "excludes")

		originals := []string{"orig1.txt", "orig2.txt"}
		for _, key := range originals {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("original-" + key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		createPITRSnapshot(t, serverURL, "pitr-excl-base")

		pivotTime := time.Now().UTC()
		time.Sleep(150 * time.Millisecond)

		extras := []string{"extra1.txt", "extra2.txt", "extra3.txt"}
		for _, key := range extras {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("extra-" + key),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut.Contents).To(gomega.HaveLen(5))

		pitrResp, err := postJSON(serverURL+"/admin/pitr", map[string]string{
			"to": pivotTime.Format(time.RFC3339Nano),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(pitrResp.Body.Close)
		gomega.Expect(pitrResp.StatusCode).To(gomega.Equal(http.StatusOK))

		listOut2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(listOut2.Contents).To(gomega.HaveLen(2), "only 2 original objects after PITR")

		for _, key := range extras {
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			gomega.Expect(err).To(gomega.HaveOccurred(), "extra %s should not exist after PITR", key)
		}
	})
}
