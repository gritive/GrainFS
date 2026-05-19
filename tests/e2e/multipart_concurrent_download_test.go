// Concurrent multipart download repro for the warp `multipart` failure:
//
//	warp: <ERROR> download error: forward: no reachable peer
//	  (last dial error: transport response from <addr> status 1:
//	  context deadline exceeded)
//
// `status 1` is `StatusOverloaded` — the receiver's inbound bulk-traffic
// limiter hit its 25 ms acquire timeout and short-circuited the stream with
// a StatusOverloaded reply, which the ForwardSender currently treats as a
// plain dial failure (no retry, fail over to the next peer — but the next
// peer is the same overloaded leader).
//
// This test issues many concurrent multipart-part GETs across all cluster
// nodes so request fan-in on the data-group leader exceeds the bulk
// limiter's 25 ms budget. With the current code the call surface bubbles
// up `ErrNoReachablePeer`-shaped errors. After the fix it must succeed.
//
// Single-node always succeeds (no forwarding), so it doubles as a parity
// guard.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipartConcurrentDownloadE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runMultipartConcurrentDownloadCases(t, newSingleNodeS3Target())
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runMultipartConcurrentDownloadCases(t, newSharedClusterS3Target(t))
	})
}

func TestMultipartUploadPartRecreatesClusterPartDirE2E(t *testing.T) {
	tgt := newSharedClusterS3Target(t)
	client := tgt.pickNode(0)

	probe := tgt.name + "-mp-put-dir-probe"
	tgt.createBkt(t, probe)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	waitForMultipartListingCreate(t, ctx, client, probe, multipartListingKey, 120*time.Second)

	bucket := tgt.uniqueBucket(t, "mp-put-dir")
	key := "warp-mp-put.bin"

	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)
	require.NotNil(t, initOut.UploadId)

	removed := removeClusterMultipartUploadDirs(t, tgt, *initOut.UploadId)
	require.NotEmpty(t, removed)

	body := bytes.Repeat([]byte{'A'}, 1024)
	part, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(body),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: initOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}},
		},
	})
	require.NoError(t, err)

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	got, err := io.ReadAll(out.Body)
	_ = out.Body.Close()
	require.NoError(t, err)
	require.Equal(t, body, got)
}

// Isolation: 10 MiB single PUT (no multipart) on cluster, full body GET +
// part-range GET. Purpose: separate "cluster forwarded GET body corruption"
// from "multipart-specific corruption". If this fails, the bug is in the
// cluster GET/forward path (EC read or stream forwarding), not multipart.
// If this passes, the bug is multipart-only.
func TestClusterSimpleLargeObjectFullGetE2E(t *testing.T) {
	tgt := newSharedClusterS3Target(t)

	ctx := context.Background()
	bucket := tgt.uniqueBucket(t, "simple-large")
	key := "warp-simple.bin"

	// Match warp multipart size so the EC + forwarding layout is identical.
	body := append(bytes.Repeat([]byte{'A'}, 5<<20), bytes.Repeat([]byte{'B'}, 5<<20)...)

	client := tgt.pickNode(0)
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)

	for w := 0; w < tgt.nodes; w++ {
		cli := tgt.pickNode(w)
		out, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		got, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		require.NoError(t, err)
		if !bytes.Equal(got, body) {
			first := -1
			for i := 0; i < len(got) && i < len(body); i++ {
				if got[i] != body[i] {
					first = i
					break
				}
			}
			t.Logf("node %d corrupted: len=%d first-bad=%d", w, len(got), first)
			for blk := 0; blk*(1<<20) < len(got); blk++ {
				start := blk * (1 << 20)
				end := start + (1 << 20)
				if end > len(got) {
					end = len(got)
				}
				blkData := got[start:end]
				aCount := bytes.Count(blkData, []byte{'A'})
				bCount := bytes.Count(blkData, []byte{'B'})
				label := "MIXED"
				if aCount == len(blkData) {
					label = "A"
				} else if bCount == len(blkData) {
					label = "B"
				}
				t.Logf("got block %d (offset %d): %s (A=%d B=%d)", blk, start, label, aCount, bCount)
			}
			require.Equalf(t, body, got, "node %d (simple PUT) full GET corrupted at byte %d", w, first)
		}
	}
}

func runMultipartConcurrentDownloadCases(t *testing.T, tgt s3Target) {
	client := tgt.pickNode(0)

	if tgt.isCluster {
		probe := tgt.name + "-mp-cd-probe"
		tgt.createBkt(t, probe)
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		waitForMultipartListingCreate(t, ctx, client, probe, multipartListingKey, 120*time.Second)
	}

	ctx := context.Background()
	bucket := tgt.uniqueBucket(t, "mp-cd")
	key := "warp-mp-burst.bin"

	part1 := bytes.Repeat([]byte{'A'}, multipartPartSize)
	part2 := bytes.Repeat([]byte{'B'}, multipartPartSize)

	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)
	uploadID := initOut.UploadId

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1),
	})
	require.NoError(t, err)

	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	require.NoError(t, err)

	// 16 concurrent GETs distributed round-robin across nodes. Mirrors the
	// warp `multipart --concurrent 16` pattern that triggers the bulk-class
	// inbound-traffic overload on the data-group leader.
	const (
		workers           = 16
		iterationsPerGoro = 4
	)

	parts := [][]byte{part1, part2}
	partsLabel := [2]byte{'A', 'B'}

	var (
		wg      sync.WaitGroup
		errMu   sync.Mutex
		errs    []error
		okCount int
		okMu    sync.Mutex
	)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			cli := tgt.pickNode(w)
			for i := 0; i < iterationsPerGoro; i++ {
				partN := int32((i % 2) + 1)
				out, err := cli.GetObject(ctx, &s3.GetObjectInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(key),
					PartNumber: aws.Int32(partN),
				})
				if err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
					continue
				}
				body, rerr := io.ReadAll(out.Body)
				_ = out.Body.Close()
				if rerr != nil {
					errMu.Lock()
					errs = append(errs, rerr)
					errMu.Unlock()
					continue
				}
				if !bytes.Equal(body, parts[partN-1]) {
					expect := partsLabel[partN-1]
					firstBad := -1
					var firstBadByte byte
					for j := 0; j < len(body); j++ {
						if body[j] != expect {
							firstBad = j
							firstBadByte = body[j]
							break
						}
					}
					var counts [256]int
					for _, c := range body {
						counts[c]++
					}
					errMu.Lock()
					errs = append(errs, fmt.Errorf(
						"w%d iter%d: requested part %d (expect all-%c), got len=%d first-bad-at=%d bad-byte=%q counts: A=%d B=%d 0=%d other=%d",
						w, i, partN, expect, len(body), firstBad, []byte{firstBadByte},
						counts['A'], counts['B'], counts[0], len(body)-counts['A']-counts['B']-counts[0],
					))
					errMu.Unlock()
					continue
				}
				okMu.Lock()
				okCount++
				okMu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		for i, e := range errs {
			if i >= 5 {
				t.Logf("... +%d more errors", len(errs)-i)
				break
			}
			t.Logf("err[%d]: %v", i, e)
		}
	}
	assert.Empty(t, errs, "concurrent multipart part GETs must not error under load")
	assert.Equal(t, workers*iterationsPerGoro, okCount, "expected every worker iteration to succeed")
}

func removeClusterMultipartUploadDirs(t *testing.T, tgt s3Target, uploadID string) []string {
	t.Helper()
	require.NotNil(t, tgt.cluster)

	matches := findClusterMultipartUploadDirs(t, tgt, uploadID)
	var removed []string
	for _, match := range matches {
		require.NoError(t, os.RemoveAll(match))
		removed = append(removed, match)
	}
	return removed
}

func findClusterMultipartUploadDirs(t *testing.T, tgt s3Target, uploadID string) []string {
	t.Helper()
	require.NotNil(t, tgt.cluster)

	var matches []string
	for _, dataDir := range tgt.cluster.dataDirs {
		found, err := filepath.Glob(filepath.Join(dataDir, "groups", "*", "parts", uploadID))
		require.NoError(t, err)
		matches = append(matches, found...)
	}
	return matches
}
