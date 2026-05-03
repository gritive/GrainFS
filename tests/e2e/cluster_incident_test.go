package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/require"
)

type incidentState struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Cause string `json:"cause"`
	Proof struct {
		Status    string `json:"status"`
		ReceiptID string `json:"receipt_id"`
	} `json:"proof"`
	Scope struct {
		Bucket  string `json:"bucket"`
		Key     string `json:"key"`
		ShardID int    `json:"shard_id"`
	} `json:"scope"`
}

func fetchIncidents(t *testing.T, endpoint string) []incidentState {
	t.Helper()
	resp, err := http.Get(endpoint + "/api/incidents?limit=50")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out []incidentState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func TestE2E_ClusterIncident_MissingShardFixedWithReceipt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s - run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-CLUSTER-INCIDENT-KEY"
		accessKey  = "incident-ak"
		secretKey  = "incident-sk"
		bucketName = "incident-bucket"
		keyName    = "incident-obj"
		numNodes   = 3
		ecData     = 2
		ecParity   = 1
	)

	httpPorts, raftPorts, nfs4Ports, nbdPorts := make([]int, numNodes), make([]int, numNodes), make([]int, numNodes), make([]int, numNodes)
	ports := uniqueFreePorts(numNodes * 4)
	for i := range numNodes {
		httpPorts[i] = ports[i]
		raftPorts[i] = ports[numNodes+i]
		nfs4Ports[i] = ports[2*numNodes+i]
		nbdPorts[i] = ports[3*numNodes+i]
	}
	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j != i {
				out = append(out, raftAddr(j))
			}
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-incident-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	procs := make([]*exec.Cmd, numNodes)
	for i := range numNodes {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", raftAddr(i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			fmt.Sprintf("--ec-data=%d", ecData),
			fmt.Sprintf("--ec-parity=%d", ecParity),
			"--seed-groups", "1",
			"--nfs4-port", fmt.Sprintf("%d", nfs4Ports[i]),
			"--nbd-port", fmt.Sprintf("%d", nbdPorts[i]),
			"--snapshot-interval", "0",
			"--scrub-interval", "2s",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		if testing.Verbose() {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
		}
		require.NoError(t, cmd.Start(), "start node %d", i)
		procs[i] = cmd
	}
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})
	for i := range procs {
		waitForPort(t, httpPorts[i], 60*time.Second)
	}
	time.Sleep(4 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	endpoints := make([]string, numNodes)
	for i := range endpoints {
		endpoints[i] = httpURL(i)
	}
	leaderIdx, err := waitForWritableEndpoint(ctx, endpoints, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
		return tryCreateBucket(attemptCtx, ecS3Client(endpoint, accessKey, secretKey), bucketName)
	})
	require.NoError(t, err)
	client := ecS3Client(endpoints[leaderIdx], accessKey, secretKey)

	payload := make([]byte, 256*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucketName), Key: aws.String(keyName), Body: bytes.NewReader(payload)})
	require.NoError(t, err)

	var victimNode int
	var victimShard string
	for i := range numNodes {
		root := filepath.Join(dataDirs[i], "shards", bucketName, keyName)
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, _ error) error {
			if d != nil && !d.IsDir() && filepath.Base(p) == "shard_0" {
				victimNode, victimShard = i, p
				return filepath.SkipAll
			}
			return nil
		})
		if victimShard != "" {
			break
		}
	}
	require.NotEmpty(t, victimShard)
	require.NoError(t, os.Remove(victimShard))

	require.Eventually(t, func() bool {
		info, err := os.Stat(victimShard)
		return err == nil && info.Size() > 0
	}, 30*time.Second, 500*time.Millisecond)

	var found incidentState
	require.Eventually(t, func() bool {
		for _, item := range fetchIncidents(t, endpoints[victimNode]) {
			if item.Cause == "missing_shard" && item.Scope.Bucket == bucketName && item.Scope.Key == keyName && item.State == "fixed" {
				found = item
				return item.Proof.Status == "signed" && item.Proof.ReceiptID != ""
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "fixed missing-shard incident with signed proof not found")

	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
		if testing.Verbose() {
			o.Logger = logging.NewStandardLogger(os.Stderr)
			o.LogSigning = true
		}
	})
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	_, receiptStatus := signedGet(t, ctx, signer, creds, endpoints[victimNode]+"/api/receipts?correlation_id="+url.QueryEscape(found.ID))
	require.Equal(t, http.StatusOK, receiptStatus)
}

func TestE2E_QuarantineIncident(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node e2e in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s - run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-QUARANTINE-INCIDENT-KEY"
		accessKey  = "qi-ak"
		secretKey  = "qi-sk"
		bucketName = "qi-bucket"
		numNodes   = 3
	)
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         numNodes,
		Mode:          ClusterModeStaticPeers,
		ClusterKey:    clusterKey,
		AccessKey:     accessKey,
		SecretKey:     secretKey,
		ECData:        2,
		ECParity:      1,
		LogPrefix:     "grainfs-quarantine-incident",
		ScrubInterval: "2s",
		DisableNFS:    true,
		DisableNBD:    true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	leaderIdx, err := waitForWritableEndpoint(ctx, c.httpURLs, 120*time.Second, 5*time.Second, time.Second, func(attemptCtx context.Context, endpoint string) error {
		return tryCreateBucket(attemptCtx, ecS3Client(endpoint, accessKey, secretKey), bucketName)
	})
	require.NoError(t, err)
	client := c.S3Client(leaderIdx)
	require.Eventually(t, func() bool {
		return tryPutObject(ctx, client, bucketName, "bad", []byte(strings.Repeat("bad", 1024))) == nil
	}, 120*time.Second, time.Second)
	require.Eventually(t, func() bool {
		return tryPutObject(ctx, client, bucketName, "good", []byte("good")) == nil
	}, 120*time.Second, time.Second)

	var corruptShard string
	for i := range numNodes {
		root := filepath.Join(c.dataDirs[i], "shards", bucketName, "bad")
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, _ error) error {
			if d != nil && !d.IsDir() && filepath.Base(p) == "shard_0" {
				corruptShard = p
				return filepath.SkipAll
			}
			return nil
		})
		if corruptShard != "" {
			break
		}
	}
	require.NotEmpty(t, corruptShard)
	f, err := os.OpenFile(corruptShard, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.Eventually(t, func() bool {
		for _, endpoint := range c.httpURLs {
			for _, item := range fetchIncidents(t, endpoint) {
				if item.Cause == "corrupt_blob" || item.Cause == "corrupt_shard" {
					return item.State == "isolated" || item.State == "needs-human"
				}
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond)

	_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String("good")})
	require.NoError(t, err, "unrelated object in same bucket must keep working")
}
