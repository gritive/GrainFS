package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestIcebergDuckDBLocalCatalogSurvivesRestartAndDrop(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {

		dataDir, err := os.MkdirTemp("", "grainfs-iceberg-duckdb-*")
		require.NoError(t, err)
		defer os.RemoveAll(dataDir)
		raftPort := freePort()
		encKeyFile := makeSharedEncryptionKeyFile(t)

		server := startIcebergE2EServer(t, dataDir, raftPort, encKeyFile)
		ak, sk := bootstrapAdminViaUDS(t, dataDir)
		createE2EBucketWithCreds(t, server.endpoint, "grainfs-tables", ak, sk)

		runDuckDBIcebergSQLWithCreds(t, server.endpoint, ak, sk, `
CREATE SCHEMA grainfs_iceberg.ns_e2e;
CREATE TABLE grainfs_iceberg.ns_e2e.t (a INTEGER);
INSERT INTO grainfs_iceberg.ns_e2e.t VALUES (42), (7);
SELECT CAST(sum(a) AS VARCHAR) AS total FROM grainfs_iceberg.ns_e2e.t;
`, "49")

		server.stop()

		server = startIcebergE2EServer(t, dataDir, raftPort, encKeyFile)
		runDuckDBIcebergSQLWithCreds(t, server.endpoint, ak, sk, `
SELECT CAST(sum(a) AS VARCHAR) AS total FROM grainfs_iceberg.ns_e2e.t;
`, "49")
		runDuckDBIcebergExecWithCreds(t, server.endpoint, ak, sk, `
DROP TABLE grainfs_iceberg.ns_e2e.t;
DROP SCHEMA grainfs_iceberg.ns_e2e;
	`)
	})
}

func TestIcebergDuckDBClusterAnyNodeTableAPI(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {

		cluster := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
			disableNFS4: true,
			disableNBD:  true,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cluster.GrantAdminOnBuckets("grainfs-tables")
		client := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("grainfs-tables")})
		require.NoError(t, err)
		requireIcebergClusterS3Ready(t, cluster, "grainfs-tables")

		runDuckDBIcebergSQLWithCreds(t, cluster.httpURLs[0], cluster.accessKey, cluster.secretKey, `
CREATE SCHEMA grainfs_iceberg.ns_cluster_e2e;
CREATE TABLE grainfs_iceberg.ns_cluster_e2e.t (a INTEGER);
INSERT INTO grainfs_iceberg.ns_cluster_e2e.t VALUES (10), (5);
SELECT CAST(sum(a) AS VARCHAR) AS total FROM grainfs_iceberg.ns_cluster_e2e.t;
`, "15")

		runDuckDBIcebergSQLWithCreds(t, cluster.httpURLs[1], cluster.accessKey, cluster.secretKey, `
INSERT INTO grainfs_iceberg.ns_cluster_e2e.t VALUES (7);
SELECT CAST(sum(a) AS VARCHAR) AS total FROM grainfs_iceberg.ns_cluster_e2e.t;
`, "22")

		runDuckDBIcebergSQLWithCreds(t, cluster.httpURLs[2], cluster.accessKey, cluster.secretKey, `
SELECT CAST(sum(a) AS VARCHAR) AS total FROM grainfs_iceberg.ns_cluster_e2e.t;
`, "22")

		runDuckDBIcebergExecWithCreds(t, cluster.httpURLs[2], cluster.accessKey, cluster.secretKey, `
DROP TABLE grainfs_iceberg.ns_cluster_e2e.t;
DROP SCHEMA grainfs_iceberg.ns_cluster_e2e;
	`)
	})
}

// runIcebergAuditCases drives the shared audit-iceberg subtests against any
// icebergTarget that was booted with --audit-iceberg. Each subtest uses a
// per-case bucket (caseSeq + tgt.name) so cluster targets can be reused
// across multiple cases without cross-contamination.
func runIcebergAuditCases(t *testing.T, tgt *icebergTarget, commitInterval time.Duration) {
	t.Run("AuditPutCommitsToIcebergTable", func(t *testing.T) {
		id := tgt.caseSeq.Add(1)
		bucket := fmt.Sprintf("test-audit-%s-%d", tgt.name, id)
		t.Cleanup(func() {
			_, _ = tgt.s3Client(0).DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
		})

		if tgt.isCluster && tgt.cluster != nil {
			tgt.cluster.GrantAdminOnBuckets(bucket)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, err := tgt.s3Client(0).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)

		const numPuts = 5
		writeStart := time.Now()
		for i := 0; i < numPuts; i++ {
			key := fmt.Sprintf("audit-test-obj-%d", i)
			_, err := tgt.s3Client(0).PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("hello audit"),
			})
			require.NoError(t, err)
		}
		writeElapsed := time.Since(writeStart)

		queryStart := time.Now()
		countAuditRows(t, tgt.endpoint(0), tgt.accessKey, tgt.secretKey,
			fmt.Sprintf("bucket = '%s' AND method = 'PUT'", bucket),
			numPuts, 30*time.Second)
		requireAuditSearchAPIRows(t, tgt.endpoint(0), tgt.accessKey, tgt.secretKey,
			bucket, numPuts, 30*time.Second)
		t.Logf("audit_iceberg_%s puts=%d commit_interval=%s write_elapsed=%s query_elapsed=%s",
			tgt.name, numPuts, commitInterval, writeElapsed, time.Since(queryStart))
	})
}

// TestAuditIcebergSingleDuckDB starts one node with audit enabled, performs S3
// PUTs, waits for the committer to flush, then verifies the audit.s3 Iceberg
// table via DuckDB.
func TestAuditIcebergSingleDuckDB(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		const commitInterval = 8 * time.Second
		tgt := newSingleNodeIcebergTargetWithAudit(t, commitInterval)
		runIcebergAuditCases(t, tgt, commitInterval)
	})
}

func requireAuditSearchAPIRows(t *testing.T, endpoint, accessKey, secretKey, bucket string, want int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		req, err := http.NewRequest(http.MethodGet, endpoint+"/api/audit/s3?bucket="+bucket+"&operation=PutObject&limit=20", nil)
		require.NoError(t, err)
		req.Host = req.URL.Host
		s3auth.SignRequest(req, accessKey, secretKey, "us-east-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, resp.Body)
			return false
		}
		var rows []struct {
			Bucket    string `json:"bucket"`
			Operation string `json:"operation"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			return false
		}
		var got int
		for _, row := range rows {
			if row.Bucket == bucket && row.Operation == "PutObject" {
				got++
			}
		}
		return got >= want
	}, timeout, 500*time.Millisecond, "audit search API must return committed PutObject rows")
}

func requireIcebergClusterS3Ready(t *testing.T, cluster *mrCluster, bucket string) {
	t.Helper()

	const key = "__iceberg_cluster_readiness"
	const body = "ready"
	writer := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := writer.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, endpoint := range cluster.httpURLs {
			readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
			out, err := ecS3Client(endpoint, cluster.accessKey, cluster.secretKey).GetObject(readCtx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				readCancel()
				return false
			}
			got, err := io.ReadAll(out.Body)
			_ = out.Body.Close()
			readCancel()
			if err != nil || string(got) != body {
				return false
			}
		}
		return true
	}, 45*time.Second, 500*time.Millisecond)
}

type icebergE2EServer struct {
	endpoint string
	stop     func()
}

func startIcebergE2EServer(t *testing.T, dataDir string, raftPort int, encKeyFile string) icebergE2EServer {
	return startIcebergE2EServerWithExtraArgs(t, dataDir, raftPort, encKeyFile)
}

func startIcebergE2EServerWithExtraArgs(t *testing.T, dataDir string, raftPort int, encKeyFile string, extraArgs ...string) icebergE2EServer {
	t.Helper()

	port := freePort()
	args := []string{
		"serve",
		"--data", dataDir,
		"--port", fmt.Sprintf("%d", port),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--encryption-key-file", encKeyFile,
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(getBinary(), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	waitForPort(t, port, 10*time.Second)
	time.Sleep(3 * time.Second)
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}
	t.Cleanup(stop)

	return icebergE2EServer{
		endpoint: fmt.Sprintf("http://127.0.0.1:%d", port),
		stop:     stop,
	}
}

func createE2EBucketWithCreds(t *testing.T, endpoint, bucket, accessKey, secretKey string) {
	t.Helper()
	client := s3ClientFor(endpoint, accessKey, secretKey)
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

func runDuckDBIcebergSQLWithCreds(t *testing.T, endpoint, accessKey, secretKey, query, want string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.QueryContext(ctx, duckDBIcebergSQL(endpoint, accessKey, secretKey, query))
	require.NoError(t, err)
	defer rows.Close()

	var got string
	require.True(t, rows.Next(), "duckdb query returned no rows")
	require.NoError(t, rows.Scan(&got))
	require.Equal(t, want, got)
}

func runDuckDBIcebergExecWithCreds(t *testing.T, endpoint, accessKey, secretKey, query string) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, err = db.ExecContext(ctx, duckDBIcebergSQL(endpoint, accessKey, secretKey, query))
	require.NoError(t, err)
}

// TestAuditIcebergClusterDuckDB starts a 3-node cluster with audit enabled and a
// short commit interval, performs S3 PUTs, waits for the committer to flush,
// then verifies the audit.s3 Iceberg table contains the expected rows via DuckDB.
func TestAuditIcebergClusterDuckDB(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {
		const commitInterval = 8 * time.Second
		tgt := newSharedClusterIcebergTargetWithAudit(t, commitInterval)
		runIcebergAuditCases(t, tgt, commitInterval)
	})
}

// TestAuditIcebergClusterFollowerShipDuckDB verifies that audit events emitted
// on a follower are shipped to the leader and become readable through DuckDB.
func TestAuditIcebergClusterFollowerShipDuckDB(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {

		const commitInterval = 8 * time.Second
		start := time.Now()
		cluster := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
			disableNFS4: true,
			disableNBD:  true,
			ExtraArgs: []string{
				"--audit-iceberg=true",
				"--audit-commit-interval", commitInterval.String(),
			},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cluster.GrantAdminOnBuckets("grainfs-audit", "grainfs-tables", "test-audit-follower")
		leaderClient := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
		_, err := leaderClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("grainfs-tables")})
		require.NoError(t, err)
		_, err = leaderClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("test-audit-follower")})
		require.NoError(t, err)
		requireIcebergClusterS3Ready(t, cluster, "grainfs-tables")

		followerIdx := (cluster.leaderIdx + 1) % len(cluster.httpURLs)
		followerClient := ecS3Client(cluster.httpURLs[followerIdx], cluster.accessKey, cluster.secretKey)
		const numPuts = 3
		writeStart := time.Now()
		for i := 0; i < numPuts; i++ {
			key := fmt.Sprintf("audit-follower-obj-%d", i)
			_, err := followerClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("test-audit-follower"),
				Key:    aws.String(key),
				Body:   strings.NewReader("hello follower audit"),
			})
			require.NoError(t, err)
		}
		writeElapsed := time.Since(writeStart)

		queryStart := time.Now()
		countAuditRows(t, cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey,
			"bucket = 'test-audit-follower' AND method = 'PUT'",
			numPuts, 30*time.Second,
		)
		t.Logf("audit_iceberg_cluster_follower_ship_duckdb puts=%d follower_idx=%d write_elapsed=%s query_elapsed=%s total_elapsed=%s",
			numPuts, followerIdx, writeElapsed, time.Since(queryStart), time.Since(start))
	})
}

// TestAuditIcebergClusterLeaderFlap verifies that audit events captured on followers
// are forwarded and committed after leader re-election.
func TestAuditIcebergClusterLeaderFlap(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {

		const commitInterval = 8 * time.Second
		cluster := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
			disableNFS4: true,
			disableNBD:  true,
			ExtraArgs: []string{
				"--audit-iceberg=true",
				"--audit-commit-interval", commitInterval.String(),
			},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		cluster.GrantAdminOnBuckets("grainfs-audit", "grainfs-tables", "flap-bucket")
		client := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("grainfs-tables")})
		require.NoError(t, err)
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("flap-bucket")})
		require.NoError(t, err)
		requireIcebergClusterS3Ready(t, cluster, "grainfs-tables")

		// Write on a follower so events need shipping.
		followerIdx := (cluster.leaderIdx + 1) % 3
		followerClient := ecS3Client(cluster.httpURLs[followerIdx], cluster.accessKey, cluster.secretKey)
		_, err = followerClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("flap-bucket"),
			Key:    aws.String("before-flap"),
			Body:   strings.NewReader("pre-flap data"),
		})
		require.NoError(t, err)

		// Kill the leader to force re-election.
		leaderProc := cluster.procs[cluster.leaderIdx]
		if leaderProc != nil && leaderProc.Process != nil {
			_ = leaderProc.Process.Signal(syscall.SIGTERM)
		}

		// Find a surviving node.
		var survivorEndpoint string
		for i, url := range cluster.httpURLs {
			if i != cluster.leaderIdx {
				survivorEndpoint = url
				break
			}
		}

		// Poll until re-election and commit complete.
		countAuditRows(t, survivorEndpoint, cluster.accessKey, cluster.secretKey,
			"bucket = 'flap-bucket'",
			1, 45*time.Second,
		)
	})
}

// countAuditRows polls the audit.s3 Iceberg table via DuckDB until the expected
// count is reached or the timeout expires. Using require.Eventually avoids fixed
// sleeps that are flaky on slow CI machines.
func countAuditRows(t *testing.T, endpoint, accessKey, secretKey, whereClause string, want int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return false
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		q := fmt.Sprintf(`SELECT CAST(COUNT(*) AS VARCHAR) AS cnt FROM grainfs_iceberg.audit.s3 WHERE %s`, whereClause)
		row := db.QueryRowContext(ctx, duckDBIcebergSQL(endpoint, accessKey, secretKey, q))
		var got string
		if err := row.Scan(&got); err != nil {
			return false
		}
		n, err := strconv.Atoi(got)
		return err == nil && n >= want
	}, timeout, 500*time.Millisecond, "audit rows not committed within %s", timeout)
}

func duckDBIcebergSQL(endpoint, accessKey, secretKey, query string) string {
	endpointHost := strings.TrimPrefix(endpoint, "http://")
	return fmt.Sprintf(`
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;
CREATE OR REPLACE SECRET grainfs_s3 (
	TYPE s3,
	KEY_ID '%s',
	SECRET '%s',
	REGION 'us-east-1',
	ENDPOINT '%s',
	URL_STYLE 'path',
	USE_SSL false
);
ATTACH 'grainfs' AS grainfs_iceberg (
	TYPE iceberg,
	ENDPOINT '%s/iceberg',
	AUTHORIZATION_TYPE 'sigv4',
	SIGV4_REGION 'us-east-1',
	SIGV4_SERVICE 's3'
);
%s
`, accessKey, secretKey, endpointHost, endpoint, query)
}
