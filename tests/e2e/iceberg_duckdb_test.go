//go:build duckdb_e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"io"
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
)

func TestIcebergDuckDBLocalCatalogSurvivesRestartAndDrop(t *testing.T) {
	skipIfShort(t, "skipping DuckDB Iceberg e2e in short mode")

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
}

func TestIcebergDuckDBClusterAnyNodeTableAPI(t *testing.T) {
	skipIfShort(t, "skipping DuckDB Iceberg cluster e2e in short mode")

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
}

// TestAuditIcebergSingleDuckDB starts one node with audit enabled, performs S3
// PUTs, waits for the committer to flush, then verifies the audit.s3 Iceberg
// table via DuckDB.
func TestAuditIcebergSingleDuckDB(t *testing.T) {
	skipIfShort(t, "skipping single-node audit Iceberg DuckDB e2e in short mode")

	const commitInterval = 8 * time.Second
	start := time.Now()
	dataDir, err := os.MkdirTemp("", "grainfs-audit-iceberg-duckdb-*")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir)
	raftPort := freePort()
	encKeyFile := makeSharedEncryptionKeyFile(t)

	server := startIcebergE2EServerWithExtraArgs(t, dataDir, raftPort, encKeyFile,
		"--audit-iceberg=true",
		"--audit-commit-interval", commitInterval.String(),
	)
	ak, sk := bootstrapAdminViaUDS(t, dataDir)
	client := s3ClientFor(server.endpoint, ak, sk)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("grainfs-tables")})
	require.NoError(t, err)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("test-audit-single")})
	require.NoError(t, err)

	const numPuts = 5
	writeStart := time.Now()
	for i := 0; i < numPuts; i++ {
		key := fmt.Sprintf("audit-test-single-obj-%d", i)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("test-audit-single"),
			Key:    aws.String(key),
			Body:   strings.NewReader("hello audit"),
		})
		require.NoError(t, err)
	}
	writeElapsed := time.Since(writeStart)

	queryStart := time.Now()
	countAuditRows(t, server.endpoint, ak, sk,
		"bucket = 'test-audit-single' AND method = 'PUT'",
		numPuts, 30*time.Second,
	)
	countAuditRows(t, server.endpoint, ak, sk,
		"ts >= NOW() - INTERVAL 1 DAY AND operation = 'PutObject' AND bucket = 'test-audit-single'",
		numPuts, 30*time.Second,
	)
	t.Logf("audit_iceberg_single_duckdb puts=%d write_elapsed=%s query_elapsed=%s total_elapsed=%s",
		numPuts, writeElapsed, time.Since(queryStart), time.Since(start))
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
	skipIfShort(t, "skipping audit Iceberg DuckDB e2e in short mode")

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

	cluster.GrantAdminOnBuckets("grainfs-audit", "grainfs-tables", "test-audit")
	client := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("grainfs-tables")})
	require.NoError(t, err)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("test-audit")})
	require.NoError(t, err)
	requireIcebergClusterS3Ready(t, cluster, "grainfs-tables")

	// Emit a few S3 events via PutObject so the ring buffer has events to commit.
	const numPuts = 5
	writeStart := time.Now()
	for i := 0; i < numPuts; i++ {
		key := fmt.Sprintf("audit-test-obj-%d", i)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("test-audit"),
			Key:    aws.String(key),
			Body:   strings.NewReader("hello audit"),
		})
		require.NoError(t, err)
	}
	writeElapsed := time.Since(writeStart)

	queryStart := time.Now()
	countAuditRows(t, cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey,
		"bucket = 'test-audit' AND method = 'PUT'",
		numPuts, 30*time.Second,
	)
	t.Logf("audit_iceberg_cluster_duckdb puts=%d write_elapsed=%s query_elapsed=%s total_elapsed=%s",
		numPuts, writeElapsed, time.Since(queryStart), time.Since(start))
}

// TestAuditIcebergClusterFollowerShipDuckDB verifies that audit events emitted
// on a follower are shipped to the leader and become readable through DuckDB.
func TestAuditIcebergClusterFollowerShipDuckDB(t *testing.T) {
	skipIfShort(t, "skipping audit Iceberg follower shipping DuckDB e2e in short mode")

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
}

// TestAuditIcebergClusterLeaderFlap verifies that audit events captured on followers
// are forwarded and committed after leader re-election.
func TestAuditIcebergClusterLeaderFlap(t *testing.T) {
	skipIfShort(t, "skipping audit Iceberg leader-flap e2e in short mode")

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
	AUTHORIZATION_TYPE 'none',
	ACCESS_DELEGATION_MODE 'none'
);
%s
`, accessKey, secretKey, endpointHost, endpoint, query)
}
