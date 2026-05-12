package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	if testing.Short() {
		t.Skip("skipping DuckDB Iceberg e2e in short mode")
	}

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
	if testing.Short() {
		t.Skip("skipping DuckDB Iceberg cluster e2e in short mode")
	}

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
	t.Helper()

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dataDir,
		"--port", fmt.Sprintf("%d", port),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--encryption-key-file", encKeyFile,
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
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
