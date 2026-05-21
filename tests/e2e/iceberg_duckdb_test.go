package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func runIcebergDuckDBLocalCatalogSurvivesRestartAndDrop(t testing.TB) {
	dataDir, err := os.MkdirTemp("", "grainfs-iceberg-duckdb-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(func() { _ = os.RemoveAll(dataDir) })
	raftPort := freePort()
	encKeyFile := makeSharedEncryptionKeyFile(t)

	server := startIcebergE2EServer(t, dataDir, raftPort, encKeyFile)
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dataDir}, 60*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey
	createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dataDir}, bootstrap.SAID, "grainfs-tables", ecS3Client(server.endpoint, ak, sk))

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

func runIcebergDuckDBClusterAnyNodeTableAPI(t testing.TB) {
	cluster := startStaticMRClusterWithOptions(t, 3, mrClusterOptions{
		disableNFS4: true,
		disableNBD:  true,
	})
	client := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
	createBucketWithAdminPolicyAttachViaUDSAny(t, cluster.dataDirs, cluster.saID, "grainfs-tables", client)
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

// runIcebergAuditCases drives the shared audit-iceberg subtests against any
// icebergTarget that was booted with --audit-iceberg. Each subtest uses a
// per-case bucket (caseSeq + tgt.name) so cluster targets can be reused
// across multiple cases without cross-contamination.
func runIcebergAuditCases(t testing.TB, tgt *icebergTarget, commitInterval time.Duration) {
	id := tgt.caseSeq.Add(1)
	bucket := fmt.Sprintf("test-audit-%s-%d", tgt.name, id)
	ginkgo.DeferCleanup(func() {
		_, _ = tgt.s3Client(0).DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	ginkgo.DeferCleanup(cancel)
	tgt.createBucketWithAdminPolicy(t, bucket)

	const numPuts = 5
	writeStart := time.Now()
	for i := 0; i < numPuts; i++ {
		key := fmt.Sprintf("audit-test-obj-%d", i)
		_, err := tgt.s3Client(0).PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("hello audit"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
}

// TestAuditIcebergSingleDuckDB starts one node with audit enabled, performs S3
// PUTs, waits for the committer to flush, then verifies the audit.s3 Iceberg
// table via DuckDB.
func runAuditIcebergSingleDuckDB(t testing.TB) {
	const commitInterval = 8 * time.Second
	tgt := newSingleNodeIcebergTargetWithAudit(t, commitInterval)
	runIcebergAuditCases(t, tgt, commitInterval)
}

func requireAuditSearchAPIRows(t testing.TB, endpoint, accessKey, secretKey, bucket string, want int, timeout time.Duration) {
	t.Helper()
	gomega.Eventually(func() bool {
		req, err := http.NewRequest(http.MethodGet, endpoint+"/api/audit/s3?bucket="+bucket+"&operation=PutObject&limit=20", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	}).WithTimeout(timeout).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "audit search API must return committed PutObject rows")
}

func requireIcebergClusterS3Ready(t testing.TB, cluster *mrCluster, bucket string) {
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	gomega.Eventually(func() bool {
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
	}).WithTimeout(45 * time.Second).WithPolling(500 * time.Millisecond).Should(gomega.BeTrue())
}

type icebergE2EServer struct {
	endpoint string
	stop     func()
}

func startIcebergE2EServer(t testing.TB, dataDir string, raftPort int, encKeyFile string) icebergE2EServer {
	return startIcebergE2EServerWithExtraArgs(t, dataDir, raftPort, encKeyFile)
}

func startIcebergE2EServerWithExtraArgs(t testing.TB, dataDir string, raftPort int, encKeyFile string, extraArgs ...string) icebergE2EServer {
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).To(gomega.Succeed())

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
	ginkgo.DeferCleanup(stop)

	return icebergE2EServer{
		endpoint: fmt.Sprintf("http://127.0.0.1:%d", port),
		stop:     stop,
	}
}

func runDuckDBIcebergSQLWithCreds(t testing.TB, endpoint, accessKey, secretKey, query, want string) {
	t.Helper()

	got := runDuckDBIcebergCLI(t, endpoint, accessKey, secretKey, query)
	gomega.Expect(got).To(gomega.Equal(want))
}

func runDuckDBIcebergExecWithCreds(t testing.TB, endpoint, accessKey, secretKey, query string) {
	t.Helper()
	_ = runDuckDBIcebergCLI(t, endpoint, accessKey, secretKey, query)
}

func runDuckDBIcebergCLI(t testing.TB, endpoint, accessKey, secretKey, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "duckdb", "-csv", "-noheader", "-c", duckDBIcebergSQL(endpoint, accessKey, secretKey, query))
	out, err := cmd.CombinedOutput()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "duckdb output:\n%s", out)
	return lastNonEmptyLine(string(out))
}

func lastNonEmptyLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}

// TestAuditIcebergClusterDuckDB starts a 3-node cluster with audit enabled and a
// short commit interval, performs S3 PUTs, waits for the committer to flush,
// then verifies the audit.s3 Iceberg table contains the expected rows via DuckDB.
func runAuditIcebergClusterDuckDB(t testing.TB) {
	const commitInterval = 8 * time.Second
	tgt := newSharedClusterIcebergTargetWithAudit(t, commitInterval)
	runIcebergAuditCases(t, tgt, commitInterval)
}

// TestAuditIcebergClusterFollowerShipDuckDB verifies that audit events emitted
// on a follower are shipped to the leader and become readable through DuckDB.
func runAuditIcebergClusterFollowerShipDuckDB(t testing.TB) {
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
	ginkgo.DeferCleanup(cancel)

	cluster.GrantAdminOnBuckets("grainfs-audit")
	leaderClient := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
	createBucketWithAdminPolicyAttachViaUDSAny(t, cluster.dataDirs, cluster.saID, "grainfs-tables", leaderClient)
	createBucketWithAdminPolicyAttachViaUDSAny(t, cluster.dataDirs, cluster.saID, "test-audit-follower", leaderClient)
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
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
func runAuditIcebergClusterLeaderFlap(t testing.TB) {
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
	ginkgo.DeferCleanup(cancel)

	cluster.GrantAdminOnBuckets("grainfs-audit")
	client := ecS3Client(cluster.httpURLs[cluster.leaderIdx], cluster.accessKey, cluster.secretKey)
	createBucketWithAdminPolicyAttachViaUDSAny(t, cluster.dataDirs, cluster.saID, "grainfs-tables", client)
	createBucketWithAdminPolicyAttachViaUDSAny(t, cluster.dataDirs, cluster.saID, "flap-bucket", client)
	requireIcebergClusterS3Ready(t, cluster, "grainfs-tables")

	// Write on a follower so events need shipping.
	followerIdx := (cluster.leaderIdx + 1) % 3
	followerClient := ecS3Client(cluster.httpURLs[followerIdx], cluster.accessKey, cluster.secretKey)
	_, err := followerClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("flap-bucket"),
		Key:    aws.String("before-flap"),
		Body:   strings.NewReader("pre-flap data"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Kill the leader to force re-election.
	oldLeaderIdx := cluster.leaderIdx
	leaderProc := cluster.procs[cluster.leaderIdx]
	if leaderProc != nil && leaderProc.Process != nil {
		gomega.Expect(leaderProc.Process.Signal(syscall.SIGTERM)).To(gomega.Succeed())
		_ = leaderProc.Wait()
		cluster.procs[oldLeaderIdx] = nil
	}

	time.Sleep(2 * time.Second)
	cluster.procs[oldLeaderIdx] = cluster.startNode(oldLeaderIdx)
	waitForPort(t, cluster.httpPorts[oldLeaderIdx], 60*time.Second)

	// Poll until the restarted node ships or commits its durable outbox.
	countAuditRowsAnyEndpoint(t, cluster.httpURLs, cluster.accessKey, cluster.secretKey,
		"bucket = 'flap-bucket'",
		1, 90*time.Second,
	)
}

// countAuditRows polls the server-side audit searcher until the expected count
// is reached. The searcher itself uses DuckDB with the internal audit reader
// credential; direct test-S3 credentials cannot read the grainfs-audit bucket.
func countAuditRows(t testing.TB, endpoint, accessKey, secretKey, whereClause string, want int, timeout time.Duration) {
	t.Helper()
	bucket := auditWhereBucket(whereClause)
	gomega.Expect(bucket).NotTo(gomega.BeEmpty(), "countAuditRows requires a bucket predicate: %s", whereClause)
	operation := ""
	if strings.Contains(whereClause, "method = 'PUT'") {
		operation = "PutObject"
	}

	deadline := time.Now().Add(timeout)
	var lastRows int
	var lastErr error
	for time.Now().Before(deadline) {
		rows, err := auditSearchRows(t, endpoint, accessKey, secretKey, bucket, operation)
		if err != nil {
			lastErr = err
		} else {
			lastRows = rows
			if rows >= want {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	ginkgo.Fail(fmt.Sprintf("audit rows not committed: timeout=%s bucket=%q operation=%q rows=%d want=%d lastErr=%v",
		timeout, bucket, operation, lastRows, want, lastErr))
}

func countAuditRowsAnyEndpoint(t testing.TB, endpoints []string, accessKey, secretKey, whereClause string, want int, timeout time.Duration) {
	t.Helper()
	bucket := auditWhereBucket(whereClause)
	gomega.Expect(bucket).NotTo(gomega.BeEmpty(), "countAuditRowsAnyEndpoint requires a bucket predicate: %s", whereClause)
	operation := ""
	if strings.Contains(whereClause, "method = 'PUT'") {
		operation = "PutObject"
	}

	deadline := time.Now().Add(timeout)
	var lastRows int
	var lastEndpoint string
	var lastErr error
	for time.Now().Before(deadline) {
		for _, endpoint := range endpoints {
			rows, err := auditSearchRows(t, endpoint, accessKey, secretKey, bucket, operation)
			if err != nil {
				lastErr = err
				lastEndpoint = endpoint
				continue
			}
			lastRows = rows
			lastEndpoint = endpoint
			if rows >= want {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	ginkgo.Fail(fmt.Sprintf("audit rows not committed: timeout=%s bucket=%q operation=%q endpoint=%q rows=%d want=%d lastErr=%v",
		timeout, bucket, operation, lastEndpoint, lastRows, want, lastErr))
}

func auditWhereBucket(whereClause string) string {
	const marker = "bucket = '"
	idx := strings.Index(whereClause, marker)
	if idx < 0 {
		return ""
	}
	rest := whereClause[idx+len(marker):]
	end := strings.Index(rest, "'")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func auditSearchRows(t testing.TB, endpoint, accessKey, secretKey, bucket, operation string) (int, error) {
	t.Helper()
	q := url.Values{}
	q.Set("bucket", bucket)
	q.Set("limit", "100")
	if operation != "" {
		q.Set("operation", operation)
	}
	req, err := http.NewRequest(http.MethodGet, endpoint+"/api/audit/s3?"+q.Encode(), nil)
	if err != nil {
		return 0, err
	}
	req.Host = req.URL.Host
	s3auth.SignRequest(req, accessKey, secretKey, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("audit search status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var rows []struct {
		Bucket    string `json:"bucket"`
		Operation string `json:"operation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return 0, err
	}
	var got int
	for _, row := range rows {
		if row.Bucket == bucket && (operation == "" || row.Operation == operation) {
			got++
		}
	}
	return got, nil
}

func duckDBIcebergSQL(endpoint, accessKey, secretKey, query string) string {
	endpointHost := strings.TrimPrefix(endpoint, "http://")
	return fmt.Sprintf(`
INSTALL httpfs;
	INSTALL iceberg;
	LOAD httpfs;
	LOAD iceberg;
	SET s3_access_key_id='%s';
	SET s3_secret_access_key='%s';
	SET s3_region='us-east-1';
	SET s3_endpoint='%s';
	SET s3_url_style='path';
	SET s3_use_ssl=false;
	SET iceberg_via_aws_sdk_for_catalog_interactions=true;
	CREATE OR REPLACE SECRET grainfs_s3 (
		TYPE s3,
		KEY_ID '%s',
	SECRET '%s',
	REGION 'us-east-1',
	ENDPOINT '%s',
		URL_STYLE 'path',
		USE_SSL false
	);
	CREATE OR REPLACE SECRET grainfs_iceberg_oauth (
		TYPE iceberg,
		CLIENT_ID '%s',
		CLIENT_SECRET '%s',
		OAUTH2_SERVER_URI '%s/iceberg/v1/oauth/tokens',
		OAUTH2_SCOPE 'PRINCIPAL_ROLE:grainfs'
	);
	ATTACH 'grainfs' AS grainfs_iceberg (
		TYPE iceberg,
		SECRET grainfs_iceberg_oauth,
		ENDPOINT '%s/iceberg',
		AUTHORIZATION_TYPE 'oauth2'
	);
	%s
	`, accessKey, secretKey, endpointHost, accessKey, secretKey, endpointHost, accessKey, secretKey, endpoint, endpoint, query)
}

var _ = ginkgo.Describe("Iceberg DuckDB", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("keeps the local catalog readable across restart and drop", func() {
			runIcebergDuckDBLocalCatalogSurvivesRestartAndDrop(ginkgo.GinkgoTB())
		})
		ginkgo.It("commits audit rows to an Iceberg table", func() {
			runAuditIcebergSingleDuckDB(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("Cluster3Node", func() {
		ginkgo.It("serves table API operations from any node", func() {
			runIcebergDuckDBClusterAnyNodeTableAPI(ginkgo.GinkgoTB())
		})
		ginkgo.It("commits cluster audit rows to an Iceberg table", func() {
			runAuditIcebergClusterDuckDB(ginkgo.GinkgoTB())
		})
		ginkgo.It("ships follower audit rows to the leader", func() {
			runAuditIcebergClusterFollowerShipDuckDB(ginkgo.GinkgoTB())
		})
		ginkgo.It("commits follower audit rows after a leader flap", func() {
			runAuditIcebergClusterLeaderFlap(ginkgo.GinkgoTB())
		})
	})
})
