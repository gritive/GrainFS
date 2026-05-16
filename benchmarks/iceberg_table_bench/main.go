package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	awsRegion         = "us-east-1"
	awsService        = "s3"
	warehouseBucket   = "grainfs-tables"
	defaultReportPath = "benchmarks/iceberg_table_report.json"
)

type config struct {
	baseURL         string
	accessKey       string
	secretKey       string
	concurrency     int
	duration        time.Duration
	namespacePrefix string
	reportPath      string
}

type runner struct {
	cfg    config
	client *http.Client

	totalRequests  atomic.Uint64
	failedRequests atomic.Uint64
	failuresMu     sync.Mutex
	failures       []failureSample

	createNamespace *operationStats
	createTable     *operationStats
	loadTable       *operationStats
	commitTable     *operationStats
	deleteTable     *operationStats
	deleteNamespace *operationStats
}

type operationStats struct {
	mu      sync.Mutex
	samples []float64
}

func newOperationStats() *operationStats {
	return &operationStats{}
}

func (s *operationStats) add(ms float64) {
	s.mu.Lock()
	s.samples = append(s.samples, ms)
	s.mu.Unlock()
}

func (s *operationStats) summary() operationSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return operationSummary{Ops: 0}
	}
	values := append([]float64(nil), s.samples...)
	sort.Float64s(values)
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return operationSummary{
		Ops:   len(values),
		P50MS: fmt.Sprintf("%.2f", percentile(values, 0.50)),
		P99MS: fmt.Sprintf("%.2f", percentile(values, 0.99)),
		AvgMS: fmt.Sprintf("%.2f", sum/float64(len(values))),
		MinMS: fmt.Sprintf("%.2f", values[0]),
		MaxMS: fmt.Sprintf("%.2f", values[len(values)-1]),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

type operationSummary struct {
	Ops   int    `json:"ops"`
	P50MS string `json:"p50_ms,omitempty"`
	P99MS string `json:"p99_ms,omitempty"`
	AvgMS string `json:"avg_ms,omitempty"`
	MinMS string `json:"min_ms,omitempty"`
	MaxMS string `json:"max_ms,omitempty"`
}

type reportSummary struct {
	TotalRequests  uint64 `json:"total_requests"`
	FailedRequests uint64 `json:"failed_requests"`
}

type failureSample struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Status int    `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
	Body   string `json:"body,omitempty"`
}

type benchmarkReport struct {
	Timestamp       string           `json:"timestamp"`
	Summary         reportSummary    `json:"summary"`
	Failures        []failureSample  `json:"failures,omitempty"`
	CreateNamespace operationSummary `json:"create_namespace"`
	CreateTable     operationSummary `json:"create_table"`
	LoadTable       operationSummary `json:"load_table"`
	CommitTable     operationSummary `json:"commit_table"`
	DeleteTable     operationSummary `json:"delete_table"`
	DeleteNamespace operationSummary `json:"delete_namespace"`
}

func parseConfig() (config, error) {
	cfg := config{}
	flag.StringVar(&cfg.baseURL, "base-url", "http://127.0.0.1:9000", "GrainFS base URL")
	flag.StringVar(&cfg.accessKey, "access-key", "", "S3 access key for warehouse bucket setup")
	flag.StringVar(&cfg.secretKey, "secret-key", "", "S3 secret key for warehouse bucket setup")
	flag.IntVar(&cfg.concurrency, "concurrency", 10, "number of concurrent workers")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "benchmark duration")
	flag.StringVar(&cfg.namespacePrefix, "namespace-prefix", "bench_ns", "Iceberg namespace prefix")
	flag.StringVar(&cfg.reportPath, "report", defaultReportPath, "JSON report path")
	flag.Parse()
	if cfg.concurrency < 1 {
		return cfg, fmt.Errorf("concurrency must be >= 1")
	}
	if cfg.duration <= 0 {
		return cfg, fmt.Errorf("duration must be > 0")
	}
	cfg.baseURL = strings.TrimRight(cfg.baseURL, "/")
	if _, err := url.ParseRequestURI(cfg.baseURL); err != nil {
		return cfg, fmt.Errorf("base-url: %w", err)
	}
	return cfg, nil
}

func newRunner(cfg config) *runner {
	maxConns := max(32, cfg.concurrency*8)
	return &runner{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				MaxIdleConns:        maxConns,
				MaxIdleConnsPerHost: maxConns,
				MaxConnsPerHost:     maxConns,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},

		createNamespace: newOperationStats(),
		createTable:     newOperationStats(),
		loadTable:       newOperationStats(),
		commitTable:     newOperationStats(),
		deleteTable:     newOperationStats(),
		deleteNamespace: newOperationStats(),
	}
}

func (r *runner) run(ctx context.Context) benchmarkReport {
	stopAt := time.Now().Add(r.cfg.duration)
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.concurrency; i++ {
		wg.Add(1)
		go r.runWorker(ctx, stopAt, i, &wg)
	}
	wg.Wait()

	return benchmarkReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Summary: reportSummary{
			TotalRequests:  r.totalRequests.Load(),
			FailedRequests: r.failedRequests.Load(),
		},
		Failures:        r.failureSamples(),
		CreateNamespace: r.createNamespace.summary(),
		CreateTable:     r.createTable.summary(),
		LoadTable:       r.loadTable.summary(),
		CommitTable:     r.commitTable.summary(),
		DeleteTable:     r.deleteTable.summary(),
		DeleteNamespace: r.deleteNamespace.summary(),
	}
}

func (r *runner) runWorker(ctx context.Context, stopAt time.Time, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	var iter uint64
	for time.Now().Before(stopAt) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		iter++
		r.runLifecycle(ctx, workerID, iter)
	}
}

func (r *runner) runLifecycle(ctx context.Context, workerID int, iter uint64) {
	suffix := fmt.Sprintf("%d_%d_%d", workerID, iter, time.Now().UnixNano())
	namespace := fmt.Sprintf("%s_%s", r.cfg.namespacePrefix, suffix)
	table := fmt.Sprintf("t_%s", suffix)
	tableLocation := fmt.Sprintf("s3://%s/warehouse/%s/%s", warehouseBucket, namespace, table)

	if ok, _ := r.doJSON(ctx, http.MethodGet, "/iceberg/v1/config?warehouse=warehouse", nil, http.StatusOK); !ok {
		return
	}

	body := mustJSON(map[string]any{
		"namespace":  []string{namespace},
		"properties": map[string]string{"owner": "go-bench"},
	})
	ok, elapsed := r.doJSON(ctx, http.MethodPost, "/iceberg/v1/namespaces", body, http.StatusOK, http.StatusConflict)
	r.createNamespace.add(ms(elapsed))
	if !ok {
		return
	}

	body = mustJSON(map[string]any{
		"name": table,
		"schema": map[string]any{
			"type":      "struct",
			"schema-id": 0,
			"fields": []map[string]any{
				{"id": 1, "name": "a", "required": false, "type": "int"},
			},
		},
		"properties": map[string]string{"benchmark": "iceberg-table-api"},
	})
	ok, elapsed = r.doJSON(ctx, http.MethodPost, "/iceberg/v1/namespaces/"+url.PathEscape(namespace)+"/tables", body, http.StatusOK)
	r.createTable.add(ms(elapsed))
	if !ok {
		return
	}

	tablePath := "/iceberg/v1/namespaces/" + url.PathEscape(namespace) + "/tables/" + url.PathEscape(table)
	ok, elapsed = r.doJSON(ctx, http.MethodGet, tablePath, nil, http.StatusOK)
	r.loadTable.add(ms(elapsed))
	if !ok {
		return
	}

	snapshotID := time.Now().UnixNano() + int64(workerID)*1_000_000 + int64(iter)
	body = mustJSON(map[string]any{
		"requirements": []map[string]any{
			{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": nil},
		},
		"updates": []map[string]any{
			{
				"action": "add-snapshot",
				"snapshot": map[string]any{
					"snapshot-id":     snapshotID,
					"sequence-number": 1,
					"timestamp-ms":    time.Now().UnixMilli(),
					"manifest-list":   fmt.Sprintf("%s/metadata/snap-%d.avro", tableLocation, snapshotID),
					"summary":         map[string]string{"operation": "append"},
					"schema-id":       0,
				},
			},
			{
				"action":      "set-snapshot-ref",
				"ref-name":    "main",
				"type":        "branch",
				"snapshot-id": snapshotID,
			},
		},
	})
	ok, elapsed = r.doJSON(ctx, http.MethodPost, tablePath, body, http.StatusOK)
	r.commitTable.add(ms(elapsed))
	if !ok {
		return
	}

	ok, elapsed = r.doJSON(ctx, http.MethodDelete, tablePath, nil, http.StatusNoContent)
	r.deleteTable.add(ms(elapsed))
	if !ok {
		return
	}

	ok, elapsed = r.doJSON(ctx, http.MethodDelete, "/iceberg/v1/namespaces/"+url.PathEscape(namespace), nil, http.StatusNoContent)
	r.deleteNamespace.add(ms(elapsed))
	_ = ok
}

func (r *runner) setupWarehouseBucket(ctx context.Context) error {
	endpoint := r.cfg.baseURL + "/" + warehouseBucket
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, nil)
	if err != nil {
		return err
	}
	signS3(req, nil, r.cfg.accessKey, r.cfg.secretKey, time.Now().UTC())
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (r *runner) doJSON(ctx context.Context, method, path string, body []byte, want ...int) (bool, time.Duration) {
	endpoint := r.cfg.baseURL + path
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		r.totalRequests.Add(1)
		r.failedRequests.Add(1)
		r.recordFailure(failureSample{Method: method, Path: path, Error: err.Error()})
		return false, 0
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	start := time.Now()
	resp, err := r.client.Do(req)
	elapsed := time.Since(start)
	r.totalRequests.Add(1)
	if err != nil {
		r.failedRequests.Add(1)
		r.recordFailure(failureSample{Method: method, Path: path, Error: err.Error()})
		return false, elapsed
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, code := range want {
		if resp.StatusCode == code {
			return true, elapsed
		}
	}
	r.failedRequests.Add(1)
	r.recordFailure(failureSample{Method: method, Path: path, Status: resp.StatusCode, Body: trimFailureBody(raw)})
	return false, elapsed
}

func (r *runner) recordFailure(sample failureSample) {
	r.failuresMu.Lock()
	if len(r.failures) < 20 {
		r.failures = append(r.failures, sample)
	}
	r.failuresMu.Unlock()
}

func (r *runner) failureSamples() []failureSample {
	r.failuresMu.Lock()
	defer r.failuresMu.Unlock()
	return append([]failureSample(nil), r.failures...)
}

func trimFailureBody(raw []byte) string {
	const max = 512
	if len(raw) > max {
		raw = raw[:max]
	}
	return string(raw)
}

func signS3(req *http.Request, payload []byte, accessKey, secretKey string, now time.Time) {
	if accessKey == "" || secretKey == "" {
		return
	}
	payloadHash := sha256.Sum256(payload)
	payloadHex := hex.EncodeToString(payloadHash[:])
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := now.UTC().Format("20060102")

	req.Header.Set("x-amz-content-sha256", payloadHex)
	req.Header.Set("x-amz-date", amzDate)
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := req.URL.RawQuery
	canonicalHeaders := "host:" + req.Host + "\n" +
		"x-amz-content-sha256:" + payloadHex + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHex,
	}, "\n")
	credentialScope := dateStamp + "/" + awsRegion + "/" + awsService + "/aws4_request"
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	signingKey := deriveSigningKey(secretKey, dateStamp)
	signature := hmacSHA256Hex(signingKey, []byte(stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+"/"+credentialScope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func deriveSigningKey(secretKey, dateStamp string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(awsRegion))
	kService := hmacSHA256(kRegion, []byte(awsService))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func hmacSHA256Hex(key, data []byte) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// failureRateExceeded is exercised by main_test.go; golangci-lint's `unused`
// linter does not count test-only references.
//
//nolint:unused
func failureRateExceeded(total, failed uint64, threshold float64) bool {
	if failed == 0 {
		return false
	}
	if total == 0 {
		return true
	}
	return float64(failed)/float64(total) > threshold
}

func benchmarkFailed(summary reportSummary) bool {
	return summary.FailedRequests > 0
}

func writeReport(path string, r benchmarkReport) error {
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func printReport(r benchmarkReport, reportPath string) {
	fmt.Println()
	fmt.Println("=== GrainFS Iceberg Table API Benchmark Report ===")
	fmt.Printf("Timestamp: %s\n", r.Timestamp)
	fmt.Printf("Total Requests: %d\n", r.Summary.TotalRequests)
	fmt.Printf("Failed: %d\n\n", r.Summary.FailedRequests)
	for _, row := range []struct {
		name string
		op   operationSummary
	}{
		{"create_namespace", r.CreateNamespace},
		{"create_table", r.CreateTable},
		{"load_table", r.LoadTable},
		{"commit_table", r.CommitTable},
		{"delete_table", r.DeleteTable},
		{"delete_namespace", r.DeleteNamespace},
	} {
		if row.op.Ops == 0 {
			continue
		}
		fmt.Printf("%s: %d ops | P50: %sms | P99: %sms | Avg: %sms\n", row.name, row.op.Ops, row.op.P50MS, row.op.P99MS, row.op.AvgMS)
	}
	fmt.Printf("\nFull report: %s\n", reportPath)
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}
	r := newRunner(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration+30*time.Second)
	defer cancel()
	if err := r.setupWarehouseBucket(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "setup warehouse bucket: %v\n", err)
		os.Exit(1)
	}
	report := r.run(ctx)
	if err := writeReport(cfg.reportPath, report); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		os.Exit(1)
	}
	printReport(report, cfg.reportPath)
	if benchmarkFailed(report.Summary) {
		os.Exit(1)
	}
}
