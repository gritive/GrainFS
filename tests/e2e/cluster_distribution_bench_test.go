package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// cluster_distribution_bench_test.go measures whether load escapes a single
// ingress node and whether any node remains a bottleneck under distributed
// shard/group ownership. It is opt-in because it starts five grainfs processes
// and captures pprof data from every node.

type distributionIngress string

const (
	distributionIngressSingle     distributionIngress = "single"
	distributionIngressRoundRobin distributionIngress = "round-robin"
)

type distributionMix string

const (
	distributionMixWriteHeavy distributionMix = "write-heavy"
	distributionMixReadHeavy  distributionMix = "read-heavy"
)

type distributionScenario struct {
	name    string
	n       int
	ingress distributionIngress
	mix     distributionMix
}

type distributionResult struct {
	scenario        distributionScenario
	bootSec         int
	cluster         procMetrics
	perNode         []procMetrics
	workload        distributionWorkloadResult
	httpRequests    []float64
	outDir          string
	nodeCPUTop      []string
	hotMutexTopText string
	hotBlockTopText string
}

type distributionWorkloadResult struct {
	puts            uint64
	gets            uint64
	putErrs         uint64
	getErrs         uint64
	bytesPut        uint64
	bytesGet        uint64
	durationSec     float64
	ingressOps      []uint64
	putLatencyP99MS float64
	getLatencyP99MS float64
}

type distributionLatencySink struct {
	mu   sync.Mutex
	puts []float64
	gets []float64
}

const (
	distributionNodes          = 5
	distributionAccessKey      = "dist-ak"
	distributionSecretKey      = "dist-sk"
	distributionClusterKey     = "E2E-DISTRIBUTION-KEY"
	distributionPrewarmObjects = 32
)

func TestE2E_ClusterDistributionBench(t *testing.T) {
	if testing.Short() {
		t.Skip("distribution bench skipped in -short mode")
	}
	if os.Getenv("GRAINFS_DISTRIBUTION_BENCH") != "1" {
		t.Skip("set GRAINFS_DISTRIBUTION_BENCH=1 to run cluster distribution benchmark")
	}
	if _, err := os.Stat(getBinary()); err != nil {
		t.Skipf("grainfs binary not found at %s - run `make build` first", getBinary())
	}

	outRoot := os.Getenv("GRAINFS_DISTRIBUTION_DIR")
	if outRoot == "" {
		outRoot = filepath.Join(os.TempDir(), fmt.Sprintf("grainfs-distribution-%d", time.Now().Unix()))
	}
	require.NoError(t, os.MkdirAll(outRoot, 0o755))
	t.Logf("distribution output dir: %s", outRoot)

	scenarios := []distributionScenario{
		{name: "single-write-N8", n: 8, ingress: distributionIngressSingle, mix: distributionMixWriteHeavy},
		{name: "single-read-N8", n: 8, ingress: distributionIngressSingle, mix: distributionMixReadHeavy},
		{name: "rr-write-N8", n: 8, ingress: distributionIngressRoundRobin, mix: distributionMixWriteHeavy},
		{name: "rr-read-N8", n: 8, ingress: distributionIngressRoundRobin, mix: distributionMixReadHeavy},
		{name: "single-write-N16", n: 16, ingress: distributionIngressSingle, mix: distributionMixWriteHeavy},
		{name: "single-read-N16", n: 16, ingress: distributionIngressSingle, mix: distributionMixReadHeavy},
		{name: "rr-write-N16", n: 16, ingress: distributionIngressRoundRobin, mix: distributionMixWriteHeavy},
		{name: "rr-read-N16", n: 16, ingress: distributionIngressRoundRobin, mix: distributionMixReadHeavy},
		{name: "single-write-N32", n: 32, ingress: distributionIngressSingle, mix: distributionMixWriteHeavy},
		{name: "single-read-N32", n: 32, ingress: distributionIngressSingle, mix: distributionMixReadHeavy},
		{name: "rr-write-N32", n: 32, ingress: distributionIngressRoundRobin, mix: distributionMixWriteHeavy},
		{name: "rr-read-N32", n: 32, ingress: distributionIngressRoundRobin, mix: distributionMixReadHeavy},
	}
	scenarios = filterDistributionScenarios(t, scenarios)

	results := make([]*distributionResult, 0, len(scenarios))
	for _, sc := range scenarios {
		t.Logf("===== %s (n=%d ingress=%s mix=%s) =====", sc.name, sc.n, sc.ingress, sc.mix)
		r := runDistributionScenario(t, sc, outRoot)
		results = append(results, r)
		t.Logf("[%s] boot=%ds CPU max/median=%.2f puts=%d/%d gets=%d/%d p99_put=%.1fms p99_get=%.1fms",
			sc.name,
			r.bootSec,
			maxMedianRatio(procField(r.perNode, func(m procMetrics) float64 { return m.cpuPct })),
			r.workload.puts,
			r.workload.putErrs,
			r.workload.gets,
			r.workload.getErrs,
			r.workload.putLatencyP99MS,
			r.workload.getLatencyP99MS)
	}
	require.NoError(t, writeDistributionReport(outRoot, results))
	t.Logf("final report: %s", filepath.Join(outRoot, "cluster-distribution.md"))
}

func filterDistributionScenarios(t *testing.T, scenarios []distributionScenario) []distributionScenario {
	t.Helper()
	filter := strings.TrimSpace(os.Getenv("GRAINFS_DISTRIBUTION_SCENARIO"))
	if filter == "" {
		return scenarios
	}
	want := map[string]bool{}
	for _, name := range strings.Split(filter, ",") {
		want[strings.TrimSpace(name)] = true
	}
	filtered := scenarios[:0]
	for _, sc := range scenarios {
		if want[sc.name] {
			filtered = append(filtered, sc)
		}
	}
	require.NotEmpty(t, filtered, "GRAINFS_DISTRIBUTION_SCENARIO=%q matched no scenarios", filter)
	return filtered
}

func runDistributionScenario(t *testing.T, sc distributionScenario, outRoot string) *distributionResult {
	t.Helper()
	scenarioDir := filepath.Join(outRoot, sc.name)
	require.NoError(t, os.MkdirAll(scenarioDir, 0o755))

	bootStart := time.Now()
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:       distributionNodes,
		SeedGroups:  sc.n,
		Mode:        ClusterModeStaticPeers,
		ClusterKey:  distributionClusterKey,
		AccessKey:   distributionAccessKey,
		SecretKey:   distributionSecretKey,
		ECData:      2,
		ECParity:    1,
		LogPrefix:   "grainfs-distribution",
		DisableNFS:  true,
		DisableNBD:  true,
		EnablePprof: true,
		ExtraArgs: []string{
			"--balancer-gossip-interval", "5s",
		},
	})
	defer c.Stop()

	bucket := fmt.Sprintf("dist-%s", strings.ToLower(strings.ReplaceAll(sc.name, "_", "-")))
	leaderIdx := ensureDistributionBucket(t, c, bucket)
	settleTime := time.Duration(sc.n)*400*time.Millisecond + 5*time.Second
	if settleTime > 60*time.Second {
		settleTime = 60 * time.Second
	}
	time.Sleep(settleTime)
	bootSec := int(time.Since(bootStart).Seconds())
	t.Logf("[%s] boot complete in %ds (writable endpoint=node-%d)", sc.name, bootSec, leaderIdx)

	pids := make([]int, distributionNodes)
	pprofURLs := make([]string, distributionNodes)
	for i := 0; i < distributionNodes; i++ {
		pids[i] = c.procs[i].Process.Pid
		pprofURLs[i] = fmt.Sprintf("http://127.0.0.1:%d", c.pprofPorts[i])
	}

	duration := envInt("GRAINFS_DISTRIBUTION_DURATION_SEC", 30)
	objectSize := envInt("GRAINFS_DISTRIBUTION_OBJECT_KB", 4096) * 1024
	conc := envInt("GRAINFS_DISTRIBUTION_CONCURRENCY", distributionDefaultConcurrency(sc.mix))

	wlStop, wlDone, wlResultPtr := startDistributionWorkload(t, c, sc, bucket, leaderIdx, conc, objectSize)
	measureStart := time.Now()

	var samples [][]procMetrics
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		samples = sampleAll(t, pids, pprofURLs, time.Duration(duration)*time.Second, 5*time.Second)
	}()

	cpuPaths := make([]string, distributionNodes)
	for i := range cpuPaths {
		cpuPaths[i] = filepath.Join(scenarioDir, fmt.Sprintf("node-%d-cpu.out", i))
	}
	cpuErrs := dumpCPUProfilesConcurrent(pprofURLs, cpuPaths, duration)
	for i, err := range cpuErrs {
		if err != nil {
			t.Logf("[%s] node-%d CPU profile failed: %v", sc.name, i, err)
		}
	}
	<-sampleDone

	profilePaths := dumpDistributionProfiles(t, scenarioDir, sc.name, pprofURLs)
	httpRequests := dumpDistributionMetrics(t, scenarioDir, sc.name, c.httpURLs)
	close(wlStop)
	<-wlDone
	wlResult := *wlResultPtr
	wlResult.durationSec = time.Since(measureStart).Seconds()

	avgs := make([]procMetrics, distributionNodes)
	for i := 0; i < distributionNodes; i++ {
		avgs[i] = avgMetrics(samples[i])
	}
	clusterAvg := avgMetrics(avgs)

	cpuTop := make([]string, distributionNodes)
	for i, p := range cpuPaths {
		if top, err := runGoToolPprofTop(p); err == nil {
			cpuTop[i] = top
		} else {
			t.Logf("[%s] node-%d CPU top failed: %v", sc.name, i, err)
		}
	}
	hotNode := hottestNode(avgs)
	mutexTop := pprofTopOrEmpty(t, profilePaths[hotNode]["mutex"])
	blockTop := pprofTopOrEmpty(t, profilePaths[hotNode]["block"])

	r := &distributionResult{
		scenario:        sc,
		bootSec:         bootSec,
		cluster:         clusterAvg,
		perNode:         avgs,
		workload:        wlResult,
		httpRequests:    httpRequests,
		outDir:          scenarioDir,
		nodeCPUTop:      cpuTop,
		hotMutexTopText: mutexTop,
		hotBlockTopText: blockTop,
	}
	require.NoError(t, writeDistributionScenarioSummary(r))

	if wlResult.puts+wlResult.gets < 10 {
		t.Errorf("[%s] workload degenerate: puts=%d gets=%d", sc.name, wlResult.puts, wlResult.gets)
	}
	totalOps := wlResult.puts + wlResult.gets
	totalErrs := wlResult.putErrs + wlResult.getErrs
	if totalOps > 0 && float64(totalErrs)/float64(totalOps+totalErrs) > 0.05 {
		t.Errorf("[%s] error rate too high: ok=%d errs=%d", sc.name, totalOps, totalErrs)
	}
	return r
}

func ensureDistributionBucket(t *testing.T, c *e2eCluster, bucket string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	leaderIdx, err := waitForWritableEndpoint(
		ctx,
		c.httpURLs,
		240*time.Second,
		5*time.Second,
		time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			return tryCreateBucket(attemptCtx, ecS3Client(endpoint, c.accessKey, c.secretKey), bucket)
		},
	)
	require.NoError(t, err, "no writable endpoint found")
	return leaderIdx
}

func startDistributionWorkload(
	t *testing.T,
	c *e2eCluster,
	sc distributionScenario,
	bucket string,
	leaderIdx int,
	concurrency int,
	objectSize int,
) (chan struct{}, chan struct{}, *distributionWorkloadResult) {
	t.Helper()
	clients := make([]*s3.Client, len(c.httpURLs))
	for i, url := range c.httpURLs {
		clients[i] = ecS3Client(url, c.accessKey, c.secretKey)
	}

	payload := bytes.Repeat([]byte("x"), objectSize)
	prewarmKeys := prewarmDistributionObjects(t, clients[leaderIdx], bucket, payload)
	result := &distributionWorkloadResult{ingressOps: make([]uint64, len(clients))}
	latencies := &distributionLatencySink{}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for worker := 0; worker < concurrency; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for iter := uint64(0); ; iter++ {
					select {
					case <-stop:
						return
					default:
					}
					nodeIdx := pickDistributionIngress(sc.ingress, leaderIdx, worker, iter, len(clients))
					atomic.AddUint64(&result.ingressOps[nodeIdx], 1)
					client := clients[nodeIdx]
					if distributionDoPut(sc.mix, iter) {
						key := fmt.Sprintf("put/%02d/%010d", worker, iter)
						start := time.Now()
						err := putDistributionObject(client, bucket, key, payload)
						latencies.addPut(time.Since(start).Seconds() * 1000)
						if err != nil {
							atomic.AddUint64(&result.putErrs, 1)
						} else {
							atomic.AddUint64(&result.puts, 1)
							atomic.AddUint64(&result.bytesPut, uint64(len(payload)))
						}
						continue
					}
					key := prewarmKeys[int(iter)%len(prewarmKeys)]
					start := time.Now()
					n, err := getDistributionObject(client, bucket, key)
					latencies.addGet(time.Since(start).Seconds() * 1000)
					if err != nil {
						atomic.AddUint64(&result.getErrs, 1)
					} else {
						atomic.AddUint64(&result.gets, 1)
						atomic.AddUint64(&result.bytesGet, uint64(n))
					}
				}
			}(worker)
		}
		wg.Wait()
		result.putLatencyP99MS = latencies.p99Put()
		result.getLatencyP99MS = latencies.p99Get()
	}()
	return stop, done, result
}

func prewarmDistributionObjects(t *testing.T, client *s3.Client, bucket string, payload []byte) []string {
	t.Helper()
	keys := make([]string, distributionPrewarmObjects)
	for i := range keys {
		keys[i] = fmt.Sprintf("warm/%04d", i)
		require.NoError(t, putDistributionObject(client, bucket, keys[i], payload), "prewarm PUT %s", keys[i])
	}
	return keys
}

func pickDistributionIngress(ingress distributionIngress, leaderIdx, worker int, iter uint64, nodes int) int {
	if ingress == distributionIngressRoundRobin {
		return int((uint64(worker) + iter) % uint64(nodes))
	}
	return leaderIdx
}

func distributionDoPut(mix distributionMix, iter uint64) bool {
	switch mix {
	case distributionMixReadHeavy:
		return iter%10 == 0
	default:
		return iter%10 != 0
	}
}

func distributionDefaultConcurrency(mix distributionMix) int {
	if mix == distributionMixReadHeavy {
		return 64
	}
	return 32
}

func putDistributionObject(client *s3.Client, bucket, key string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(payload),
	}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	})
	return err
}

func getDistributionObject(client *s3.Client, bucket, key string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()
	return io.Copy(io.Discard, out.Body)
}

func dumpDistributionProfiles(t *testing.T, scenarioDir, name string, pprofURLs []string) []map[string]string {
	t.Helper()
	out := make([]map[string]string, len(pprofURLs))
	for i, base := range pprofURLs {
		out[i] = map[string]string{}
		for _, profile := range []string{"heap", "allocs", "goroutine", "mutex", "block"} {
			path := filepath.Join(scenarioDir, fmt.Sprintf("node-%d-%s.out", i, profile))
			out[i][profile] = path
			if err := fetchProfile(base+"/debug/pprof/"+profile, path); err != nil {
				t.Logf("[%s] node-%d %s profile failed: %v", name, i, profile, err)
			}
		}
	}
	return out
}

func dumpDistributionMetrics(t *testing.T, scenarioDir, name string, httpURLs []string) []float64 {
	t.Helper()
	out := make([]float64, len(httpURLs))
	for i, base := range httpURLs {
		resp, err := http.Get(base + "/metrics") //nolint:noctx
		if err != nil {
			t.Logf("[%s] node-%d metrics scrape failed: %v", name, i, err)
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			t.Logf("[%s] node-%d metrics read failed: %v", name, i, readErr)
			continue
		}
		path := filepath.Join(scenarioDir, fmt.Sprintf("node-%d-metrics.prom", i))
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Logf("[%s] node-%d metrics write failed: %v", name, i, err)
		}
		out[i] = sumPromCounter(string(data), "grainfs_http_requests_total")
	}
	return out
}

func sumPromCounter(data, metric string) float64 {
	var sum float64
	for _, line := range strings.Split(data, "\n") {
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, metric) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err == nil {
			sum += v
		}
	}
	return sum
}

func pprofTopOrEmpty(t *testing.T, path string) string {
	t.Helper()
	top, err := runGoToolPprofTop(path)
	if err != nil {
		t.Logf("go tool pprof -top %s failed: %v", path, err)
		return ""
	}
	return top
}

func writeDistributionScenarioSummary(r *distributionResult) error {
	var b strings.Builder
	writeDistributionResult(&b, r, true)
	return os.WriteFile(filepath.Join(r.outDir, "summary.md"), []byte(b.String()), 0o644)
}

func writeDistributionReport(outRoot string, results []*distributionResult) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Cluster Distribution Benchmark\n\n")
	fmt.Fprintf(&b, "Output: `%s`\n\n", outRoot)
	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "| scenario | N | ingress | mix | boot | CPU max/median | RSS max/median | PUT ok/err | GET ok/err | PUT p99 ms | GET p99 ms | tput MB/s |\n")
	fmt.Fprintf(&b, "|---------|---:|---------|-----|-----:|---------------:|---------------:|-----------:|-----------:|-----------:|-----------:|----------:|\n")
	for _, r := range results {
		writeDistributionSummaryRow(&b, r)
	}
	for _, r := range results {
		fmt.Fprintf(&b, "\n---\n\n")
		writeDistributionResult(&b, r, false)
	}
	return os.WriteFile(filepath.Join(outRoot, "cluster-distribution.md"), []byte(b.String()), 0o644)
}

func writeDistributionSummaryRow(b *strings.Builder, r *distributionResult) {
	tput := 0.0
	if r.workload.durationSec > 0 {
		tput = float64(r.workload.bytesPut+r.workload.bytesGet) / r.workload.durationSec / 1024 / 1024
	}
	fmt.Fprintf(b, "| %s | %d | %s | %s | %ds | %.2f | %.2f | %d/%d | %d/%d | %.1f | %.1f | %.1f |\n",
		r.scenario.name,
		r.scenario.n,
		r.scenario.ingress,
		r.scenario.mix,
		r.bootSec,
		maxMedianRatio(procField(r.perNode, func(m procMetrics) float64 { return m.cpuPct })),
		maxMedianRatio(procField(r.perNode, func(m procMetrics) float64 { return m.rssMB })),
		r.workload.puts,
		r.workload.putErrs,
		r.workload.gets,
		r.workload.getErrs,
		r.workload.putLatencyP99MS,
		r.workload.getLatencyP99MS,
		tput)
}

func writeDistributionResult(b *strings.Builder, r *distributionResult, includeAllCPU bool) {
	fmt.Fprintf(b, "# %s\n\n", r.scenario.name)
	fmt.Fprintf(b, "- N: %d\n", r.scenario.n)
	fmt.Fprintf(b, "- ingress: %s\n", r.scenario.ingress)
	fmt.Fprintf(b, "- mix: %s\n", r.scenario.mix)
	fmt.Fprintf(b, "- boot: %ds\n", r.bootSec)
	fmt.Fprintf(b, "- cluster avg: RSS=%.1fMB heap=%.1fMB CPU=%.1f%% goroutines=%d\n", r.cluster.rssMB, r.cluster.heapMB, r.cluster.cpuPct, r.cluster.goroutines)
	fmt.Fprintf(b, "- CPU max/median: %.2f\n\n", maxMedianRatio(procField(r.perNode, func(m procMetrics) float64 { return m.cpuPct })))

	fmt.Fprintf(b, "## Per-node\n\n")
	fmt.Fprintf(b, "| node | ingress ops | http reqs | RSS MB | heap MB | CPU%% | goroutines |\n")
	fmt.Fprintf(b, "|-----:|------------:|----------:|-------:|--------:|-----:|-----------:|\n")
	for i, m := range r.perNode {
		ops := uint64(0)
		if i < len(r.workload.ingressOps) {
			ops = r.workload.ingressOps[i]
		}
		reqs := 0.0
		if i < len(r.httpRequests) {
			reqs = r.httpRequests[i]
		}
		fmt.Fprintf(b, "| %d | %d | %.0f | %.1f | %.1f | %.1f | %d |\n", i, ops, reqs, m.rssMB, m.heapMB, m.cpuPct, m.goroutines)
	}

	tput := 0.0
	if r.workload.durationSec > 0 {
		tput = float64(r.workload.bytesPut+r.workload.bytesGet) / r.workload.durationSec / 1024 / 1024
	}
	fmt.Fprintf(b, "\n## Workload\n\n")
	fmt.Fprintf(b, "- PUT: ok=%d err=%d p99=%.1fms bytes=%d\n", r.workload.puts, r.workload.putErrs, r.workload.putLatencyP99MS, r.workload.bytesPut)
	fmt.Fprintf(b, "- GET: ok=%d err=%d p99=%.1fms bytes=%d\n", r.workload.gets, r.workload.getErrs, r.workload.getLatencyP99MS, r.workload.bytesGet)
	fmt.Fprintf(b, "- duration: %.1fs throughput: %.1f MB/s\n\n", r.workload.durationSec, tput)

	if includeAllCPU {
		for i, top := range r.nodeCPUTop {
			if top == "" {
				continue
			}
			fmt.Fprintf(b, "## node-%d CPU top\n\n```\n%s\n```\n\n", i, top)
		}
	} else {
		hot := hottestNode(r.perNode)
		if hot < len(r.nodeCPUTop) && r.nodeCPUTop[hot] != "" {
			fmt.Fprintf(b, "## hottest-node CPU top (node-%d)\n\n```\n%s\n```\n\n", hot, r.nodeCPUTop[hot])
		}
	}
	if r.hotMutexTopText != "" {
		fmt.Fprintf(b, "## hottest-node mutex top\n\n```\n%s\n```\n\n", r.hotMutexTopText)
	}
	if r.hotBlockTopText != "" {
		fmt.Fprintf(b, "## hottest-node block top\n\n```\n%s\n```\n\n", r.hotBlockTopText)
	}
}

func procField(in []procMetrics, field func(procMetrics) float64) []float64 {
	out := make([]float64, 0, len(in))
	for _, m := range in {
		out = append(out, field(m))
	}
	return out
}

func maxMedianRatio(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	median := sorted[len(sorted)/2]
	if median <= 0 {
		return math.Inf(1)
	}
	return sorted[len(sorted)-1] / median
}

func hottestNode(values []procMetrics) int {
	idx := 0
	for i := 1; i < len(values); i++ {
		if values[i].cpuPct > values[idx].cpuPct {
			idx = i
		}
	}
	return idx
}

func envInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func (s *distributionLatencySink) addPut(ms float64) {
	s.mu.Lock()
	s.puts = append(s.puts, ms)
	s.mu.Unlock()
}

func (s *distributionLatencySink) addGet(ms float64) {
	s.mu.Lock()
	s.gets = append(s.gets, ms)
	s.mu.Unlock()
}

func (s *distributionLatencySink) p99Put() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return percentile(s.puts, 0.99)
}

func (s *distributionLatencySink) p99Get() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return percentile(s.gets, 0.99)
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
