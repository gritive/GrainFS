package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// cluster_perf_profile_test.go — 5노드 클러스터 idle/load CPU·메모리 프로파일링.
//
// 게이트: GRAINFS_PERF=1
// 산출물 dir 기본: /tmp/grainfs-perf-<unix-ts>/  (GRAINFS_PERF_DIR로 override)
//
// 4개 시나리오:
//   idle-N8   — N=8  raft groups, 워크로드 없음, 30s 측정
//   idle-N64  — N=64 raft groups, 워크로드 없음, 30s 측정
//   load-N8   — N=8,  S3 PUT/GET 워크로드 60s, 그중 30s CPU 프로파일
//   load-N64  — N=64, S3 PUT/GET 워크로드 60s, 그중 30s CPU 프로파일
//
// 노드별 산출물(시나리오 디렉터리 내):
//   node-{0..4}-cpu.out / -heap.out / -mutex.out / -allocs.out / -goroutine.out
//   summary.md  (시나리오 단일 요약)
//
// 최종 산출물:
//   cluster-profile.md  (4시나리오 비교 + 노드별 평균 + go tool pprof -top 힌트)

type perfScenario struct {
	name     string // ex: "load-N64"
	n        int    // seed-groups
	withLoad bool   // S3 PUT/GET 워크로드 동작
}

type perfResult struct {
	scenario   perfScenario
	bootSec    int
	cluster    procMetrics // 5노드 평균
	perNode    []procMetrics
	workload   workloadResult
	outDir     string
	cpuTopText string // `go tool pprof -top` (node-0) 결과
}

type workloadResult struct {
	puts        uint64
	gets        uint64
	putErrs     uint64
	getErrs     uint64
	bytesPut    uint64
	bytesGet    uint64
	durationSec float64
}

const (
	perfNumNodes         = 5
	perfClusterKey       = "E2E-PERF-KEY"
	perfAccessKey        = "perf-ak"
	perfSecretKey        = "perf-sk"
	perfMeasureSec       = 30 // 30s 메트릭 샘플 + 동시 30s CPU 프로파일
	perfWorkloadTotalSec = 60 // 워크로드 총 지속 (CPU 30s는 그 안의 중간)
	perfWorkloadConc     = 32
	perfObjectSize       = 4 * 1024 * 1024 // 4 MiB
	perfPrewarmObjects   = 16
)

// TestE2E_ClusterPerf_All 은 4개 시나리오를 차례로 실행하고 통합 리포트를 생성한다.
//
// t.Run으로 묶지 않는 이유: cluster_scale_bench_test.go 노트와 동일 — sub-test
// 사이 cleanup이 즉시 풀리지 않아 5 procs × N raft groups 부팅이 누적되며 flake.
// 한 함수 안에서 명시적 cleanup loop으로 시나리오를 직렬화한다.
func TestE2E_ClusterPerf_All(t *testing.T) {
	if testing.Short() {
		t.Skip("perf profile suite skipped in -short mode")
	}
	if os.Getenv("GRAINFS_PERF") != "1" {
		t.Skip("set GRAINFS_PERF=1 to run cluster perf profiling")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	outRoot := os.Getenv("GRAINFS_PERF_DIR")
	if outRoot == "" {
		outRoot = filepath.Join(os.TempDir(), fmt.Sprintf("grainfs-perf-%d", time.Now().Unix()))
	}
	require.NoError(t, os.MkdirAll(outRoot, 0o755))
	t.Logf("perf output dir: %s", outRoot)

	scenarios := []perfScenario{
		{name: "idle-N8", n: 8, withLoad: false},
		{name: "idle-N64", n: 64, withLoad: false},
		{name: "load-N8", n: 8, withLoad: true},
		{name: "load-N64", n: 64, withLoad: true},
	}
	// GRAINFS_PERF_SCENARIO=idle-N8,load-N8 형태로 필터.
	if filter := os.Getenv("GRAINFS_PERF_SCENARIO"); filter != "" {
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
		scenarios = filtered
		if len(scenarios) == 0 {
			t.Fatalf("GRAINFS_PERF_SCENARIO=%q matched no scenarios", filter)
		}
	}

	results := make([]*perfResult, 0, len(scenarios))
	for _, sc := range scenarios {
		t.Logf("===== %s (n=%d, withLoad=%v) =====", sc.name, sc.n, sc.withLoad)
		r := runPerfScenario(t, sc, outRoot)
		results = append(results, r)
		t.Logf("[%s] boot=%ds RSS=%.1fMB heap=%.1fMB CPU=%.1f%% gor=%d puts=%d gets=%d",
			sc.name, r.bootSec, r.cluster.rssMB, r.cluster.heapMB, r.cluster.cpuPct, r.cluster.goroutines,
			r.workload.puts, r.workload.gets)
	}

	require.NoError(t, writePerfReport(outRoot, results))
	t.Logf("final report: %s/cluster-profile.md", outRoot)
}

// runPerfScenario 는 한 시나리오를 처음부터 끝까지 실행하고 cleanup까지 수행.
func runPerfScenario(t *testing.T, sc perfScenario, outRoot string) *perfResult {
	t.Helper()
	binary := getBinary()
	scenarioDir := filepath.Join(outRoot, sc.name)
	require.NoError(t, os.MkdirAll(scenarioDir, 0o755))

	httpPorts := make([]int, perfNumNodes)
	raftPorts := make([]int, perfNumNodes)
	pprofPorts := make([]int, perfNumNodes)
	for i := range httpPorts {
		httpPorts[i] = freePort()
		raftPorts[i] = freePort()
		pprofPorts[i] = freePort()
	}
	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }
	pprofURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", pprofPorts[i]) }
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j == i {
				continue
			}
			out = append(out, raftAddr(j))
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, perfNumNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-perf-%s-%d-*", sc.name, i))
		require.NoError(t, err)
		dataDirs[i] = d
	}

	procs := make([]*exec.Cmd, perfNumNodes)
	cleanup := func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
		for _, d := range dataDirs {
			_ = os.RemoveAll(d)
		}
	}
	defer cleanup()

	bootStart := time.Now()
	for i := 0; i < perfNumNodes; i++ {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("perf-%s-%d", sc.name, i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", perfClusterKey,
			"--access-key", perfAccessKey,
			"--secret-key", perfSecretKey,
			fmt.Sprintf("--seed-groups=%d", sc.n),
			fmt.Sprintf("--pprof-port=%d", pprofPorts[i]),
			"--nfs4-port", fmt.Sprintf("%d", freePort()),
			"--nbd-port", fmt.Sprintf("%d", freePort()),
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		// 노드별 stdout/stderr를 scenarioDir에 캡처해 leader-election 실패 등 boot 단계
		// 진단을 가능하게 한다. 파일은 cleanup()에서 procs와 함께 회수.
		nodeLog, err := os.Create(filepath.Join(scenarioDir, fmt.Sprintf("node-%d.log", i)))
		require.NoError(t, err, "create node-%d log", i)
		cmd.Stdout = nodeLog
		cmd.Stderr = nodeLog
		require.NoError(t, cmd.Start(), "start node %d", i)
		procs[i] = cmd
	}
	waitForPortsParallel(t, httpPorts, 180*time.Second)
	waitForPortsParallel(t, pprofPorts, 30*time.Second)

	// 리더 발견 + bucket 준비
	bucket := fmt.Sprintf("perf-%s", strings.ToLower(strings.ReplaceAll(sc.name, "_", "-")))
	leaderIdx := waitForLeaderAndCreateBucket(t, httpURL, bucket)

	// seed loop가 끝날 때까지 대기 (scale_bench 와 동일 공식)
	settleTime := time.Duration(sc.n)*400*time.Millisecond + 5*time.Second
	if settleTime > 60*time.Second {
		settleTime = 60 * time.Second
	}
	time.Sleep(settleTime)
	bootSec := int(time.Since(bootStart).Seconds())
	t.Logf("[%s] boot complete in %ds (leader=node-%d)", sc.name, bootSec, leaderIdx)

	pids := make([]int, perfNumNodes)
	pprofURLs := make([]string, perfNumNodes)
	for i := 0; i < perfNumNodes; i++ {
		pids[i] = procs[i].Process.Pid
		pprofURLs[i] = pprofURL(i)
	}

	// 워크로드 (load 시나리오만): 측정 시작 전부터 prewarm + 백그라운드 PUT/GET 시작
	var wlResult workloadResult
	var wlStop chan struct{}
	var wlDone chan struct{}
	if sc.withLoad {
		wlStop, wlDone = startWorkload(t, httpURL(leaderIdx), bucket, &wlResult)
	}

	// 측정 윈도우: 30s 메트릭 샘플링 + 30s CPU 프로파일 + 끝나면 heap/mutex/allocs/goroutine
	measureStart := time.Now()
	var samples [][]procMetrics
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		samples = sampleAll(t, pids, pprofURLs, perfMeasureSec*time.Second, 5*time.Second)
	}()

	// CPU 프로파일은 5노드 동시 캡처 (각각 30s blocking)
	cpuPaths := make([]string, perfNumNodes)
	for i := range cpuPaths {
		cpuPaths[i] = filepath.Join(scenarioDir, fmt.Sprintf("node-%d-cpu.out", i))
	}
	cpuErrs := dumpCPUProfilesConcurrent(pprofURLs, cpuPaths, perfMeasureSec)
	for i, e := range cpuErrs {
		if e != nil {
			t.Logf("[%s] node-%d CPU profile failed: %v", sc.name, i, e)
		}
	}

	<-sampleDone

	// 측정 이후 추가 프로파일 (CPU 캡처 동안 생긴 데이터 포함)
	for i := 0; i < perfNumNodes; i++ {
		base := pprofURLs[i]
		nd := filepath.Join(scenarioDir, fmt.Sprintf("node-%d-", i))
		for _, p := range []struct{ url, file string }{
			{base + "/debug/pprof/heap", nd + "heap.out"},
			{base + "/debug/pprof/mutex", nd + "mutex.out"},
			{base + "/debug/pprof/allocs", nd + "allocs.out"},
			{base + "/debug/pprof/goroutine", nd + "goroutine.out"},
		} {
			if err := fetchProfile(p.url, p.file); err != nil {
				t.Logf("[%s] %s: %v", sc.name, p.file, err)
			}
		}
	}

	// 워크로드 정지: spec상 perfWorkloadTotalSec(60s)까지 계속 돌리고 멈춘다.
	// CPU 프로파일 30s는 워크로드의 앞 절반에서 캡처되었고, 후반 30s는
	// 정상 상태(prewarm 영향 없는) 메트릭/프로파일을 heap/mutex 등에 반영하는 용도.
	if sc.withLoad {
		remaining := perfWorkloadTotalSec*time.Second - time.Since(measureStart)
		if remaining > 0 {
			t.Logf("[%s] workload extending %s to reach %ds total", sc.name, remaining, perfWorkloadTotalSec)
			time.Sleep(remaining)
		}
		close(wlStop)
		<-wlDone
		wlResult.durationSec = time.Since(measureStart).Seconds()

		// silent-zero 가드: 워크로드가 사실상 비어있으면 시나리오를 erroneous로 마킹
		if wlResult.puts < 5 {
			t.Errorf("[%s] workload degenerate: puts=%d gets=%d putErrs=%d getErrs=%d — load comparison is unreliable",
				sc.name, wlResult.puts, wlResult.gets, wlResult.putErrs, wlResult.getErrs)
		} else if wlResult.putErrs > wlResult.puts/5 {
			t.Logf("[%s] WARN high PUT error rate: puts=%d errs=%d", sc.name, wlResult.puts, wlResult.putErrs)
		}
	}

	avgs := make([]procMetrics, perfNumNodes)
	for i := 0; i < perfNumNodes; i++ {
		avgs[i] = avgMetrics(samples[i])
	}
	clusterAvg := avgMetrics(avgs)

	r := &perfResult{
		scenario: sc,
		bootSec:  bootSec,
		cluster:  clusterAvg,
		perNode:  avgs,
		workload: wlResult,
		outDir:   scenarioDir,
	}

	// node-0 CPU 프로파일에 대한 `go tool pprof -top` 캡처 (요약용)
	if top, err := runGoToolPprofTop(cpuPaths[0]); err == nil {
		r.cpuTopText = top
	} else {
		t.Logf("[%s] go tool pprof -top node-0-cpu.out: %v", sc.name, err)
	}

	require.NoError(t, writeScenarioSummary(r))
	return r
}

// waitForLeaderAndCreateBucket 는 노드를 round-robin으로 시도하며 bucket 생성에
// 성공한 노드 인덱스를 리더(또는 라우팅 가능한 노드)로 본다.
func waitForLeaderAndCreateBucket(t *testing.T, httpURL func(int) string, bucket string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var leader = -1
	require.Eventually(t, func() bool {
		for i := 0; i < perfNumNodes; i++ {
			c := ecS3Client(httpURL(i), perfAccessKey, perfSecretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			if err == nil {
				leader = i
				return true
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "no node accepted CreateBucket")
	return leader
}

// startWorkload 는 지정된 endpoint에 perfWorkloadConc 개 goroutine으로 PUT70/GET30
// 혼합 부하를 가한다. wlStop을 close하면 정지하고 wlDone을 close한다.
func startWorkload(t *testing.T, endpoint, bucket string, out *workloadResult) (chan struct{}, chan struct{}) {
	t.Helper()
	c := ecS3Client(endpoint, perfAccessKey, perfSecretKey)

	// Prewarm: GET 대상 객체 16개 업로드
	prewarmKeys := make([]string, perfPrewarmObjects)
	prewarmBuf := make([]byte, perfObjectSize)
	if _, err := rand.Read(prewarmBuf); err != nil {
		t.Fatalf("rand prewarm: %v", err)
	}
	for i := 0; i < perfPrewarmObjects; i++ {
		key := fmt.Sprintf("warm/%04d", i)
		prewarmKeys[i] = key
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(prewarmBuf),
		})
		cancel()
		if err != nil {
			t.Fatalf("prewarm PUT %s: %v", key, err)
		}
	}
	t.Logf("prewarm: %d objects of %dKB seeded", perfPrewarmObjects, perfObjectSize/1024)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for g := 0; g < perfWorkloadConc; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				buf := make([]byte, perfObjectSize)
				if _, err := rand.Read(buf); err != nil {
					return
				}
				putKeyN := uint64(0)
				for {
					select {
					case <-stop:
						return
					default:
					}
					// 70% PUT / 30% GET (전역 카운터 modulo 10 — 다소 stochastic하지만 장기 비율은 7:3)
					op := (atomic.LoadUint64(&out.puts) + atomic.LoadUint64(&out.gets)) % 10
					if op < 7 {
						key := fmt.Sprintf("put/g%02d/%08d", gid, putKeyN)
						putKeyN++
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						_, err := c.PutObject(ctx, &s3.PutObjectInput{
							Bucket: aws.String(bucket),
							Key:    aws.String(key),
							Body:   bytes.NewReader(buf),
						})
						cancel()
						if err != nil {
							atomic.AddUint64(&out.putErrs, 1)
						} else {
							atomic.AddUint64(&out.puts, 1)
							atomic.AddUint64(&out.bytesPut, uint64(len(buf)))
						}
					} else {
						key := prewarmKeys[int(putKeyN)%perfPrewarmObjects]
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						getOut, err := c.GetObject(ctx, &s3.GetObjectInput{
							Bucket: aws.String(bucket),
							Key:    aws.String(key),
						})
						if err != nil {
							atomic.AddUint64(&out.getErrs, 1)
							cancel()
							continue
						}
						n, _ := io.Copy(io.Discard, getOut.Body)
						_ = getOut.Body.Close()
						cancel()
						atomic.AddUint64(&out.gets, 1)
						atomic.AddUint64(&out.bytesGet, uint64(n))
					}
				}
			}(g)
		}
		wg.Wait()
	}()
	return stop, done
}

// dumpCPUProfilesConcurrent 는 5노드 CPU 프로파일을 동시 캡처. 길이 = perfNumNodes.
func dumpCPUProfilesConcurrent(pprofURLs, outPaths []string, seconds int) []error {
	errs := make([]error, len(pprofURLs))
	var wg sync.WaitGroup
	for i := range pprofURLs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			url := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", pprofURLs[idx], seconds)
			errs[idx] = fetchProfile(url, outPaths[idx])
		}(i)
	}
	wg.Wait()
	return errs
}

// runGoToolPprofTop 은 `go tool pprof -top -nodecount 20 <file>` 결과를 텍스트로 반환.
// go가 PATH에 없거나 파일이 없으면 빈 문자열 + nil(에러는 caller가 처리).
func runGoToolPprofTop(profilePath string) (string, error) {
	if _, err := os.Stat(profilePath); err != nil {
		return "", err
	}
	cmd := exec.Command("go", "tool", "pprof", "-top", "-nodecount", "20", profilePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("go tool pprof: %w", err)
	}
	return string(out), nil
}

func writeScenarioSummary(r *perfResult) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", r.scenario.name)
	fmt.Fprintf(&b, "- N (raft groups): %d\n", r.scenario.n)
	fmt.Fprintf(&b, "- with workload: %v\n", r.scenario.withLoad)
	fmt.Fprintf(&b, "- boot: %ds\n", r.bootSec)
	fmt.Fprintf(&b, "- cluster avg: RSS=%.1fMB heap=%.1fMB CPU=%.1f%% goroutines=%d\n\n",
		r.cluster.rssMB, r.cluster.heapMB, r.cluster.cpuPct, r.cluster.goroutines)
	fmt.Fprintf(&b, "## Per-node\n\n")
	fmt.Fprintf(&b, "| node | RSS (MB) | heap (MB) | CPU%% | gor |\n")
	fmt.Fprintf(&b, "|------|---------:|----------:|-----:|----:|\n")
	for i, m := range r.perNode {
		fmt.Fprintf(&b, "| %d | %.1f | %.1f | %.1f | %d |\n", i, m.rssMB, m.heapMB, m.cpuPct, m.goroutines)
	}
	if r.scenario.withLoad {
		fmt.Fprintf(&b, "\n## Workload\n\n")
		fmt.Fprintf(&b, "- PUT: ok=%d err=%d bytes=%d\n", r.workload.puts, r.workload.putErrs, r.workload.bytesPut)
		fmt.Fprintf(&b, "- GET: ok=%d err=%d bytes=%d\n", r.workload.gets, r.workload.getErrs, r.workload.bytesGet)
		if r.workload.durationSec > 0 {
			tputMBps := float64(r.workload.bytesPut+r.workload.bytesGet) / r.workload.durationSec / 1024 / 1024
			fmt.Fprintf(&b, "- duration: %.1fs total throughput: %.1f MB/s\n", r.workload.durationSec, tputMBps)
		}
	}
	if r.cpuTopText != "" {
		fmt.Fprintf(&b, "\n## node-0 CPU top (go tool pprof -top -nodecount 20)\n\n```\n%s\n```\n", r.cpuTopText)
	}
	return os.WriteFile(filepath.Join(r.outDir, "summary.md"), []byte(b.String()), 0o644)
}

func writePerfReport(outRoot string, results []*perfResult) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Cluster Profile — %s\n\n", filepath.Base(outRoot))
	fmt.Fprintf(&b, "5-node grainfs cluster, idle vs load × N=8 vs N=64.\n\n")
	fmt.Fprintf(&b, "Notes:\n")
	fmt.Fprintf(&b, "- `heap (MB)` is `runtime.MemStats.HeapAlloc` — Go가 OS에서 가져온 heap 영역에 할당한 총량. badger SKL/ristretto 등 사전 할당 arena가 포함되어 macOS에서는 working set(RSS)보다 클 수 있음.\n")
	fmt.Fprintf(&b, "- `RSS (MB)` is `ps`-reported resident set size — 실제 물리 메모리에 매핑된 양.\n")
	fmt.Fprintf(&b, "- Load 시나리오의 워크로드는 `httpURL(leaderIdx)` 단일 노드에 집중 — leader 핫스팟 위주, 분산 부하가 아님을 감안.\n\n")
	fmt.Fprintf(&b, "## Summary matrix (cluster avg = mean of 5 nodes)\n\n")
	fmt.Fprintf(&b, "| scenario | N | load | boot | RSS (MB) | heap (MB) | CPU%% | gor | PUTs | GETs | tput MB/s |\n")
	fmt.Fprintf(&b, "|---------|---:|:----:|----:|---------:|----------:|-----:|----:|-----:|-----:|----------:|\n")
	for _, r := range results {
		tput := 0.0
		if r.workload.durationSec > 0 {
			tput = float64(r.workload.bytesPut+r.workload.bytesGet) / r.workload.durationSec / 1024 / 1024
		}
		fmt.Fprintf(&b, "| %s | %d | %v | %ds | %.1f | %.1f | %.1f | %d | %d | %d | %.1f |\n",
			r.scenario.name, r.scenario.n, r.scenario.withLoad, r.bootSec,
			r.cluster.rssMB, r.cluster.heapMB, r.cluster.cpuPct, r.cluster.goroutines,
			r.workload.puts, r.workload.gets, tput)
	}
	fmt.Fprintf(&b, "\n## node-0 CPU hotspots (per scenario)\n\n")
	for _, r := range results {
		fmt.Fprintf(&b, "### %s\n\n", r.scenario.name)
		if r.cpuTopText == "" {
			fmt.Fprintf(&b, "_no CPU profile captured_\n\n")
			continue
		}
		fmt.Fprintf(&b, "```\n%s\n```\n\n", r.cpuTopText)
	}
	fmt.Fprintf(&b, "## Files\n\n")
	for _, r := range results {
		fmt.Fprintf(&b, "- `%s/` — node-{0..4}-{cpu,heap,mutex,allocs,goroutine}.out + summary.md\n", r.scenario.name)
	}
	fmt.Fprintf(&b, "\n## Inspect\n\n")
	fmt.Fprintf(&b, "```\ngo tool pprof -top -nodecount 30 %s/<scenario>/node-0-cpu.out\n", outRoot)
	fmt.Fprintf(&b, "go tool pprof -web %s/<scenario>/node-0-heap.out\n", outRoot)
	fmt.Fprintf(&b, "go tool pprof -top %s/<scenario>/node-0-mutex.out   # lock contention\n```\n", outRoot)

	return os.WriteFile(filepath.Join(outRoot, "cluster-profile.md"), []byte(b.String()), 0o644)
}
