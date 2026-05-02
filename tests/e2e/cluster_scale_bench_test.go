package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scaleBenchResult 는 한 번의 N 측정 결과.
type scaleBenchResult struct {
	n         int
	bootSec   int
	perProc   procMetrics // 5 proc 평균
	elections int         // 30s idle 동안 election count (현재 미수집, 0 placeholder)
}

// runScaleBench 는 5 process를 boot, --seed-groups=N 로 시드, 30s idle 측정 후 종료.
func runScaleBench(t *testing.T, n int) scaleBenchResult {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-SCALE-BENCH-KEY"
		accessKey  = "scale-ak"
		secretKey  = "scale-sk"
		numNodes   = 5
	)

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	nfs4Ports := make([]int, numNodes)
	nbdPorts := make([]int, numNodes)
	pprofPorts := make([]int, numNodes)
	ports := uniqueFreePorts(numNodes * 5)
	for i := range httpPorts {
		httpPorts[i] = ports[i*5]
		raftPorts[i] = ports[i*5+1]
		nfs4Ports[i] = ports[i*5+2]
		nbdPorts[i] = ports[i*5+3]
		pprofPorts[i] = ports[i*5+4]
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

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-bench-N%d-%d-*", n, i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		stderrFile, err := os.Create(fmt.Sprintf("/tmp/scale-n%d-node-%d-stderr.log", n, i))
		require.NoError(t, err, "create stderr file for node %d", i)
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", raftAddr(i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			fmt.Sprintf("--seed-groups=%d", n),
			fmt.Sprintf("--pprof-port=%d", pprofPorts[i]),
			"--nfs4-port", fmt.Sprintf("%d", nfs4Ports[i]),
			"--nbd-port", fmt.Sprintf("%d", nbdPorts[i]),
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		cmd.Stdout = stderrFile
		cmd.Stderr = stderrFile
		require.NoError(t, cmd.Start(), "start node %d", i)
		t.Cleanup(func() {
			stderrFile.Close()
			if t.Failed() {
				t.Logf("N=%d node %d stderr saved to %s", n, i, stderrFile.Name())
			} else {
				os.Remove(stderrFile.Name())
			}
		})
		return cmd
	}

	bootStart := time.Now()
	procs := make([]*exec.Cmd, numNodes)
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})

	for i := 0; i < numNodes; i++ {
		procs[i] = startNode(i)
		time.Sleep(150 * time.Millisecond)
	}
	waitForPortsParallel(t, httpPorts, 180*time.Second)
	waitForPortsParallel(t, pprofPorts, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	endpoints := make([]string, numNodes)
	for i := range endpoints {
		endpoints[i] = httpURL(i)
	}
	leaderIdx, err := waitForWritableEndpoint(
		ctx,
		endpoints,
		240*time.Second,
		5*time.Second,
		time.Second,
		func(attemptCtx context.Context, endpoint string) error {
			c := ecS3Client(endpoint, accessKey, secretKey)
			return tryCreateBucket(attemptCtx, c, fmt.Sprintf("scale-n%d", n))
		},
	)
	require.NoError(t, err, "no leader found")
	t.Logf("N=%d leader node %d at %s", n, leaderIdx, endpoints[leaderIdx])

	// seed loop가 끝날 때까지 대기 (N × ~400ms 추정 + 여유 5s, 최대 60s 클램프)
	settleTime := time.Duration(n)*400*time.Millisecond + 5*time.Second
	if settleTime > 60*time.Second {
		settleTime = 60 * time.Second
	}
	time.Sleep(settleTime)

	bootSec := int(time.Since(bootStart).Seconds())
	t.Logf("N=%d boot complete in %ds, sampling for 30s", n, bootSec)

	pids := make([]int, numNodes)
	pprofURLs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		pids[i] = procs[i].Process.Pid
		pprofURLs[i] = pprofURL(i)
	}

	samples := sampleAll(t, pids, pprofURLs, 30*time.Second, 5*time.Second)
	avgs := make([]procMetrics, numNodes)
	for i := 0; i < numNodes; i++ {
		avgs[i] = avgMetrics(samples[i])
	}
	clusterAvg := avgMetrics(avgs)

	return scaleBenchResult{
		n:         n,
		bootSec:   bootSec,
		perProc:   clusterAvg,
		elections: 0, // future: parse logs or expose /metrics
	}
}

// TestE2E_ClusterScaleBench_N8 is an opt-in scale measurement.
// It boots five processes and samples pprof for 30s, so keep it out of the
// default e2e suite where correctness tests should remain deterministic.
func TestE2E_ClusterScaleBench_N8(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale bench in -short mode")
	}
	if os.Getenv("GRAINFS_BENCH_FULL") != "1" {
		t.Skip("set GRAINFS_BENCH_FULL=1 to run scale bench")
	}
	r := runScaleBench(t, 8)
	t.Logf("N=8 result: boot=%ds RSS=%.1fMB heap=%.1fMB CPU=%.1f%% gor=%d",
		r.bootSec, r.perProc.rssMB, r.perProc.heapMB, r.perProc.cpuPct, r.perProc.goroutines)
}

// 각 N을 별도 Test 함수로 분리한다 — t.Run으로 묶으면 sub-test 사이의 cleanup이
// 즉시 풀리지 않고 macOS에서 5 process × N groups boot이 누적되어 flake.
// shell loop로 각 N을 별도 `go test` 프로세스에서 실행해 fresh state 보장.
//
// 권장 실행:
//   GRAINFS_BENCH_FULL=1 for N in 8 32 64 128; do
//     go test -count=1 -timeout 600s -v \
//       -run "^TestE2E_ClusterScaleBench_N${N}$" ./tests/e2e/ |\
//       tee -a /tmp/grainfs-bench/sweep.log
//     sleep 5
//   done

func runScaleBenchTest(t *testing.T, n int) {
	if testing.Short() {
		t.Skip("skipping scale bench in -short mode")
	}
	if os.Getenv("GRAINFS_BENCH_FULL") != "1" && n > 8 {
		t.Skip("set GRAINFS_BENCH_FULL=1 to run N>8 scale bench")
	}
	r := runScaleBench(t, n)
	t.Logf("N=%d result: boot=%ds RSS=%.1fMB heap=%.1fMB CPU=%.1f%% gor=%d",
		r.n, r.bootSec, r.perProc.rssMB, r.perProc.heapMB, r.perProc.cpuPct, r.perProc.goroutines)
	t.Logf("BENCH_ROW: %s", formatRow(r.n, r.perProc, r.bootSec, r.elections))
}

func TestE2E_ClusterScaleBench_N32(t *testing.T)  { runScaleBenchTest(t, 32) }
func TestE2E_ClusterScaleBench_N64(t *testing.T)  { runScaleBenchTest(t, 64) }
func TestE2E_ClusterScaleBench_N128(t *testing.T) { runScaleBenchTest(t, 128) }
