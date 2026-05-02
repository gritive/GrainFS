package e2e

import (
	"bufio"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// procMetrics 는 한 번의 샘플링에서 수집된 process-level metrics다.
type procMetrics struct {
	pid        int
	rssMB      float64 // resident set size (MB)
	cpuPct     float64 // ps -o pcpu (%)
	heapMB     float64 // runtime.MemStats.HeapAlloc from /debug/pprof/heap?debug=1 (MB)
	goroutines int     // /debug/pprof/goroutine count
}

// fetchPprofHeap 는 /debug/pprof/heap?debug=1 의 텍스트 모드 헤더에서
// HeapAlloc 값을 추출해 MB로 반환한다. HeapAlloc은 RSS가 아니라 Go live
// heap 계정값이며, pprof의 inuse_space top과도 같은 표본은 아니다.
//
// 헤더 예시: "# HeapAlloc = 12345678"
func fetchPprofHeap(pprofURL string) (float64, error) {
	resp, err := http.Get(pprofURL + "/debug/pprof/heap?debug=1")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "# HeapAlloc = ") {
			parts := strings.SplitN(line, "= ", 2)
			if len(parts) != 2 {
				continue
			}
			n, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return 0, err
			}
			return float64(n) / (1024 * 1024), nil
		}
	}
	return 0, fmt.Errorf("HeapAlloc not found in heap profile")
}

// fetchPprofGoroutines 는 goroutine count를 반환한다.
//
// 첫 줄: "goroutine profile: total 123"
func fetchPprofGoroutines(pprofURL string) (int, error) {
	resp, err := http.Get(pprofURL + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	if !scanner.Scan() {
		return 0, fmt.Errorf("empty goroutine profile")
	}
	first := scanner.Text()
	parts := strings.Fields(first)
	if len(parts) < 4 {
		return 0, fmt.Errorf("unexpected first line: %q", first)
	}
	return strconv.Atoi(parts[3])
}

// psSample 은 ps로 RSS와 CPU%를 수집한다. macOS와 Linux 둘 다 지원.
//
// `ps -o rss=,pcpu= -p <pid>` 출력: "  12345  3.2"
func psSample(pid int) (rssMB, cpuPct float64, err error) {
	cmd := exec.Command("ps", "-o", "rss=,pcpu=", "-p", strconv.Itoa(pid))
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(out))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected ps output: %q", out)
	}
	rssKB, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, 0, err
	}
	cpu, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, 0, err
	}
	return rssKB / 1024, cpu, nil
}

// sampleProc 은 한 번의 샘플링.
func sampleProc(pid int, pprofURL string) (procMetrics, error) {
	m := procMetrics{pid: pid}
	rss, cpu, err := psSample(pid)
	if err != nil {
		return m, fmt.Errorf("ps: %w", err)
	}
	m.rssMB = rss
	m.cpuPct = cpu
	heap, err := fetchPprofHeap(pprofURL)
	if err != nil {
		return m, fmt.Errorf("heap: %w", err)
	}
	m.heapMB = heap
	gor, err := fetchPprofGoroutines(pprofURL)
	if err != nil {
		return m, fmt.Errorf("goroutines: %w", err)
	}
	m.goroutines = gor
	return m, nil
}

// avgMetrics 는 여러 샘플을 평균낸다.
func avgMetrics(samples []procMetrics) procMetrics {
	if len(samples) == 0 {
		return procMetrics{}
	}
	var avg procMetrics
	avg.pid = samples[0].pid
	for _, s := range samples {
		avg.rssMB += s.rssMB
		avg.cpuPct += s.cpuPct
		avg.heapMB += s.heapMB
		avg.goroutines += s.goroutines
	}
	n := float64(len(samples))
	avg.rssMB /= n
	avg.cpuPct /= n
	avg.heapMB /= n
	avg.goroutines /= len(samples)
	return avg
}

// sampleAll 은 duration 동안 interval 마다 모든 process를 샘플링.
func sampleAll(t testing.TB, pids []int, pprofURLs []string, duration, interval time.Duration) [][]procMetrics {
	t.Helper()
	if len(pids) != len(pprofURLs) {
		t.Fatalf("pids/pprofURLs length mismatch: %d vs %d", len(pids), len(pprofURLs))
	}
	samples := make([][]procMetrics, len(pids))
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		for i, pid := range pids {
			m, err := sampleProc(pid, pprofURLs[i])
			if err != nil {
				t.Logf("sample proc %d failed: %v", pid, err)
				continue
			}
			samples[i] = append(samples[i], m)
		}
		time.Sleep(interval)
	}
	return samples
}

// formatRow 는 마크다운 표 한 줄을 만든다.
func formatRow(n int, perProc procMetrics, bootSec, elections int) string {
	return fmt.Sprintf("| %d | %d s | %.1f MB | %.1f MB | %.1f%% | %d | %d |",
		n, bootSec, perProc.rssMB, perProc.heapMB, perProc.cpuPct, perProc.goroutines, elections)
}
