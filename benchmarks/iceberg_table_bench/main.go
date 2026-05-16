package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type operationStats struct {
	mu       sync.Mutex
	samples  []float64
	requests atomic.Uint64
}

func newOperationStats() *operationStats {
	return &operationStats{}
}

func (s *operationStats) add(ms float64) {
	s.requests.Add(1)
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

type benchmarkReport struct {
	Timestamp       string           `json:"timestamp"`
	Summary         reportSummary    `json:"summary"`
	CreateNamespace operationSummary `json:"create_namespace"`
	CreateTable     operationSummary `json:"create_table"`
	LoadTable       operationSummary `json:"load_table"`
	CommitTable     operationSummary `json:"commit_table"`
	DeleteTable     operationSummary `json:"delete_table"`
	DeleteNamespace operationSummary `json:"delete_namespace"`
}

func failureRateExceeded(total, failed uint64, threshold float64) bool {
	if failed == 0 {
		return false
	}
	if total == 0 {
		return true
	}
	return float64(failed)/float64(total) > threshold
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
	_ = time.Now
}
