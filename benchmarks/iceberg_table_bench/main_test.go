package main

import (
	"encoding/json"
	"testing"
)

func TestOperationStatsSummary(t *testing.T) {
	s := newOperationStats()
	s.add(30)
	s.add(10)
	s.add(20)

	got := s.summary()
	if got.Ops != 3 {
		t.Fatalf("Ops = %d, want 3", got.Ops)
	}
	if got.P50MS != "20.00" {
		t.Fatalf("P50MS = %q, want 20.00", got.P50MS)
	}
	if got.P99MS != "30.00" {
		t.Fatalf("P99MS = %q, want 30.00", got.P99MS)
	}
	if got.AvgMS != "20.00" || got.MinMS != "10.00" || got.MaxMS != "30.00" {
		t.Fatalf("summary = %+v", got)
	}
}

func TestBenchmarkReportJSONShape(t *testing.T) {
	r := benchmarkReport{
		Timestamp:       "2026-05-16T00:00:00Z",
		Summary:         reportSummary{TotalRequests: 7, FailedRequests: 1},
		CreateNamespace: operationSummary{Ops: 1, P50MS: "1.00", P99MS: "1.00", AvgMS: "1.00", MinMS: "1.00", MaxMS: "1.00"},
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["create_namespace"]; !ok {
		t.Fatalf("report missing create_namespace: %s", raw)
	}
	if _, ok := decoded["summary"]; !ok {
		t.Fatalf("report missing summary: %s", raw)
	}
}

func TestFailureRateExceeded(t *testing.T) {
	if failureRateExceeded(100, 5, 0.05) {
		t.Fatal("5% must not exceed 5% threshold")
	}
	if !failureRateExceeded(100, 6, 0.05) {
		t.Fatal("6% must exceed 5% threshold")
	}
	if !failureRateExceeded(0, 1, 0.05) {
		t.Fatal("failures with no total requests must exceed threshold")
	}
}
