package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
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
		Failures:        []failureSample{{Method: http.MethodGet, Path: "/iceberg/v1/config", Status: http.StatusInternalServerError}},
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
	if _, ok := decoded["failures"]; !ok {
		t.Fatalf("report missing failures: %s", raw)
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

func TestBenchmarkFailedRequiresZeroFailedRequests(t *testing.T) {
	if !benchmarkFailed(reportSummary{TotalRequests: 100, FailedRequests: 1}) {
		t.Fatal("benchmark must fail when any request fails")
	}
	if benchmarkFailed(reportSummary{TotalRequests: 100, FailedRequests: 0}) {
		t.Fatal("benchmark must pass when all requests pass")
	}
}

func TestSignS3AddsRequiredHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/grainfs-tables", nil)
	if err != nil {
		t.Fatal(err)
	}
	signS3(req, []byte{}, "AKIA_TEST", "SECRET_TEST", time.Date(2026, 5, 16, 1, 2, 3, 0, time.UTC))
	if req.Header.Get("Authorization") == "" {
		t.Fatal("missing Authorization")
	}
	if req.Header.Get("x-amz-content-sha256") == "" {
		t.Fatal("missing x-amz-content-sha256")
	}
	if req.Header.Get("x-amz-date") != "20260516T010203Z" {
		t.Fatalf("x-amz-date = %q", req.Header.Get("x-amz-date"))
	}
}
