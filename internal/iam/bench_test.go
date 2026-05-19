package iam

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// BenchmarkApplySACreate (SC#9) measures FSM apply latency for the most
// common admin command (SACreate) against a 1k-SA pre-seeded store.
// Budget: p99 < 5ms. Enforced as a bench-time assertion so a regression
// lands as a test failure.
func BenchmarkApplySACreate(b *testing.B) {
	s := NewStore()
	enc := newTestEncryptor(b)
	ap := NewApplier(s, enc)

	// Pre-seed 1k SAs to make the apply path realistic (map insert at scale).
	for i := 0; i < 1000; i++ {
		sa := ServiceAccount{
			ID:        fmt.Sprintf("seed-%d", i),
			Name:      fmt.Sprintf("seed-%d", i),
			CreatedAt: time.Now(),
		}
		if err := ap.ApplySACreate(buildSACreatePayload(sa)); err != nil {
			b.Fatal(err)
		}
	}

	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sa := ServiceAccount{
			ID:        fmt.Sprintf("bench-%d", i),
			Name:      fmt.Sprintf("bench-%d", i),
			CreatedAt: time.Now(),
		}
		payload := buildSACreatePayload(sa)
		start := time.Now()
		if err := ap.ApplySACreate(payload); err != nil {
			b.Fatal(err)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()

	p50 := percentile(samples, 50)
	p99 := percentile(samples, 99)
	b.ReportMetric(float64(p50.Microseconds()), "p50_us/op")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us/op")
	if p99 > 5*time.Millisecond {
		b.Fatalf("ApplySACreate p99 = %v, want < 5ms (SC#9 budget)", p99)
	}
}

// BenchmarkResolveSA (SC#10) measures the IAM hot path (ResolveSA) at
// 10k-SA scale. Budget: p99 < 1ms. Enforced as a bench-time assertion.
func BenchmarkResolveSA(b *testing.B) {
	s := NewStore()
	enc := newTestEncryptor(b)
	ap := NewApplier(s, enc)

	const N = 10000
	aks := make([]string, N)
	for i := 0; i < N; i++ {
		sa := ServiceAccount{
			ID:        fmt.Sprintf("sa-%d", i),
			Name:      fmt.Sprintf("sa-%d", i),
			CreatedAt: time.Now(),
		}
		if err := ap.ApplySACreate(buildSACreatePayload(sa)); err != nil {
			b.Fatal(err)
		}
		ak := fmt.Sprintf("AKGFBENCH%016d", i)
		aks[i] = ak
		plaintext := fmt.Sprintf("secret-%d-very-long-padding-padding-padding", i)
		wrapped, err := WrapSecret(enc, sa.ID, plaintext)
		if err != nil {
			b.Fatal(err)
		}
		k := AccessKey{
			AccessKey:    ak,
			SecretKey:    plaintext,
			SecretKeyEnc: wrapped,
			SAID:         sa.ID,
			Status:       KeyStatusActive,
			CreatedAt:    time.Now(),
		}
		if err := ap.ApplyKeyCreate(buildKeyCreatePayload(k)); err != nil {
			b.Fatal(err)
		}
	}

	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ak := aks[i%N]
		start := time.Now()
		_, _, ok := ResolveSA(s, ak)
		if !ok {
			b.Fatalf("ResolveSA failed for %s", ak)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()

	p50 := percentile(samples, 50)
	p99 := percentile(samples, 99)
	b.ReportMetric(float64(p50.Nanoseconds()), "p50_ns/op")
	b.ReportMetric(float64(p99.Nanoseconds()), "p99_ns/op")
	if p99 > time.Millisecond {
		b.Fatalf("ResolveSA p99 = %v, want < 1ms (SC#10 budget)", p99)
	}
}

// percentile returns the value at the p-th percentile of samples.
// Sorts a copy so the caller's slice order is preserved.
func percentile(samples []time.Duration, p int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := (len(sorted) * p) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
