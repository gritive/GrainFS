package scrubber

import (
	"context"
	"testing"
)

// 본 microbenchmark는 Director actor refactor가 admin-path 메서드에
// 추가하는 channel hop 비용을 절대값으로 측정한다.
// 비교 기준: refactor 이전 mu Lock/Unlock은 ~20-50ns였다.
// 비교 후 channel + goroutine hop은 ~μs scale 예상.
//
// 실행: go test -bench=BenchmarkDirector -benchmem -run=^$ ./internal/scrubber/
//
// 영향 평가: scrub admin 경로는 *초당 0.x회* 수준의 호출 빈도이므로,
// 마이크로초 단위 추가 latency는 운영자 시각으로 무시 가능.

func benchSetup(b *testing.B) *Director {
	b.Helper()
	d := NewDirector(DirectorOpts{Incident: &recordingIncident{}, QueueSize: 256})
	d.Register("ec", &countingSource{name: "ec"}, noopVerifier{})
	d.Start(context.Background())
	b.Cleanup(d.Stop)
	return d
}

// BenchmarkDirector_Trigger_Dedup: 두 번째 이후 호출이 dedup으로 short-circuit.
// dedup map lookup만 거치므로 controller round-trip의 순수 비용.
func BenchmarkDirector_Trigger_Dedup(b *testing.B) {
	d := benchSetup(b)
	req := TriggerReq{Bucket: "b1", KeyPrefix: "p", Scope: ScopeFull}
	// 1회 trigger로 dedup entry 등록 — 이후 모든 호출은 dedup hit.
	d.Trigger(req)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.Trigger(req)
	}
}

// BenchmarkDirector_LookupDedup: cluster propose 경로 short-circuit.
// 가장 빈번할 가능성이 있는 admin-side 경로.
func BenchmarkDirector_LookupDedup(b *testing.B) {
	d := benchSetup(b)
	d.Trigger(TriggerReq{Bucket: "b1", KeyPrefix: "p", Scope: ScopeFull})
	req := TriggerReq{Bucket: "b1", KeyPrefix: "p", Scope: ScopeFull}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.LookupDedup(req)
	}
}

// BenchmarkDirector_ApplyFromFSM_NonBlocking: raft FSM apply 경로.
// non-blocking inbox send + drop-on-full. 호출자 비용만 측정 (controller가
// 별도 goroutine에서 처리하므로 round-trip 없음).
func BenchmarkDirector_ApplyFromFSM_NonBlocking(b *testing.B) {
	d := benchSetup(b)
	entry := ScrubTriggerEntry{
		SessionID: "sess",
		Bucket:    "b1",
		KeyPrefix: "p",
		Scope:     ScopeFull,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.ApplyFromFSM(entry)
	}
}

// BenchmarkDirector_GetSession: admin 단일 조회 — controller round-trip.
func BenchmarkDirector_GetSession(b *testing.B) {
	d := benchSetup(b)
	id, _ := d.Trigger(TriggerReq{Bucket: "b1", KeyPrefix: "p", Scope: ScopeFull})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.GetSession(id)
	}
}
