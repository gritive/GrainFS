package admin

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/execution"
	"github.com/gritive/GrainFS/internal/serveruntime/executioncluster"
)

func BenchmarkTriggerScrubLegacyProposer(b *testing.B) {
	p := &mockScrubProposer{entry: scrubber.ScrubTriggerEntry{SessionID: "sid-1"}, created: true}
	deps := &Deps{ScrubProposer: p}
	req := ScrubReq{Bucket: "ec1"}
	for i := 0; i < b.N; i++ {
		if _, err := TriggerScrub(context.Background(), deps, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTriggerScrubExecutionSeam(b *testing.B) {
	exec := &mockExecutionExecutor{result: execution.Result{Scrub: execution.ScrubResult{SessionID: "sid-1", Created: true}}}
	deps := &Deps{Execution: exec}
	req := ScrubReq{Bucket: "ec1"}
	for i := 0; i < b.N; i++ {
		if _, err := TriggerScrub(context.Background(), deps, req); err != nil {
			b.Fatal(err)
		}
	}
}

type benchScrubBackend struct{}

func (benchScrubBackend) TriggerScrub(context.Context, execution.Operation) (execution.ScrubResult, error) {
	return execution.ScrubResult{SessionID: "sid-1", Created: true}, nil
}

func BenchmarkTriggerScrubExecutionActor(b *testing.B) {
	exec := executioncluster.NewExecutor(benchScrubBackend{})
	b.Cleanup(func() {
		if err := exec.Close(context.Background()); err != nil {
			b.Fatalf("close executor: %v", err)
		}
	})
	deps := &Deps{Execution: exec}
	req := ScrubReq{Bucket: "ec1"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := TriggerScrub(context.Background(), deps, req); err != nil {
			b.Fatal(err)
		}
	}
}
