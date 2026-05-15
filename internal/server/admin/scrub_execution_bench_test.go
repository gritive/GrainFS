package admin

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/execution"
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
