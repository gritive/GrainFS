package putpipeline

import (
	"context"
	"testing"
	"time"
)

func TestIdleTimeoutContext_StallCancels(t *testing.T) {
	ctx, _, stop := idleTimeoutContext(context.Background(), 50*time.Millisecond)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle ctx must cancel on stall")
	}
}

func TestIdleTimeoutContext_ProgressKeepsAlive(t *testing.T) {
	ctx, reset, stop := idleTimeoutContext(context.Background(), 50*time.Millisecond)
	defer stop()
	for i := 0; i < 6; i++ {
		time.Sleep(25 * time.Millisecond)
		reset()
	}
	select {
	case <-ctx.Done():
		t.Fatal("progress must keep ctx alive")
	default:
	}
}

func TestIdleTimeoutContext_StopCancels(t *testing.T) {
	ctx, _, stop := idleTimeoutContext(context.Background(), time.Hour)
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop must cancel")
	}
}

// TestIdleTimeoutContext_ResetAfterFireIsSafe pins the monotonic-cancellation
// property: a reset() that arrives AFTER the idle timer fired and after stop()
// must not panic and must not revive a Done ctx.
func TestIdleTimeoutContext_ResetAfterFireIsSafe(t *testing.T) {
	ctx, reset, stop := idleTimeoutContext(context.Background(), 20*time.Millisecond)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("must fire on stall")
	}
	stop()
	reset()
	reset()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("ctx must stay Done after a late reset")
	}
}
