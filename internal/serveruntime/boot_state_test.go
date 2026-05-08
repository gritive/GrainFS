package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBootState_AddCleanup_LIFO(t *testing.T) {
	s := newBootState(Config{})
	var order []string
	s.AddCleanup(func() { order = append(order, "A") })
	s.AddCleanup(func() { order = append(order, "B") })
	s.AddCleanup(func() { order = append(order, "C") })

	s.Cleanup()

	assert.Equal(t, []string{"C", "B", "A"}, order, "cleanup must run LIFO to match defer semantics")
}

func TestBootState_Cleanup_IdempotentAfterDrain(t *testing.T) {
	s := newBootState(Config{})
	calls := 0
	s.AddCleanup(func() { calls++ })

	s.Cleanup()
	s.Cleanup() // second drain must be a no-op

	assert.Equal(t, 1, calls, "Cleanup must not re-fire after the first drain")
}

func TestBootState_Cleanup_PanicInOneFn_OthersStillRun(t *testing.T) {
	s := newBootState(Config{})
	var order []string
	s.AddCleanup(func() { order = append(order, "A") })
	s.AddCleanup(func() { panic("boom from B") })
	s.AddCleanup(func() { order = append(order, "C") })

	assert.NotPanics(t, func() { s.Cleanup() }, "panic in a cleanup must not escape")
	assert.Equal(t, []string{"C", "A"}, order, "fns before and after the panicking one still run")
}

func TestBootState_AddCleanup_NilFn_Skipped(t *testing.T) {
	s := newBootState(Config{})
	calls := 0
	s.AddCleanup(func() { calls++ })
	s.AddCleanup(nil)
	s.AddCleanup(func() { calls++ })

	assert.NotPanics(t, func() { s.Cleanup() })
	assert.Equal(t, 2, calls)
}

func TestBootState_NilReceiver_CleanupSafe(t *testing.T) {
	var s *bootState
	assert.NotPanics(t, func() { s.Cleanup() }, "nil-receiver Cleanup is a defensive no-op for early-error paths")
}

func TestBootState_NewBootState_BindsConfig(t *testing.T) {
	cfg := Config{NodeID: "node-x", DataDir: "/tmp/x"}
	s := newBootState(cfg)
	assert.Equal(t, "node-x", s.cfg.NodeID)
	assert.Equal(t, "/tmp/x", s.cfg.DataDir)
	assert.Empty(t, s.cleanups)
}
