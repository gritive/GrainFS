// §5 T44: TLS posture helpers.
package serveruntime

import (
	"context"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// enforceTLSPosture is retained as a boot hook, but no longer gates startup on
// the removed iam.anon-enabled key. TLS requirements are now operator policy,
// not a side effect of first service-account bootstrap.
func enforceTLSPosture(cfg *config.Store, nc *nodeconfig.NodeConfig) error {
	return nil
}

// bootTLSPostureGate runs enforceTLSPosture as a boot phase. Wired in Run()
// AFTER bootHTTPServerAndAdmin (so state.cfgStore is populated and state.srv
// is constructed) and BEFORE bootDegradedAndServices (which goroutines
// srv.Run() — the listener actually starts there).
func bootTLSPostureGate(state *bootState) error {
	nc := nodeconfig.New(state.cfg.DataDir)
	return enforceTLSPosture(state.cfgStore, nc)
}

// iamPostureChecker preserves the AdminAPI dependency shape. SA creation no
// longer flips a global anonymous config key, so the check is a no-op.
type iamPostureChecker struct {
	cfg *config.Store
	nc  *nodeconfig.NodeConfig
}

func (p *iamPostureChecker) CheckAnonOff(_ context.Context) error {
	return nil
}

// newIAMPostureChecker builds the adapter wired into AdminAPI at boot.
func newIAMPostureChecker(cfg *config.Store, nc *nodeconfig.NodeConfig) *iamPostureChecker {
	return &iamPostureChecker{cfg: cfg, nc: nc}
}
