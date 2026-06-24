package serveruntime

import (
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// newTestCfgStore returns a config.Store with the cluster keys registered.
func newTestCfgStore(t *testing.T) *config.Store {
	t.Helper()
	s := config.NewStore()
	config.RegisterClusterKeys(s, config.ReloadHooks{})
	return s
}

func TestTLSPostureNoLongerDependsOnAnonymousConfig(t *testing.T) {
	cfg := newTestCfgStore(t)
	nc := nodeconfig.New(t.TempDir())

	if _, ok := cfg.GetBool("iam.anon-enabled"); ok {
		t.Fatal("iam.anon-enabled should not be registered")
	}
	if err := enforceTLSPosture(cfg, nc); err != nil {
		t.Fatalf("posture gate should not reject based on removed anon config: %v", err)
	}
}
