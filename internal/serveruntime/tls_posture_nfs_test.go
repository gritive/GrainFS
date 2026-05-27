package serveruntime

import "testing"

func TestNFSBootPostureNoLongerDependsOnAnonymousConfig(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	state := &bootState{
		cfg:      Config{DataDir: t.TempDir()},
		cfgStore: newTestCfgStore(t),
	}
	if _, ok := state.cfgStore.GetBool("iam.anon-enabled"); ok {
		t.Fatal("iam.anon-enabled should not be registered")
	}
	if err := bootNodeServicesPostureGate(state); err != nil {
		t.Fatalf("NFS/9P posture gate should not reject based on removed anon config: %v", err)
	}
}
