package cluster

import "context"

// NewTestDistributedBackend exposes newTestDistributedBackend to external
// test packages (package cluster_test) without making it part of the
// production API. Only compiled when running tests.
func NewTestDistributedBackend(t clusterTestTB) *DistributedBackend {
	return newTestDistributedBackend(t)
}

// UpgradeObjectECForTest resolves the object's current placement and drives the
// unexported upgradeObjectEC re-encode path to newCfg. It exists so external
// (package cluster_test) tests can exercise the reshard upgrade against an
// object produced by the real streaming pipeline — putpipeline imports cluster,
// so the pipeline-PUT harness can only live in cluster_test, while
// upgradeObjectEC is unexported. This one-line bridge is the idiomatic Go seam
// between the two.
func (b *DistributedBackend) UpgradeObjectECForTest(ctx context.Context, bucket, key string, newCfg ECConfig) error {
	_, meta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return err
	}
	resolved, err := b.ResolvePlacement(ctx, bucket, key, meta)
	if err != nil {
		return err
	}
	return b.upgradeObjectEC(ctx, bucket, key, resolved.Record, newCfg)
}

// Root returns the backend's root directory (test helper).
func (b *DistributedBackend) Root() string { return b.root }

// SelfAddr returns the backend's self address (test helper).
func (b *DistributedBackend) SelfAddr() string { return b.selfAddr }
