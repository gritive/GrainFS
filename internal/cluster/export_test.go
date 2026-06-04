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

// PlanPlacementForTest resolves the per-shard node placement the multi-node
// streaming PUT path would compute for key, using the exact same unexported
// planObjectWritePlacement + ecObjectShardKey(key, "") inputs as
// PutObjectWithRequest's dispatch (object_put.go). External (package
// cluster_test) failure-injection tests use it to discover which shard index
// lands on which node at runtime — placement is deterministic per key but the
// loopback peer addresses are random per run, so the data-vs-parity and
// local-vs-remote mapping cannot be hardcoded. Returning the production plan
// (not a re-implemented PlaceShards) means the test cannot drift from the real
// HRW/weighting it targets.
func (b *DistributedBackend) PlanPlacementForTest(ctx context.Context, key string) ([]string, ECConfig, error) {
	plan, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation: "put_object",
		ShardKey:  ecObjectShardKey(key, ""),
	})
	if err != nil {
		return nil, ECConfig{}, err
	}
	return plan.NodeIDs, plan.Config, nil
}

// Root returns the backend's root directory (test helper).
func (b *DistributedBackend) Root() string { return b.root }

// SelfAddr returns the backend's self address (test helper).
func (b *DistributedBackend) SelfAddr() string { return b.selfAddr }
