package admin

import (
	"context"
	"errors"
	"fmt"
)

// cascadeBucketConfigAfterDelete clears per-bucket config that lives outside the
// data-Raft bucket keyspace — the lifecycle configuration and the IAM
// bucket-upstream record — so a recreated same-name bucket starts clean. Both
// meta-Raft deletes are idempotent (no-op on a missing key), so this is safe to
// call on both the successful-delete and already-absent-bucket paths, and safe
// to retry.
//
// CALLER CONTRACT: invoke only once the bucket itself is known to be gone
// (delete succeeded, or the bucket was already absent). Never call it on a
// failed delete of a surviving bucket (e.g. ErrBucketNotEmpty) — that would
// strip a live bucket's config. observedLifecycleGen/observedUpstreamGen are the
// CAS tokens the caller MUST capture BEFORE the data-Raft delete; the FSM apply
// only deletes when the stored generation still equals the observed one, so a
// racing recreate (which bumps the generation past the captured value) makes
// this cascade a no-op and keeps the recreated bucket's fresh config.
func cascadeBucketConfigAfterDelete(ctx context.Context, d *Deps, name string, observedLifecycleGen, observedUpstreamGen uint64) error {
	var errs []error
	if d.LifecycleDeleteProp != nil {
		if err := d.LifecycleDeleteProp.ProposeLifecycleDelete(ctx, name, observedLifecycleGen); err != nil {
			errs = append(errs, fmt.Errorf("delete lifecycle config: %w", err))
		}
	}
	if d.BucketUpstreamDeleteProp != nil {
		if err := d.BucketUpstreamDeleteProp.ProposeBucketUpstreamDelete(ctx, name, observedUpstreamGen); err != nil {
			errs = append(errs, fmt.Errorf("delete IAM bucket-upstream: %w", err))
		}
	}
	if len(errs) > 0 {
		return NewInternal("cascade delete bucket config after bucket delete: " + errors.Join(errs...).Error())
	}
	return nil
}
