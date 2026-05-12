package cluster

import (
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodeClusterConfigPatchCmd builds a MetaCmd-wrapped FlatBuffer carrying a
// MetaClusterConfigPatchCmd payload. Used by operator/admin endpoints to
// propose a cluster-config patch via Raft.
func EncodeClusterConfigPatchCmd(p ClusterConfigPatch) ([]byte, error) {
	b := clusterBuilderPool.Get()

	// Strings / byte vector must be built before the parent table Start.
	var webhookOff flatbuffers.UOffsetT
	if p.AlertWebhook != nil {
		webhookOff = b.CreateString(*p.AlertWebhook)
	}
	var wrappedOff flatbuffers.UOffsetT
	if p.AlertWebhookSecretWrapped != nil {
		wrappedOff = b.CreateByteVector(p.AlertWebhookSecretWrapped)
	}
	var resetKeysOff flatbuffers.UOffsetT
	if len(p.ResetKeys) > 0 {
		offs := make([]flatbuffers.UOffsetT, len(p.ResetKeys))
		for i, s := range p.ResetKeys {
			offs[i] = b.CreateString(s)
		}
		clusterpb.MetaClusterConfigPatchCmdStartResetKeysVector(b, len(offs))
		for i := len(offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offs[i])
		}
		resetKeysOff = b.EndVector(len(offs))
	}

	boxBool := func(v *bool) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.BoolBoxStart(b)
		clusterpb.BoolBoxAddV(b, *v)
		return clusterpb.BoolBoxEnd(b)
	}
	boxF64 := func(v *float64) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.DoubleBoxStart(b)
		clusterpb.DoubleBoxAddV(b, *v)
		return clusterpb.DoubleBoxEnd(b)
	}
	boxI32 := func(v *int32) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.Int32BoxStart(b)
		clusterpb.Int32BoxAddV(b, *v)
		return clusterpb.Int32BoxEnd(b)
	}
	boxI64 := func(v *int64) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.Int64BoxStart(b)
		clusterpb.Int64BoxAddV(b, *v)
		return clusterpb.Int64BoxEnd(b)
	}
	boxDur := func(v *time.Duration) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		n := int64(*v)
		return boxI64(&n)
	}

	enabledOff := boxBool(p.BalancerEnabled)
	trigOff := boxF64(p.BalancerImbalanceTriggerPct)
	stopOff := boxF64(p.BalancerImbalanceStopPct)
	mrOff := boxI32(p.BalancerMigrationRate)
	tenureOff := boxDur(p.BalancerLeaderTenureMin)
	warmupOff := boxDur(p.BalancerWarmupTimeout)
	cbOff := boxF64(p.BalancerCBThreshold)
	mrTriesOff := boxI32(p.BalancerMigrationMaxRetries)
	pendingOff := boxDur(p.BalancerMigrationPendingTTL)
	gossipOff := boxDur(p.BalancerGossipInterval)
	diskWarnOff := boxF64(p.DiskWarnFrac)
	diskCritOff := boxF64(p.DiskCriticalFrac)

	clusterpb.MetaClusterConfigPatchCmdStart(b)
	if enabledOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerEnabled(b, enabledOff)
	}
	if trigOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerImbalanceTriggerPct(b, trigOff)
	}
	if stopOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerImbalanceStopPct(b, stopOff)
	}
	if mrOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerMigrationRate(b, mrOff)
	}
	if tenureOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerLeaderTenureMinNs(b, tenureOff)
	}
	if warmupOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerWarmupTimeoutNs(b, warmupOff)
	}
	if cbOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerCbThreshold(b, cbOff)
	}
	if mrTriesOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerMigrationMaxRetries(b, mrTriesOff)
	}
	if pendingOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerMigrationPendingTtlNs(b, pendingOff)
	}
	if gossipOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddBalancerGossipIntervalNs(b, gossipOff)
	}
	if webhookOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddAlertWebhook(b, webhookOff)
	}
	if wrappedOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddAlertWebhookSecretWrapped(b, wrappedOff)
	}
	if diskWarnOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddDiskWarnFrac(b, diskWarnOff)
	}
	if diskCritOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddDiskCriticalFrac(b, diskCritOff)
	}
	if resetKeysOff != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddResetKeys(b, resetKeysOff)
	}
	if p.ExpectedRev != 0 {
		clusterpb.MetaClusterConfigPatchCmdAddExpectedRev(b, p.ExpectedRev)
	}
	inner := fbFinish(b, clusterpb.MetaClusterConfigPatchCmdEnd(b))

	return encodeMetaCmd(clusterpb.MetaCmdTypeClusterConfigPatch, inner)
}

// DecodeClusterConfigPatchCmd is the inverse of EncodeClusterConfigPatchCmd's
// inner payload (i.e. the MetaCmd.Data bytes). Returns the in-memory patch
// struct ready for ClusterConfig.applyPatch.
func DecodeClusterConfigPatchCmd(data []byte) (ClusterConfigPatch, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaClusterConfigPatchCmd {
		return clusterpb.GetRootAsMetaClusterConfigPatchCmd(d, 0)
	})
	if err != nil {
		return ClusterConfigPatch{}, fmt.Errorf("decode MetaClusterConfigPatchCmd: %w", err)
	}
	var p ClusterConfigPatch

	if b := t.BalancerEnabled(nil); b != nil {
		v := b.V()
		p.BalancerEnabled = &v
	}
	if b := t.BalancerImbalanceTriggerPct(nil); b != nil {
		v := b.V()
		p.BalancerImbalanceTriggerPct = &v
	}
	if b := t.BalancerImbalanceStopPct(nil); b != nil {
		v := b.V()
		p.BalancerImbalanceStopPct = &v
	}
	if b := t.BalancerMigrationRate(nil); b != nil {
		v := b.V()
		p.BalancerMigrationRate = &v
	}
	if b := t.BalancerLeaderTenureMinNs(nil); b != nil {
		d := time.Duration(b.V())
		p.BalancerLeaderTenureMin = &d
	}
	if b := t.BalancerWarmupTimeoutNs(nil); b != nil {
		d := time.Duration(b.V())
		p.BalancerWarmupTimeout = &d
	}
	if b := t.BalancerCbThreshold(nil); b != nil {
		v := b.V()
		p.BalancerCBThreshold = &v
	}
	if b := t.BalancerMigrationMaxRetries(nil); b != nil {
		v := b.V()
		p.BalancerMigrationMaxRetries = &v
	}
	if b := t.BalancerMigrationPendingTtlNs(nil); b != nil {
		d := time.Duration(b.V())
		p.BalancerMigrationPendingTTL = &d
	}
	if b := t.BalancerGossipIntervalNs(nil); b != nil {
		d := time.Duration(b.V())
		p.BalancerGossipInterval = &d
	}
	if wh := t.AlertWebhook(); wh != nil {
		s := string(wh)
		p.AlertWebhook = &s
	}
	if t.AlertWebhookSecretWrappedLength() > 0 {
		p.AlertWebhookSecretWrapped = t.AlertWebhookSecretWrappedBytes()
	}
	if b := t.DiskWarnFrac(nil); b != nil {
		v := b.V()
		p.DiskWarnFrac = &v
	}
	if b := t.DiskCriticalFrac(nil); b != nil {
		v := b.V()
		p.DiskCriticalFrac = &v
	}
	if n := t.ResetKeysLength(); n > 0 {
		p.ResetKeys = make([]string, n)
		for i := 0; i < n; i++ {
			p.ResetKeys[i] = string(t.ResetKeys(i))
		}
	}
	p.ExpectedRev = t.ExpectedRev()
	return p, nil
}

// serializeClusterConfig encodes a stand-alone FlatBuffers ClusterConfig table
// from c's current snapshot. Used by MetaFSM.Snapshot to embed the cluster
// config in the meta-state snapshot. Empty (zero-rev, no explicit fields)
// configs still produce a valid buffer — the table just has rev=0 and no
// optional boxes set, which round-trips back to the zero snap on Restore.
func serializeClusterConfig(c *ClusterConfig) []byte {
	s := c.snap.Load()
	b := clusterBuilderPool.Get()

	// All strings / byte vectors must be created before parent Start.
	var webhookOff flatbuffers.UOffsetT
	if s.alertWebhookHasExplicit {
		webhookOff = b.CreateString(s.alertWebhook)
	}
	var wrappedOff flatbuffers.UOffsetT
	if len(s.alertWebhookSecretWrapped) > 0 {
		wrappedOff = b.CreateByteVector(s.alertWebhookSecretWrapped)
	}

	boxBool := func(v *bool) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.BoolBoxStart(b)
		clusterpb.BoolBoxAddV(b, *v)
		return clusterpb.BoolBoxEnd(b)
	}
	boxF64 := func(v *float64) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.DoubleBoxStart(b)
		clusterpb.DoubleBoxAddV(b, *v)
		return clusterpb.DoubleBoxEnd(b)
	}
	boxI32 := func(v *int32) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.Int32BoxStart(b)
		clusterpb.Int32BoxAddV(b, *v)
		return clusterpb.Int32BoxEnd(b)
	}
	boxDur := func(v *time.Duration) flatbuffers.UOffsetT {
		if v == nil {
			return 0
		}
		clusterpb.Int64BoxStart(b)
		clusterpb.Int64BoxAddV(b, int64(*v))
		return clusterpb.Int64BoxEnd(b)
	}

	enabledOff := boxBool(s.balancerEnabled)
	trigOff := boxF64(s.balancerImbalanceTriggerPct)
	stopOff := boxF64(s.balancerImbalanceStopPct)
	mrOff := boxI32(s.balancerMigrationRate)
	tenureOff := boxDur(s.balancerLeaderTenureMin)
	warmupOff := boxDur(s.balancerWarmupTimeout)
	cbOff := boxF64(s.balancerCBThreshold)
	mrTriesOff := boxI32(s.balancerMigrationMaxRetries)
	pendingOff := boxDur(s.balancerMigrationPendingTTL)
	gossipOff := boxDur(s.balancerGossipInterval)
	diskWarnOff := boxF64(s.diskWarnFrac)
	diskCritOff := boxF64(s.diskCriticalFrac)

	clusterpb.ClusterConfigStart(b)
	clusterpb.ClusterConfigAddRev(b, s.rev)
	clusterpb.ClusterConfigAddUpdatedAtUnixMs(b, s.updatedAt.UnixMilli())
	if enabledOff != 0 {
		clusterpb.ClusterConfigAddBalancerEnabled(b, enabledOff)
	}
	if trigOff != 0 {
		clusterpb.ClusterConfigAddBalancerImbalanceTriggerPct(b, trigOff)
	}
	if stopOff != 0 {
		clusterpb.ClusterConfigAddBalancerImbalanceStopPct(b, stopOff)
	}
	if mrOff != 0 {
		clusterpb.ClusterConfigAddBalancerMigrationRate(b, mrOff)
	}
	if tenureOff != 0 {
		clusterpb.ClusterConfigAddBalancerLeaderTenureMinNs(b, tenureOff)
	}
	if warmupOff != 0 {
		clusterpb.ClusterConfigAddBalancerWarmupTimeoutNs(b, warmupOff)
	}
	if cbOff != 0 {
		clusterpb.ClusterConfigAddBalancerCbThreshold(b, cbOff)
	}
	if mrTriesOff != 0 {
		clusterpb.ClusterConfigAddBalancerMigrationMaxRetries(b, mrTriesOff)
	}
	if pendingOff != 0 {
		clusterpb.ClusterConfigAddBalancerMigrationPendingTtlNs(b, pendingOff)
	}
	if gossipOff != 0 {
		clusterpb.ClusterConfigAddBalancerGossipIntervalNs(b, gossipOff)
	}
	if webhookOff != 0 {
		clusterpb.ClusterConfigAddAlertWebhook(b, webhookOff)
	}
	if wrappedOff != 0 {
		clusterpb.ClusterConfigAddAlertWebhookSecretWrapped(b, wrappedOff)
	}
	if diskWarnOff != 0 {
		clusterpb.ClusterConfigAddDiskWarnFrac(b, diskWarnOff)
	}
	if diskCritOff != 0 {
		clusterpb.ClusterConfigAddDiskCriticalFrac(b, diskCritOff)
	}
	return fbFinish(b, clusterpb.ClusterConfigEnd(b))
}

// deserializeClusterConfig is the inverse of serializeClusterConfig: it parses
// a stand-alone ClusterConfig FlatBuffer back into a *clusterConfigSnap that
// the caller can hand to ClusterConfig.ReplaceSnap. Used by MetaFSM.Restore.
func deserializeClusterConfig(buf []byte) (*clusterConfigSnap, error) {
	t, err := fbSafe(buf, func(d []byte) *clusterpb.ClusterConfig {
		return clusterpb.GetRootAsClusterConfig(d, 0)
	})
	if err != nil {
		return nil, fmt.Errorf("decode ClusterConfig snapshot: %w", err)
	}
	snap := &clusterConfigSnap{
		rev:       t.Rev(),
		updatedAt: time.UnixMilli(t.UpdatedAtUnixMs()),
	}
	if b := t.BalancerEnabled(nil); b != nil {
		v := b.V()
		snap.balancerEnabled = &v
	}
	if b := t.BalancerImbalanceTriggerPct(nil); b != nil {
		v := b.V()
		snap.balancerImbalanceTriggerPct = &v
	}
	if b := t.BalancerImbalanceStopPct(nil); b != nil {
		v := b.V()
		snap.balancerImbalanceStopPct = &v
	}
	if b := t.BalancerMigrationRate(nil); b != nil {
		v := b.V()
		snap.balancerMigrationRate = &v
	}
	if b := t.BalancerLeaderTenureMinNs(nil); b != nil {
		d := time.Duration(b.V())
		snap.balancerLeaderTenureMin = &d
	}
	if b := t.BalancerWarmupTimeoutNs(nil); b != nil {
		d := time.Duration(b.V())
		snap.balancerWarmupTimeout = &d
	}
	if b := t.BalancerCbThreshold(nil); b != nil {
		v := b.V()
		snap.balancerCBThreshold = &v
	}
	if b := t.BalancerMigrationMaxRetries(nil); b != nil {
		v := b.V()
		snap.balancerMigrationMaxRetries = &v
	}
	if b := t.BalancerMigrationPendingTtlNs(nil); b != nil {
		d := time.Duration(b.V())
		snap.balancerMigrationPendingTTL = &d
	}
	if b := t.BalancerGossipIntervalNs(nil); b != nil {
		d := time.Duration(b.V())
		snap.balancerGossipInterval = &d
	}
	// AlertWebhook is a plain string field. FBS distinguishes "field absent"
	// (accessor returns nil) from "field present, value is empty string"
	// (accessor returns non-nil zero-length slice) — both round-trip
	// alertWebhookHasExplicit correctly because the serializer only emits
	// the field when alertWebhookHasExplicit is true.
	if wh := t.AlertWebhook(); wh != nil {
		snap.alertWebhook = string(wh)
		snap.alertWebhookHasExplicit = true
	}
	if t.AlertWebhookSecretWrappedLength() > 0 {
		// Copy out of the FB buffer so the snap outlives buf.
		raw := t.AlertWebhookSecretWrappedBytes()
		cp := make([]byte, len(raw))
		copy(cp, raw)
		snap.alertWebhookSecretWrapped = cp
	}
	if b := t.DiskWarnFrac(nil); b != nil {
		v := b.V()
		snap.diskWarnFrac = &v
	}
	if b := t.DiskCriticalFrac(nil); b != nil {
		v := b.V()
		snap.diskCriticalFrac = &v
	}
	return snap, nil
}
