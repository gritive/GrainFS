package cluster

import (
	"sync/atomic"
	"time"
)

// ClusterConfigAlertSecretAAD is the additional-authenticated-data binding the
// wrapped alert-webhook-secret ciphertext to this specific cluster-config
// field. Mirrors IAM's per-SA AAD pattern: prevents cross-ciphertext
// substitution by the storage layer. Constant — never rotated.
// Referenced by adminapi (PATCH wrap) and alerts consumer (unwrap).
var ClusterConfigAlertSecretAAD = []byte("cluster_config.alert_webhook_secret")

// ClusterConfigPatch is the in-memory equivalent of MetaClusterConfigPatchCmd.
// Each pointer field: nil = leave alone, non-nil = set explicit value.
// alert-webhook is treated specially: empty string means "leave alone";
// use ResetKeys to clear it.
type ClusterConfigPatch struct {
	BalancerEnabled             *bool
	BalancerImbalanceTriggerPct *float64
	BalancerImbalanceStopPct    *float64
	BalancerMigrationRate       *int32
	BalancerLeaderTenureMin     *time.Duration
	BalancerWarmupTimeout       *time.Duration
	BalancerCBThreshold         *float64
	BalancerMigrationMaxRetries *int32
	BalancerMigrationPendingTTL *time.Duration
	BalancerGossipInterval      *time.Duration

	AlertWebhook              *string
	AlertWebhookSecretWrapped []byte // nil = leave alone; len()==0 with explicit nil semantics handled via ResetKeys

	DiskWarnFrac     *float64
	DiskCriticalFrac *float64

	SnapshotInterval *time.Duration
	SnapshotRetain   *int32

	ResetKeys   []string
	ExpectedRev uint64 // 0 = no CAS

	// UpdatedAtUnixMs is the wall-clock timestamp stamped by the proposer
	// before the patch is sent through Raft. The FSM apply path uses this
	// value (instead of time.Now()) so every replica writes the same
	// clusterConfigSnap.updatedAt for the same log entry — required for
	// deterministic FSM state and byte-identical snapshots.
	// 0 = unstamped (legacy/test path; FSM falls back to time.Now()).
	UpdatedAtUnixMs int64
}

// ClusterConfig is the merged view that consumers read. The "explicit" inner
// struct mirrors the FSM-stored sparse state; getters fall back to code defaults
// for any nil field.
//
// Mutation paths are single-writer (MetaFSM.Apply goroutine). Readers use
// atomic.Pointer to swap whole snapshots so consumer reads are lock-free.
type ClusterConfig struct {
	snap atomic.Pointer[clusterConfigSnap]
}

type clusterConfigSnap struct {
	rev       uint64
	updatedAt time.Time

	balancerEnabled             *bool
	balancerImbalanceTriggerPct *float64
	balancerImbalanceStopPct    *float64
	balancerMigrationRate       *int32
	balancerLeaderTenureMin     *time.Duration
	balancerWarmupTimeout       *time.Duration
	balancerCBThreshold         *float64
	balancerMigrationMaxRetries *int32
	balancerMigrationPendingTTL *time.Duration
	balancerGossipInterval      *time.Duration

	alertWebhook              string // "" if not explicitly set
	alertWebhookHasExplicit   bool   // distinguishes default "" from explicit ""
	alertWebhookSecretWrapped []byte // nil if not set

	diskWarnFrac     *float64
	diskCriticalFrac *float64

	snapshotInterval *time.Duration
	snapshotRetain   *int32
}

func NewClusterConfig() *ClusterConfig {
	c := &ClusterConfig{}
	c.snap.Store(&clusterConfigSnap{})
	return c
}

// ReplaceSnap atomically swaps the internal snapshot pointer to s. Used by
// MetaFSM.Restore so the outer *ClusterConfig handle held by consumers stays
// valid across snapshot restore. Single-writer (FSM goroutine).
func (c *ClusterConfig) ReplaceSnap(s *clusterConfigSnap) { c.snap.Store(s) }

// applyPatch merges patch into a new snapshot and atomically swaps.
// Called from MetaFSM.Apply only. Does NOT validate — caller must Validate()
// the resulting snapshot before committing.
func (c *ClusterConfig) applyPatch(p ClusterConfigPatch, ts time.Time) {
	prev := c.snap.Load()
	next := *prev // shallow copy; pointer fields are immutable so sharing is safe
	next.rev = prev.rev + 1
	next.updatedAt = ts

	if p.BalancerEnabled != nil {
		next.balancerEnabled = p.BalancerEnabled
	}
	if p.BalancerImbalanceTriggerPct != nil {
		next.balancerImbalanceTriggerPct = p.BalancerImbalanceTriggerPct
	}
	if p.BalancerImbalanceStopPct != nil {
		next.balancerImbalanceStopPct = p.BalancerImbalanceStopPct
	}
	if p.BalancerMigrationRate != nil {
		next.balancerMigrationRate = p.BalancerMigrationRate
	}
	if p.BalancerLeaderTenureMin != nil {
		next.balancerLeaderTenureMin = p.BalancerLeaderTenureMin
	}
	if p.BalancerWarmupTimeout != nil {
		next.balancerWarmupTimeout = p.BalancerWarmupTimeout
	}
	if p.BalancerCBThreshold != nil {
		next.balancerCBThreshold = p.BalancerCBThreshold
	}
	if p.BalancerMigrationMaxRetries != nil {
		next.balancerMigrationMaxRetries = p.BalancerMigrationMaxRetries
	}
	if p.BalancerMigrationPendingTTL != nil {
		next.balancerMigrationPendingTTL = p.BalancerMigrationPendingTTL
	}
	if p.BalancerGossipInterval != nil {
		next.balancerGossipInterval = p.BalancerGossipInterval
	}
	if p.AlertWebhook != nil {
		next.alertWebhook = *p.AlertWebhook
		next.alertWebhookHasExplicit = true
	}
	if p.AlertWebhookSecretWrapped != nil {
		next.alertWebhookSecretWrapped = p.AlertWebhookSecretWrapped
	}
	if p.DiskWarnFrac != nil {
		next.diskWarnFrac = p.DiskWarnFrac
	}
	if p.DiskCriticalFrac != nil {
		next.diskCriticalFrac = p.DiskCriticalFrac
	}
	if p.SnapshotInterval != nil {
		next.snapshotInterval = p.SnapshotInterval
	}
	if p.SnapshotRetain != nil {
		next.snapshotRetain = p.SnapshotRetain
	}

	for _, k := range p.ResetKeys {
		next.clearKey(k)
	}

	c.snap.Store(&next)
}

func (s *clusterConfigSnap) clearKey(k string) {
	switch k {
	case "balancer-enabled":
		s.balancerEnabled = nil
	case "balancer-imbalance-trigger-pct":
		s.balancerImbalanceTriggerPct = nil
	case "balancer-imbalance-stop-pct":
		s.balancerImbalanceStopPct = nil
	case "balancer-migration-rate":
		s.balancerMigrationRate = nil
	case "balancer-leader-tenure-min":
		s.balancerLeaderTenureMin = nil
	case "balancer-warmup-timeout":
		s.balancerWarmupTimeout = nil
	case "balancer-cb-threshold":
		s.balancerCBThreshold = nil
	case "balancer-migration-max-retries":
		s.balancerMigrationMaxRetries = nil
	case "balancer-migration-pending-ttl":
		s.balancerMigrationPendingTTL = nil
	case "balancer-gossip-interval":
		s.balancerGossipInterval = nil
	case "alert-webhook":
		s.alertWebhook = ""
		s.alertWebhookHasExplicit = false
	case "alert-webhook-secret":
		s.alertWebhookSecretWrapped = nil
	case "disk-warn-threshold":
		s.diskWarnFrac = nil
	case "disk-critical-threshold":
		s.diskCriticalFrac = nil
	case "snapshot-interval":
		s.snapshotInterval = nil
	case "snapshot-retain":
		s.snapshotRetain = nil
	}
}

// AllConfigKeys returns the canonical ordered list of cluster-config keys
// — used by `cluster config show` and to validate operator-supplied key names.
func AllConfigKeys() []string {
	return []string{
		"balancer-enabled",
		"balancer-imbalance-trigger-pct",
		"balancer-imbalance-stop-pct",
		"balancer-migration-rate",
		"balancer-leader-tenure-min",
		"balancer-warmup-timeout",
		"balancer-cb-threshold",
		"balancer-migration-max-retries",
		"balancer-migration-pending-ttl",
		"balancer-gossip-interval",
		"alert-webhook",
		"alert-webhook-secret",
		"disk-warn-threshold",
		"disk-critical-threshold",
		"snapshot-interval",
		"snapshot-retain",
	}
}

// Rev returns the current revision.
func (c *ClusterConfig) Rev() uint64 { return c.snap.Load().rev }

// UpdatedAt returns the last-update timestamp.
func (c *ClusterConfig) UpdatedAt() time.Time { return c.snap.Load().updatedAt }

func (c *ClusterConfig) BalancerEnabled() bool {
	if v := c.snap.Load().balancerEnabled; v != nil {
		return *v
	}
	return DefaultClusterBalancerEnabled
}

func (c *ClusterConfig) BalancerImbalanceTriggerPct() float64 {
	if v := c.snap.Load().balancerImbalanceTriggerPct; v != nil {
		return *v
	}
	return DefaultClusterBalancerImbalanceTriggerPct
}

func (c *ClusterConfig) BalancerImbalanceStopPct() float64 {
	if v := c.snap.Load().balancerImbalanceStopPct; v != nil {
		return *v
	}
	return DefaultClusterBalancerImbalanceStopPct
}

func (c *ClusterConfig) BalancerMigrationRate() int32 {
	if v := c.snap.Load().balancerMigrationRate; v != nil {
		return *v
	}
	return DefaultClusterBalancerMigrationRate
}

func (c *ClusterConfig) BalancerLeaderTenureMin() time.Duration {
	if v := c.snap.Load().balancerLeaderTenureMin; v != nil {
		return *v
	}
	return DefaultClusterBalancerLeaderTenureMin
}

func (c *ClusterConfig) BalancerWarmupTimeout() time.Duration {
	if v := c.snap.Load().balancerWarmupTimeout; v != nil {
		return *v
	}
	return DefaultClusterBalancerWarmupTimeout
}

func (c *ClusterConfig) BalancerCBThreshold() float64 {
	if v := c.snap.Load().balancerCBThreshold; v != nil {
		return *v
	}
	return DefaultClusterBalancerCBThreshold
}

func (c *ClusterConfig) BalancerMigrationMaxRetries() int32 {
	if v := c.snap.Load().balancerMigrationMaxRetries; v != nil {
		return *v
	}
	return DefaultClusterBalancerMigrationMaxRetries
}

func (c *ClusterConfig) BalancerMigrationPendingTTL() time.Duration {
	if v := c.snap.Load().balancerMigrationPendingTTL; v != nil {
		return *v
	}
	return DefaultClusterBalancerMigrationPendingTTL
}

func (c *ClusterConfig) BalancerGossipInterval() time.Duration {
	if v := c.snap.Load().balancerGossipInterval; v != nil {
		return *v
	}
	return DefaultClusterBalancerGossipInterval
}

func (c *ClusterConfig) AlertWebhook() string {
	s := c.snap.Load()
	if s.alertWebhookHasExplicit {
		return s.alertWebhook
	}
	return DefaultAlertWebhook()
}

func (c *ClusterConfig) AlertWebhookSecretWrapped() []byte {
	return c.snap.Load().alertWebhookSecretWrapped
}

func (c *ClusterConfig) DiskWarnFrac() float64 {
	if v := c.snap.Load().diskWarnFrac; v != nil {
		return *v
	}
	return DefaultClusterDiskWarnFrac
}

func (c *ClusterConfig) DiskCriticalFrac() float64 {
	if v := c.snap.Load().diskCriticalFrac; v != nil {
		return *v
	}
	return DefaultClusterDiskCriticalFrac
}

func (c *ClusterConfig) SnapshotInterval() time.Duration {
	if v := c.snap.Load().snapshotInterval; v != nil {
		return *v
	}
	return DefaultClusterSnapshotInterval
}

func (c *ClusterConfig) SnapshotRetain() int32 {
	if v := c.snap.Load().snapshotRetain; v != nil {
		return *v
	}
	return DefaultClusterSnapshotRetain
}

// SourceForKey returns "default" or "explicit" — used by `cluster config show`.
func (c *ClusterConfig) SourceForKey(key string) string {
	s := c.snap.Load()
	switch key {
	case "balancer-enabled":
		if s.balancerEnabled != nil {
			return "explicit"
		}
	case "balancer-imbalance-trigger-pct":
		if s.balancerImbalanceTriggerPct != nil {
			return "explicit"
		}
	case "balancer-imbalance-stop-pct":
		if s.balancerImbalanceStopPct != nil {
			return "explicit"
		}
	case "balancer-migration-rate":
		if s.balancerMigrationRate != nil {
			return "explicit"
		}
	case "balancer-leader-tenure-min":
		if s.balancerLeaderTenureMin != nil {
			return "explicit"
		}
	case "balancer-warmup-timeout":
		if s.balancerWarmupTimeout != nil {
			return "explicit"
		}
	case "balancer-cb-threshold":
		if s.balancerCBThreshold != nil {
			return "explicit"
		}
	case "balancer-migration-max-retries":
		if s.balancerMigrationMaxRetries != nil {
			return "explicit"
		}
	case "balancer-migration-pending-ttl":
		if s.balancerMigrationPendingTTL != nil {
			return "explicit"
		}
	case "balancer-gossip-interval":
		if s.balancerGossipInterval != nil {
			return "explicit"
		}
	case "alert-webhook":
		if s.alertWebhookHasExplicit {
			return "explicit"
		}
	case "alert-webhook-secret":
		if s.alertWebhookSecretWrapped != nil {
			return "explicit"
		}
	case "disk-warn-threshold":
		if s.diskWarnFrac != nil {
			return "explicit"
		}
	case "disk-critical-threshold":
		if s.diskCriticalFrac != nil {
			return "explicit"
		}
	case "snapshot-interval":
		if s.snapshotInterval != nil {
			return "explicit"
		}
	case "snapshot-retain":
		if s.snapshotRetain != nil {
			return "explicit"
		}
	}
	return "default"
}
