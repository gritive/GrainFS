package cluster

import "time"

// ClusterConfig defaults — the values returned by getters when the FSM has no
// explicit override. Sourced from the v0.150 flag defaults (balancer values
// mirror DefaultBalancerConfig()). These are the single source of truth for
// "system default". Operators changing them via `cluster config set` produce
// explicit FSM entries; resetting puts the cluster back here.
const (
	DefaultClusterBalancerEnabled             = true
	DefaultClusterBalancerImbalanceTriggerPct = 20.0 // percent (0-100), matches BalancerConfig.ImbalanceTriggerPct
	DefaultClusterBalancerImbalanceStopPct    = 5.0  // percent (0-100)
	DefaultClusterBalancerMigrationRate       = 1
	DefaultClusterBalancerLeaderTenureMin     = 5 * time.Minute
	DefaultClusterBalancerWarmupTimeout       = 60 * time.Second
	DefaultClusterBalancerCBThreshold         = 0.90
	DefaultClusterBalancerMigrationMaxRetries = 3
	DefaultClusterBalancerMigrationPendingTTL = 5 * time.Minute
	DefaultClusterBalancerGossipInterval      = 30 * time.Second

	DefaultClusterDiskWarnFrac     = 0.80
	DefaultClusterDiskCriticalFrac = 0.90

	DefaultClusterSnapshotInterval = 1 * time.Hour
	DefaultClusterSnapshotRetain   = 24
)

// DefaultAlertWebhook returns "" — webhooks are disabled by default.
// Kept as a function for symmetry with future string defaults.
func DefaultAlertWebhook() string { return "" }
