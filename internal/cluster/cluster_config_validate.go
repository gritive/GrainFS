package cluster

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Validate runs cross-field invariants over the *effective* (merged) values
// reachable through the getters. Called from MetaFSM.Apply after applyPatch;
// failure → reject the patch (roll back to pre-patch snapshot).
func (c *ClusterConfig) Validate() error {
	var errs []string

	trig := c.BalancerImbalanceTriggerPct()
	if trig <= 0 || trig > 100 {
		errs = append(errs, fmt.Sprintf("balancer-imbalance-trigger-pct must be in (0, 100], got %v", trig))
	}
	stop := c.BalancerImbalanceStopPct()
	if stop < 0 || stop >= 100 {
		errs = append(errs, fmt.Sprintf("balancer-imbalance-stop-pct must be in [0, 100), got %v", stop))
	}
	if stop > trig {
		errs = append(errs, fmt.Sprintf("balancer-imbalance-stop-pct must be <= trigger-pct (stop=%v trig=%v)", stop, trig))
	}
	if cb := c.BalancerCBThreshold(); cb <= 0 || cb > 1 {
		errs = append(errs, fmt.Sprintf("balancer-cb-threshold must be in (0, 1], got %v", cb))
	}
	if r := c.BalancerMigrationRate(); r < 1 {
		errs = append(errs, fmt.Sprintf("balancer-migration-rate must be >= 1, got %v", r))
	}
	if r := c.BalancerMigrationMaxRetries(); r < 0 {
		errs = append(errs, fmt.Sprintf("balancer-migration-max-retries must be >= 0, got %v", r))
	}
	if d := c.BalancerLeaderTenureMin(); d <= 0 {
		errs = append(errs, fmt.Sprintf("balancer-leader-tenure-min must be > 0, got %v", d))
	}
	if d := c.BalancerWarmupTimeout(); d < 0 {
		errs = append(errs, fmt.Sprintf("balancer-warmup-timeout must be >= 0, got %v", d))
	}
	if d := c.BalancerMigrationPendingTTL(); d <= 0 {
		errs = append(errs, fmt.Sprintf("balancer-migration-pending-ttl must be > 0, got %v", d))
	}
	if d := c.BalancerGossipInterval(); d <= 0 {
		errs = append(errs, fmt.Sprintf("balancer-gossip-interval must be > 0, got %v", d))
	}

	dw := c.DiskWarnFrac()
	if dw <= 0 || dw > 1 {
		errs = append(errs, fmt.Sprintf("disk-warn-threshold must be in (0, 1], got %v", dw))
	}
	dc := c.DiskCriticalFrac()
	if dc <= 0 || dc > 1 {
		errs = append(errs, fmt.Sprintf("disk-critical-threshold must be in (0, 1], got %v", dc))
	}
	if dw > dc {
		errs = append(errs, fmt.Sprintf("disk-warn-threshold must be <= disk-critical-threshold (warn=%v crit=%v)", dw, dc))
	}

	if d := c.SnapshotInterval(); d < 0 {
		errs = append(errs, fmt.Sprintf("snapshot-interval must be >= 0 (0=disable), got %v", d))
	} else if d > 0 && d < time.Second {
		errs = append(errs, fmt.Sprintf("snapshot-interval must be 0 (disable) or >= 1s, got %v", d))
	}
	if r := c.SnapshotRetain(); r < 1 {
		errs = append(errs, fmt.Sprintf("snapshot-retain must be >= 1, got %v", r))
	}

	if u := c.AlertWebhook(); u != "" {
		if !strings.HasPrefix(u, "https://") && !strings.HasPrefix(u, "http://localhost") {
			errs = append(errs, fmt.Sprintf("alert-webhook must be https:// or http://localhost (tests), got %q", u))
		}
	}

	if bl := c.BoundedLoadsC(); bl < 1.0 || bl > 3.0 {
		errs = append(errs, fmt.Sprintf("bounded-loads-c must be in [1.0, 3.0], got %v", bl))
	}
	if blLow := c.BoundedLoadsCLow(); blLow < 0.5 || blLow >= c.BoundedLoadsC() {
		errs = append(errs, fmt.Sprintf("bounded-loads-c-low must be in [0.5, %v), got %v", c.BoundedLoadsC(), blLow))
	}
	if d := c.BoundedLoadsMaxStaleTTL(); d < time.Second {
		errs = append(errs, fmt.Sprintf("bounded-loads-max-stale-ttl must be >= 1s, got %v", d))
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New("cluster config invalid: " + strings.Join(errs, "; "))
}
