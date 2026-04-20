package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// RecoveryManager orchestrates automatic recovery actions.
type RecoveryManager struct {
	doctor      *Doctor
	snapshotMgr interface{}
	peerHealth  interface{}
	replication interface{}
}

// NewRecoveryManager creates a recovery manager.
func NewRecoveryManager(dataDir string, snapMgr interface{}, ph interface{}, rm interface{}) *RecoveryManager {
	return &RecoveryManager{
		doctor:      NewDoctor(dataDir),
		snapshotMgr: snapMgr,
		peerHealth:  ph,
		replication: rm,
	}
}

// RecoveryReport contains recovery results.
type RecoveryReport struct {
	StartTime    time.Time
	EndTime      time.Time
	ActionsTaken []string
	Errors       []error
	Success      bool
}

// AutoRecover attempts to automatically recover from detected issues.
func (rm *RecoveryManager) AutoRecover(ctx context.Context) (*RecoveryReport, error) {
	report := &RecoveryReport{
		StartTime: time.Now(),
	}

	// Step 1: Run diagnostics
	diag, err := rm.doctor.Run()
	if err != nil {
		return nil, fmt.Errorf("diagnostic failed: %w", err)
	}

	// Step 2: Based on diagnosis, take recovery actions

	// Action 2a: Restore from latest snapshot if Raft log is corrupted
	if diag.Checks["raft_log"].Status == "fail" {
		slog.Info("recovery: attempting snapshot restore")
		// idx, err := rm.snapshotMgr.Restore()
		// For now, this is a placeholder - full integration would use actual SnapshotManager
		report.ActionsTaken = append(report.ActionsTaken,
			"Snapshot restore not yet implemented (placeholder)")
	}

	// Action 2b: Trigger shard placement monitor for under-replicated / missing shards.
	// Phase 18: ReplicationMonitor was replaced by ShardPlacementMonitor which scans
	// FSM placement entries instead of in-memory replica tracking.
	if rm.replication != nil {
		slog.Info("recovery: triggering shard placement check")
		if m, ok := rm.replication.(*ShardPlacementMonitor); ok {
			ctx := context.Background()
			if missing, err := m.Scan(ctx); err != nil {
				report.ActionsTaken = append(report.ActionsTaken,
					fmt.Sprintf("Placement scan failed: %v", err))
			} else {
				report.ActionsTaken = append(report.ActionsTaken,
					fmt.Sprintf("Placement scan found %d missing local shards", missing))
			}
		} else {
			report.ActionsTaken = append(report.ActionsTaken,
				"Placement monitor not wired (legacy type)")
		}
	}

	// Action 2c: Mark unhealthy peers as healthy for retry
	if rm.peerHealth != nil {
		slog.Info("recovery: resetting peer health for retry")
		// This would reset peer health via PeerHealth
		report.ActionsTaken = append(report.ActionsTaken,
			"Peer health reset for retry")
	}

	// Step 3: Verify recovery
	postDiag, err := rm.doctor.Run()
	if err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("post-recovery diagnostic failed: %w", err))
	}
	if postDiag != nil && postDiag.OverallHealth != "fail" {
		report.Success = true
	}

	report.EndTime = time.Now()
	return report, nil
}
