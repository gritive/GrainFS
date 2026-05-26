package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/incident"
)

type IncidentRecorder interface {
	Record(context.Context, []incident.Fact) error
}

type IncidentRepairRequest struct {
	Bucket string
	Key    string
	// VersionID is empty for segment/coalesced (shard-key) repairs: those have no
	// object version, so the physical target is carried by ShardKey instead and
	// surfaced in the Diagnosed fact Message (incident.Scope has no ShardKey field).
	VersionID     string
	ShardIdx      int
	Recorder      IncidentRecorder
	CorrelationID string
	Now           time.Time
	// ShardKey, when non-empty, routes RepairShardLocalWithIncident to the shard-key repair path; Placement must also be set.
	ShardKey string
	// Placement is the resolved EC placement used by the shard-key repair path; only consulted when ShardKey is non-empty.
	Placement PlacementRecord
}

// repairDiagMessage builds the Diagnosed-fact message, appending the shardKey
// for traceability on the segment/coalesced (shard-key) repair path.
func repairDiagMessage(req IncidentRepairRequest) string {
	msg := "attempting shard reconstruction from surviving peers"
	if req.ShardKey != "" {
		msg += fmt.Sprintf(" (shardKey=%s)", req.ShardKey)
	}
	return msg
}

func (b *DistributedBackend) RepairShardLocalWithIncident(ctx context.Context, req IncidentRepairRequest) error {
	if req.ShardKey != "" && len(req.Placement.Nodes) == 0 {
		return fmt.Errorf("shard-key repair requires non-empty Placement.Nodes for %s", req.ShardKey)
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cid := req.CorrelationID
	if cid == "" {
		cid = incidentID(req.Bucket, req.Key, req.VersionID, req.ShardIdx, now)
	}
	scope := incident.Scope{Kind: incident.ScopeShard, Bucket: req.Bucket, Key: req.Key, VersionID: req.VersionID, ShardID: req.ShardIdx, NodeID: b.NodeID()}
	facts := []incident.Fact{{
		CorrelationID: cid,
		Type:          incident.FactObserved,
		Cause:         incident.CauseMissingShard,
		Scope:         scope,
		At:            now,
	}}
	if err := recordIncident(ctx, req.Recorder, facts); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactActionFailed, Action: incident.ActionReconstructShard, ErrorCode: "context_canceled", At: time.Now().UTC()})
		_ = recordIncident(context.Background(), req.Recorder, facts)
		return err
	}
	facts = append(facts,
		incident.Fact{CorrelationID: cid, Type: incident.FactDiagnosed, Cause: incident.CauseMissingShard, Scope: scope, Message: repairDiagMessage(req), At: time.Now().UTC()},
		incident.Fact{CorrelationID: cid, Type: incident.FactActionStarted, Action: incident.ActionReconstructShard, At: time.Now().UTC()},
	)
	var err error
	if req.ShardKey != "" {
		err = b.RepairShardAtShardKey(ctx, req.Bucket, req.ShardKey, req.Placement, req.ShardIdx)
	} else {
		err = b.RepairShard(ctx, req.Bucket, req.Key, req.VersionID, req.ShardIdx)
	}
	if err != nil {
		readable := false
		if req.ShardKey != "" {
			readable = b.localRepairTargetReadableAtShardKey(ctx, req.Bucket, req.ShardKey, req.Placement, req.ShardIdx)
		} else {
			readable = b.localRepairTargetReadable(ctx, req.Bucket, req.Key, req.VersionID, req.ShardIdx)
		}
		if readable {
			facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactVerified, At: time.Now().UTC()})
			return recordIncident(ctx, req.Recorder, facts)
		}
		code := "repair_failed"
		if errors.Is(err, context.Canceled) {
			code = "context_canceled"
		} else if strings.Contains(err.Error(), "other shards readable") || strings.Contains(err.Error(), "only ") {
			code = "insufficient_survivors"
		}
		facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactActionFailed, Action: incident.ActionReconstructShard, ErrorCode: code, At: time.Now().UTC()})
		_ = recordIncident(context.Background(), req.Recorder, facts)
		return err
	}
	facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactVerified, At: time.Now().UTC()})
	return recordIncident(ctx, req.Recorder, facts)
}

func (b *DistributedBackend) localRepairTargetReadable(ctx context.Context, bucket, key, versionID string, shardIdx int) bool {
	if b.shardSvc == nil {
		return false
	}
	if versionID == "" {
		latest, err := b.fsm.LookupLatestVersion(bucket, key)
		if err != nil {
			return false
		}
		versionID = latest
	}
	resolved, err := b.ResolvePlacement(ctx, bucket, key, b.readPlacementMeta(bucket, key, versionID))
	if err != nil || shardIdx < 0 || shardIdx >= len(resolved.Record.Nodes) {
		return false
	}
	if resolved.Record.Nodes[shardIdx] != b.currentSelfAddr() {
		return false
	}
	_, err = b.shardSvc.ReadLocalShard(bucket, resolved.ShardKey, shardIdx)
	return err == nil
}

// localRepairTargetReadableAtShardKey mirrors localRepairTargetReadable but uses
// the explicit shardKey + resolved placement record rather than re-resolving
// object-version placement. Used for segment/coalesced shard repair where the
// physical shard key is not derivable from an object version.
func (b *DistributedBackend) localRepairTargetReadableAtShardKey(_ context.Context, bucket, shardKey string, rec PlacementRecord, shardIdx int) bool {
	if b.shardSvc == nil {
		return false
	}
	if shardIdx < 0 || shardIdx >= len(rec.Nodes) {
		return false
	}
	if rec.Nodes[shardIdx] != b.currentSelfAddr() {
		return false
	}
	_, err := b.shardSvc.ReadLocalShard(bucket, shardKey, shardIdx)
	return err == nil
}

func (b *DistributedBackend) RecordRepairReceiptSigned(ctx context.Context, req IncidentRepairRequest, receiptID string) error {
	if receiptID == "" {
		return nil
	}
	now := time.Now().UTC()
	cid := req.CorrelationID
	if cid == "" {
		cid = incidentID(req.Bucket, req.Key, req.VersionID, req.ShardIdx, now)
	}
	scope := incident.Scope{Kind: incident.ScopeShard, Bucket: req.Bucket, Key: req.Key, VersionID: req.VersionID, ShardID: req.ShardIdx, NodeID: b.NodeID()}
	facts := []incident.Fact{
		{CorrelationID: cid, Type: incident.FactObserved, Cause: incident.CauseMissingShard, Scope: scope, At: now},
		{CorrelationID: cid, Type: incident.FactDiagnosed, Cause: incident.CauseMissingShard, Scope: scope, Message: repairDiagMessage(req), At: now.Add(time.Millisecond)},
		{CorrelationID: cid, Type: incident.FactActionStarted, Action: incident.ActionReconstructShard, At: now.Add(2 * time.Millisecond)},
		{CorrelationID: cid, Type: incident.FactVerified, At: now.Add(3 * time.Millisecond)},
		{CorrelationID: cid, Type: incident.FactReceiptSigned, ReceiptID: receiptID, At: now.Add(4 * time.Millisecond)},
	}
	return recordIncident(ctx, req.Recorder, facts)
}

func recordIncident(ctx context.Context, rec IncidentRecorder, facts []incident.Fact) error {
	if rec == nil {
		return nil
	}
	return rec.Record(ctx, facts)
}

func incidentID(bucket, key, versionID string, shardIdx int, now time.Time) string {
	return fmt.Sprintf("%s:%s:%s:%s:%d", now.Format("20060102T150405.000000000Z07:00"), bucket, key, versionID, shardIdx)
}
