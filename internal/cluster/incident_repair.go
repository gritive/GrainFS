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
	Bucket    string
	Key       string
	VersionID string
	ShardIdx  int
	Recorder  IncidentRecorder
	Now       time.Time
}

func (b *DistributedBackend) RepairShardLocalWithIncident(ctx context.Context, req IncidentRepairRequest) error {
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cid := incidentID(req.Bucket, req.Key, req.VersionID, req.ShardIdx, now)
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
		incident.Fact{CorrelationID: cid, Type: incident.FactDiagnosed, Cause: incident.CauseMissingShard, Scope: scope, Message: "attempting shard reconstruction from surviving peers", At: time.Now().UTC()},
		incident.Fact{CorrelationID: cid, Type: incident.FactActionStarted, Action: incident.ActionReconstructShard, At: time.Now().UTC()},
	)
	err := b.RepairShard(ctx, req.Bucket, req.Key, req.VersionID, req.ShardIdx)
	if err != nil {
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

func recordIncident(ctx context.Context, rec IncidentRecorder, facts []incident.Fact) error {
	if rec == nil {
		return nil
	}
	return rec.Record(ctx, facts)
}

func incidentID(bucket, key, versionID string, shardIdx int, now time.Time) string {
	return fmt.Sprintf("%s:%s:%s:%s:%d", now.Format("20060102T150405.000000000Z07:00"), bucket, key, versionID, shardIdx)
}
