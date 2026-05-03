package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/incident"
)

var ErrObjectQuarantined = errors.New("object quarantined")

func quarantineKey(bucket, key string) []byte {
	return []byte("quarantine:" + bucket + "/" + key)
}

func (b *DistributedBackend) QuarantineObject(ctx context.Context, bucket, key, cause, reason string) error {
	return b.propose(ctx, CmdPutObjectQuarantine, PutObjectQuarantineCmd{
		Bucket: bucket,
		Key:    key,
		Cause:  cause,
		Reason: reason,
	})
}

func (b *DistributedBackend) QuarantineCorruptShardLocal(bucket, key, versionID string, shardIdx int, reason string) error {
	now := time.Now().UTC()
	cid := incidentID(bucket, key, versionID, shardIdx, now)
	scope := incident.Scope{
		Kind:      incident.ScopeObject,
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		ShardID:   shardIdx,
		NodeID:    b.NodeID(),
	}
	facts := []incident.Fact{
		{CorrelationID: cid, Type: incident.FactObserved, Cause: incident.CauseCorruptShard, Scope: scope, At: now},
		{CorrelationID: cid, Type: incident.FactActionStarted, Action: incident.ActionIsolateObject, At: now.Add(time.Millisecond)},
	}
	if err := b.QuarantineObject(context.Background(), bucket, key, string(incident.CauseCorruptShard), reason); err != nil {
		facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactActionFailed, Action: incident.ActionIsolateObject, ErrorCode: "quarantine_failed", At: time.Now().UTC()})
		_ = recordIncident(context.Background(), b.incidentRecorder, facts)
		return err
	}
	facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactIsolated, Action: incident.ActionIsolateObject, At: time.Now().UTC()})
	return recordIncident(context.Background(), b.incidentRecorder, facts)
}

func (b *DistributedBackend) isObjectQuarantined(bucket, key string) (bool, PutObjectQuarantineCmd, error) {
	var out PutObjectQuarantineCmd
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(quarantineKey(bucket, key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			return json.Unmarshal(v, &out)
		})
	})
	if err != nil {
		return false, out, err
	}
	return out.Bucket != "", out, nil
}

func objectQuarantinedError(bucket, key string, q PutObjectQuarantineCmd) error {
	return fmt.Errorf("%w: %s/%s cause=%s reason=%s", ErrObjectQuarantined, bucket, key, q.Cause, q.Reason)
}
