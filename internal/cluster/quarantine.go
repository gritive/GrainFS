package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/incident"
)

var ErrObjectQuarantined = errors.New("object quarantined")

func quarantineKey(bucket, key, versionID string) []byte {
	return []byte("quarantine:" + bucket + "\x00" + key + "\x00" + versionID)
}

func (b *DistributedBackend) QuarantineObject(ctx context.Context, bucket, key, versionID, cause, reason string) error {
	return b.propose(ctx, CmdPutObjectQuarantine, PutObjectQuarantineCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		Cause:     cause,
		Reason:    reason,
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
	if err := b.QuarantineObject(context.Background(), bucket, key, versionID, string(incident.CauseCorruptShard), reason); err != nil {
		facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactActionFailed, Action: incident.ActionIsolateObject, ErrorCode: "quarantine_failed", At: time.Now().UTC()})
		_ = recordIncident(context.Background(), b.incidentRecorder, facts)
		return err
	}
	facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactIsolated, Action: incident.ActionIsolateObject, At: time.Now().UTC()})
	return recordIncident(context.Background(), b.incidentRecorder, facts)
}

func (b *DistributedBackend) isObjectQuarantined(bucket, key, versionID string) (bool, PutObjectQuarantineCmd, error) {
	var out PutObjectQuarantineCmd
	err := b.db.View(func(txn *badger.Txn) error {
		for _, candidate := range []string{versionID, ""} {
			item, err := txn.Get(quarantineKey(bucket, key, candidate))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				return json.Unmarshal(v, &out)
			})
		}
		return nil
	})
	if err != nil {
		return false, out, err
	}
	return out.Bucket != "", out, nil
}

func objectQuarantinedError(bucket, key string, q PutObjectQuarantineCmd) error {
	scope := bucket + "/" + key
	if q.VersionID != "" {
		scope += "@" + q.VersionID
	}
	return fmt.Errorf("%w: %s cause=%s reason=%s", ErrObjectQuarantined, scope, q.Cause, strings.TrimSpace(q.Reason))
}
