package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/incident"
)

var ErrObjectQuarantined = errors.New("object quarantined")

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

// QuarantineCorruptShardLocalAtShardKey quarantines the parent object of a corrupt
// segment/coalesced shard, using the version the scan observed (NOT "latest now").
// If that exact version no longer exists (deleted/replaced between scan and callback),
// it logs and returns without quarantining — never marks a different (clean) version.
func (b *DistributedBackend) QuarantineCorruptShardLocalAtShardKey(bucket, objectKey, versionID, shardKey string, shardIdx int, reason string) error {
	// Verify the exact (bucket, objectKey, versionID) object-version meta still exists.
	exists, err := b.objectVersionMetaExists(bucket, objectKey, versionID)
	if err != nil {
		return fmt.Errorf("QuarantineCorruptShardLocalAtShardKey: existence check: %w", err)
	}
	if !exists {
		log.Warn().
			Str("bucket", bucket).
			Str("key", objectKey).
			Str("version_id", versionID).
			Str("shard_key", shardKey).
			Msg("quarantine: object-version no longer exists (scan/callback race); skipping quarantine")
		return nil
	}

	now := time.Now().UTC()
	cid := incidentID(bucket, objectKey, versionID, shardIdx, now)
	scope := incident.Scope{
		Kind:      incident.ScopeObject,
		Bucket:    bucket,
		Key:       objectKey,
		VersionID: versionID,
		ShardID:   shardIdx,
		NodeID:    b.NodeID(),
	}
	facts := []incident.Fact{
		{CorrelationID: cid, Type: incident.FactObserved, Cause: incident.CauseCorruptShard, Scope: scope, Message: "shard_key=" + shardKey, At: now},
		{CorrelationID: cid, Type: incident.FactActionStarted, Action: incident.ActionIsolateObject, At: now.Add(time.Millisecond)},
	}
	if err := b.QuarantineObject(context.Background(), bucket, objectKey, versionID, string(incident.CauseCorruptShard), reason); err != nil {
		facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactActionFailed, Action: incident.ActionIsolateObject, ErrorCode: "quarantine_failed", At: time.Now().UTC()})
		_ = recordIncident(context.Background(), b.incidentRecorder, facts)
		return err
	}
	facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactIsolated, Action: incident.ActionIsolateObject, At: time.Now().UTC()})
	return recordIncident(context.Background(), b.incidentRecorder, facts)
}

// objectVersionMetaExists reports whether an exact (bucket, key, versionID)
// object-version metadata key exists in the FSM store.
func (b *DistributedBackend) objectVersionMetaExists(bucket, key, versionID string) (bool, error) {
	var exists bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	return exists, err
}

func (b *DistributedBackend) isObjectQuarantined(bucket, key, versionID string) (bool, PutObjectQuarantineCmd, error) {
	var out PutObjectQuarantineCmd
	err := b.db.View(func(txn *badger.Txn) error {
		for _, candidate := range []string{versionID, ""} {
			item, err := txn.Get(b.ks().QuarantineKey(bucket, key, candidate))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				var decErr error
				out, decErr = decodePutObjectQuarantineCmdStorage(v)
				return decErr
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
