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
// It first re-verifies that the target's shard is STILL referenced in the current
// object-version meta (via ShardTargetStillReferenced): if the version was
// deleted/replaced or the segment/coalesced shard was legitimately coalesced away
// between scan and callback, it logs and returns without quarantining — never marks
// a different (clean) version or a stale, de-referenced shard.
func (b *DistributedBackend) QuarantineCorruptShardLocalAtShardKey(t ECShardScanTarget, shardIdx int, reason string) error {
	if !b.ShardTargetStillReferenced(context.Background(), t) {
		log.Warn().
			Str("bucket", t.Bucket).
			Str("key", t.ObjectKey).
			Str("version_id", t.VersionID).
			Str("shard_key", t.ShardKey).
			Msg("quarantine: shard no longer referenced in current meta (scan/callback race); skipping quarantine")
		return nil
	}

	now := time.Now().UTC()
	cid := incidentID(t.Bucket, t.ObjectKey, t.VersionID, shardIdx, now)
	scope := incident.Scope{
		Kind:      incident.ScopeObject,
		Bucket:    t.Bucket,
		Key:       t.ObjectKey,
		VersionID: t.VersionID,
		ShardID:   shardIdx,
		NodeID:    b.NodeID(),
	}
	facts := []incident.Fact{
		{CorrelationID: cid, Type: incident.FactObserved, Cause: incident.CauseCorruptShard, Scope: scope, Message: "shard_key=" + t.ShardKey, At: now},
		{CorrelationID: cid, Type: incident.FactActionStarted, Action: incident.ActionIsolateObject, At: now.Add(time.Millisecond)},
	}
	if err := b.QuarantineObject(context.Background(), t.Bucket, t.ObjectKey, t.VersionID, string(incident.CauseCorruptShard), reason); err != nil {
		facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactActionFailed, Action: incident.ActionIsolateObject, ErrorCode: "quarantine_failed", At: time.Now().UTC()})
		_ = recordIncident(context.Background(), b.incidentRecorder, facts)
		return err
	}
	facts = append(facts, incident.Fact{CorrelationID: cid, Type: incident.FactIsolated, Action: incident.ActionIsolateObject, At: time.Now().UTC()})
	return recordIncident(context.Background(), b.incidentRecorder, facts)
}

// ShardTargetStillReferenced re-reads the parent object-version meta and confirms the
// target's shard is still referenced (defends against coalesce/delete/GC between the
// monitor's scan and the deferred callback). ObjectVersion targets: just verify the
// version still exists (no shard-ref to check). Segment: blobID still in Segments[].
// Coalesced: target.ShardKey still in Coalesced[].ShardKey. Any read error other than
// not-found returns false (conservative: don't act on uncertain state).
func (b *DistributedBackend) ShardTargetStillReferenced(ctx context.Context, t ECShardScanTarget) bool {
	var m objectMeta
	found := false
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().ObjectMetaKeyV(t.Bucket, t.ObjectKey, t.VersionID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		val, verr := b.itemValueCopy(item)
		if verr != nil {
			return verr
		}
		decoded, derr := unmarshalObjectMeta(val)
		if derr != nil {
			return derr
		}
		m = decoded
		found = true
		return nil
	})
	if err != nil {
		log.Warn().
			Str("bucket", t.Bucket).
			Str("key", t.ObjectKey).
			Str("version_id", t.VersionID).
			Str("shard_key", t.ShardKey).
			Err(err).
			Msg("ShardTargetStillReferenced: meta read failed; treating as de-referenced")
		return false
	}
	if !found {
		return false
	}

	switch t.Kind {
	case ECShardObjectVersion:
		// version-exists IS the check; there is no separate shard ref.
		return true
	case ECShardSegment:
		for i := range m.Segments {
			if t.ObjectKey+"/segments/"+m.Segments[i].BlobID == t.ShardKey {
				return true
			}
		}
		return false
	case ECShardCoalesced:
		for i := range m.Coalesced {
			if m.Coalesced[i].ShardKey == t.ShardKey {
				return true
			}
		}
		return false
	default:
		return false
	}
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
