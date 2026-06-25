package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/storage"
)

var ErrObjectQuarantined = errors.New("object quarantined")

func (b *DistributedBackend) QuarantineObject(ctx context.Context, bucket, key, versionID, cause, reason string) error {
	unlock := b.objectMetaRMWLock(bucket, key)
	defer unlock()

	var cmd PutObjectMetaCmd
	var err error
	if versionID != "" {
		var found bool
		cmd, found, err = b.readQuorumMetaVersion(bucket, key, versionID)
		if err != nil {
			return fmt.Errorf("read quorum meta version: %w", err)
		}
		if !found {
			// Fall back to the latest-only blob when versionID matches.
			cmd, err = b.readQuorumMetaCmd(bucket, key)
			if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
				return fmt.Errorf("read quorum meta: %w", err)
			}
			if err == nil && cmd.VersionID == versionID {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("object not found: %s/%s@%s", bucket, key, versionID)
		}
	} else {
		cmd, err = b.readQuorumMetaCmd(bucket, key)
		if errors.Is(err, storage.ErrObjectNotFound) {
			return fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		if err != nil {
			return fmt.Errorf("read quorum meta: %w", err)
		}
		if cmd.Key == "" {
			return fmt.Errorf("object not found: %s/%s", bucket, key)
		}
	}
	cmd.IsQuarantined = true
	cmd.QuarantineCause = cause
	cmd.MetaSeq++
	return b.writeQuorumMeta(ctx, cmd)
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
	// The placement monitor may run on a NON-OWNER node; route the SET to the
	// owner so its RMW lock serializes it (incident recording stays local).
	if err := b.quarantineSet(context.Background(), bucket, key, versionID, string(incident.CauseCorruptShard), reason); err != nil {
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
	// The placement monitor may run on a NON-OWNER node; route the SET to the
	// owner so its RMW lock serializes it (re-verification + incident stay local).
	if err := b.quarantineSet(context.Background(), t.Bucket, t.ObjectKey, t.VersionID, string(incident.CauseCorruptShard), reason); err != nil {
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
//
// F5: a blob-resident appendable/coalesced object has no FSM obj: record — its
// manifest lives in the quorum-meta blob. On an FSM miss this falls back to the
// blob manifest (per-version blob by VersionID, then latest-only) so a blob-backed
// coalesced/segment shard is not falsely reported de-referenced.
func (b *DistributedBackend) ShardTargetStillReferenced(ctx context.Context, t ECShardScanTarget) bool {
	m, found, err := b.readShardTargetMeta(t)
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

// readShardTargetMeta resolves the parent object-version meta for a shard target
// from the off-raft quorum-meta blob. Returns (meta, found, err): found=false
// means the object-version genuinely does not exist (de-referenced).
func (b *DistributedBackend) readShardTargetMeta(t ECShardScanTarget) (objectMeta, bool, error) {
	// Prefer the specific per-version blob (matches the scanned version exactly);
	// a blob-backed appendable on a non-versioned bucket has no per-version blob,
	// so also try the latest-only blob whose VersionID matches the target.
	if b.shardSvc == nil {
		return objectMeta{}, false, nil
	}
	if t.VersionID != "" {
		if cmd, ok, verr := b.readQuorumMetaVersion(t.Bucket, t.ObjectKey, t.VersionID); verr == nil && ok {
			return buildPutObjectMeta(cmd), true, nil
		}
	}
	if cmd, lerr := b.readQuorumMetaCmd(t.Bucket, t.ObjectKey); lerr == nil {
		// Only accept the latest-only blob when it is the same version the scan
		// observed (or the scan carried no version) — never quarantine a different
		// version's shard via a stale latest blob.
		if t.VersionID == "" || cmd.VersionID == t.VersionID {
			return buildPutObjectMeta(cmd), true, nil
		}
	}
	return objectMeta{}, false, nil
}

func (b *DistributedBackend) isObjectQuarantined(bucket, key, versionID string) (bool, string, error) {
	var cmd PutObjectMetaCmd
	var err error
	if versionID != "" {
		// Prefer the per-version blob (versioned bucket); fall back to the
		// latest-only blob when the version matches (non-versioned bucket: the
		// quarantine RMW writes to the latest-only blob since there is only one
		// live version at a time).
		var found bool
		cmd, found, err = b.readQuorumMetaVersion(bucket, key, versionID)
		if err != nil {
			return false, "", fmt.Errorf("isObjectQuarantined: %w", err)
		}
		if !found {
			// Fall back to latest-only blob — valid for non-versioned buckets
			// where QuarantineObject writes to the latest-only blob.
			cmd, err = b.readQuorumMetaCmd(bucket, key)
			if errors.Is(err, storage.ErrObjectNotFound) {
				// Object has no quorum-meta blob yet; not quarantined.
				return false, "", nil
			}
			if err != nil {
				return false, "", fmt.Errorf("isObjectQuarantined: %w", err)
			}
			// Only honour the latest-only blob's quarantine flag when it
			// matches the queried version (prevents stale reads after a
			// re-upload that cleared the quarantine flag).
			if cmd.VersionID != versionID {
				return false, "", nil
			}
		}
	} else {
		cmd, err = b.readQuorumMetaCmd(bucket, key)
		if errors.Is(err, storage.ErrObjectNotFound) {
			// Object has no quorum-meta blob yet; not quarantined.
			return false, "", nil
		}
		if err != nil {
			return false, "", fmt.Errorf("isObjectQuarantined: %w", err)
		}
	}
	return cmd.IsQuarantined, cmd.QuarantineCause, nil
}

func objectQuarantinedError(bucket, key, cause string) error {
	return fmt.Errorf("%w: %s cause=%s", ErrObjectQuarantined, bucket+"/"+key, cause)
}
