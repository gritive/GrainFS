package cluster

import (
	"context"
	"fmt"
)

// ECRewrapTarget identifies a single EC-distributed shard that a DEK rewrap
// lane must migrate. ShardKey is the canonical shard-service key; NodeIDs is
// positional: shard i lives on NodeIDs[i].
type ECRewrapTarget struct {
	Bucket   string
	ShardKey string
	NodeIDs  []string
}

// CollectECRewrapTargets enumerates every EC shard across ALL stored object
// versions (including non-latest and legacy unversioned) in this backend's data
// group and returns them as a flat slice of ECRewrapTarget. It replaces the
// old ScanGroupObjects+OwnedShards+ScanGroupSegmentShards triple so the DEK
// rewrap lane covers non-latest versions that the lat:-only scan missed.
//
// Validation is split by kind: object-version refs (which buildECShardTargets
// emits unconditionally) are filtered here — owner-local (ECData==0) and
// malformed refs are skipped. Segment/coalesced refs are already validated
// (and dropped on failure) inside buildECShardTargets, so they arrive clean.
func (b *DistributedBackend) CollectECRewrapTargets() ([]ECRewrapTarget, error) {
	var out []ECRewrapTarget
	appendTarget := func(t ECShardScanTarget) error {
		switch t.Kind {
		case ECShardObjectVersion:
			if !validateECRefPlacement(t.ECData, t.ECParity, t.NodeIDs) {
				return nil // owner-local or malformed — skip
			}
			out = append(out, ECRewrapTarget{
				Bucket:   t.Bucket,
				ShardKey: ecObjectShardKey(t.ObjectKey, t.VersionID),
				NodeIDs:  t.NodeIDs,
			})
		default: // ECShardSegment / ECShardCoalesced — ShardKey + Placement already set + validated
			out = append(out, ECRewrapTarget{
				Bucket:   t.Bucket,
				ShardKey: t.ShardKey,
				NodeIDs:  t.Placement.Nodes,
			})
		}
		return nil
	}

	// FSM scan: carve-outs (appendable/coalesced) + internal-bucket records. (Plain
	// versioned and non-versioned objects no longer write FSM obj: records — they are
	// blob-only; the per-version blob enum below covers versioned, the latest-only
	// blob enum covers non-versioned/Suspended.)
	if err := b.fsm.IterECShardScanTargetsAllVersions(appendTarget); err != nil {
		return nil, err
	}

	// Per-version blob enum (versioning-enabled buckets): the per-version blob is the
	// authority for versioned objects once their FSM obj: records are removed, so DEK
	// rewrap MUST enumerate from blobs or it would silently skip every versioned
	// object's shards — leaving them encrypted under a retired KEK generation. It is
	// cluster-wide and FAIL-CLOSED (scanQuorumMetaVersionsClusterAll): a missed peer
	// aborts the sweep rather than under-enumerating (a node that holds a shard but
	// not the K-of-N blob still gets its shard covered via the peer's blob). Reuses
	// buildECShardTargets on the decoded blob, yielding identical object-version +
	// segment shard targets; the lane rewraps only the shards local to each node.
	buckets, err := b.ListBuckets(context.Background())
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		on, serr := b.blobAuthReadOn(bucket)
		if serr != nil {
			return nil, serr
		}
		// versioning-Enabled → per-version blob tree; non-versioned/Suspended →
		// latest-only blob tree. Both cluster-wide + fail-closed so a parity node
		// that missed the K-of-N blob still gets its shard covered via a peer's blob.
		var cmds []PutObjectMetaCmd
		var cerr error
		if on {
			cmds, cerr = b.scanQuorumMetaVersionsClusterAll(bucket, "")
		} else {
			cmds, cerr = b.scanQuorumMetaClusterAll(bucket)
		}
		if cerr != nil {
			return nil, fmt.Errorf("ec rewrap blob enum %s: %w", bucket, cerr)
		}
		for _, cmd := range cmds {
			if cmd.IsHardDeleted || cmd.IsDeleteMarker {
				continue // tombstone / delete marker — no live shards to rewrap
			}
			meta := buildPutObjectMeta(cmd)
			ref := ObjectMetaRef{Bucket: bucket, Key: cmd.Key, VersionID: cmd.VersionID}
			if err := b.fsm.buildECShardTargets(ref, meta, appendTarget); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
