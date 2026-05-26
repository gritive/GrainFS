package chunkref

import "strconv"

// objectKeySep separates the (bucket, key, versionID) components of an
// object-version ManifestID. It matches the cluster meta-FSM object-index key
// convention (objectIndexVersionKey, internal/cluster/meta_fsm_placement.go),
// so single and cluster compose the same cluster-global identity.
const objectKeySep = "\x00"

// ObjectVersionID composes the cluster-global ManifestID for an object version.
// The raw S3 versionID alone is NOT unique across different (bucket,key); the
// composed "bucket\x00key\x00versionID" string is. versionID may be empty for
// non-versioned buckets — the composition stays unique per (bucket,key).
func ObjectVersionID(bucket, key, versionID string) ManifestID {
	return ManifestID{
		Domain:    DomainObjectVersion,
		VersionID: bucket + objectKeySep + key + objectKeySep + versionID,
	}
}

// SnapshotID composes the ManifestID for a snapshot descriptor identified by its
// monotonic sequence number (snapshot.Snapshot.Seq).
func SnapshotID(seq uint64) ManifestID {
	return ManifestID{
		Domain:    DomainSnapshot,
		VersionID: strconv.FormatUint(seq, 10),
	}
}
