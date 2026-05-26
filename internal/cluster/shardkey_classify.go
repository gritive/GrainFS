package cluster

import "strings"

// StartupRepairShardKind identifies the structural form of an EC shard key.
type StartupRepairShardKind int

const (
	// ShardKindObjectVersion is the standard form: "<objectKey>/<versionID>".
	ShardKindObjectVersion StartupRepairShardKind = iota
	// ShardKindSegment is the chunked-PUT segment form: "<objectKey>/segments/<blobID>".
	ShardKindSegment
	// ShardKindCoalesced is the coalesced form: "<objectKey>/coalesced/<coalescedID>".
	ShardKindCoalesced
)

// ClassifyStartupRepairShardKey splits a physical shard key into its object key,
// kind, and the kind-specific id (versionID, segment BlobID, or coalescedID).
//
// Matching priority:
//  1. If shardKey contains "/coalesced/", split at the FIRST occurrence.
//  2. Else if shardKey contains "/segments/", split at the FIRST occurrence.
//  3. Otherwise, split at the LAST "/" (object-version form): objectKey is the
//     part before and id (the versionID) is the part after. If there is no
//     "/", the whole key is the objectKey and id is "".
//
// Ambiguity note: an object key that literally contains "/segments/" or
// "/coalesced/" is inherently ambiguous because the WAL OpShardPut record
// carries no kind tag. Such keys will be misclassified. This is a documented
// scheme limitation; the misclassification results only in a benign skip
// downstream (the shard remains covered by read-time EC reconstruction).
func ClassifyStartupRepairShardKey(shardKey string) (objectKey string, kind StartupRepairShardKind, id string) {
	const coalescedMarker = "/coalesced/"
	const segmentMarker = "/segments/"

	if i := strings.Index(shardKey, coalescedMarker); i >= 0 {
		return shardKey[:i], ShardKindCoalesced, shardKey[i+len(coalescedMarker):]
	}
	if i := strings.Index(shardKey, segmentMarker); i >= 0 {
		return shardKey[:i], ShardKindSegment, shardKey[i+len(segmentMarker):]
	}

	// Object-version: split at the last "/".
	objectKey, id = shardKey, ""
	if i := strings.LastIndexByte(shardKey, '/'); i >= 0 {
		objectKey, id = shardKey[:i], shardKey[i+1:]
	}
	return objectKey, ShardKindObjectVersion, id
}
