package cluster

import (
	"errors"
)

// ErrPutObjectMetaCAS is returned (wrapped) when a PutObjectMeta command carries
// an ExpectedETag that no longer matches the current object's ETag. Callers that
// perform a compare-and-swap meta write (e.g. relocation) use errors.Is to map
// this to a benign "object changed" skip rather than a hard failure.
var ErrPutObjectMetaCAS = errors.New("put object meta CAS: etag mismatch")

func buildPutObjectMeta(cmd PutObjectMetaCmd) objectMeta {
	etag := cmd.ETag
	if cmd.IsDeleteMarker {
		etag = deleteMarkerETag
	}
	return objectMeta{
		Key:              cmd.Key,
		Size:             cmd.Size,
		ContentType:      cmd.ContentType,
		ETag:             etag,
		LastModified:     cmd.ModTime,
		ECData:           cmd.ECData,
		ECParity:         cmd.ECParity,
		StripeBytes:      cmd.StripeBytes,
		NodeIDs:          cmd.NodeIDs,
		PlacementGroupID: cmd.PlacementGroupID,
		UserMetadata:     cmd.UserMetadata,
		SSEAlgorithm:     cmd.SSEAlgorithm,
		Parts:            cmd.Parts,
		Segments:         segmentMetaEntriesToRefs(cmd.Segments),
		Coalesced:        cmd.Coalesced,
		IsAppendable:     cmd.IsAppendable,
		Tags:             cmd.Tags,
		ACL:              cmd.ACL,
		MetaSeq:          cmd.MetaSeq,
	}
}
