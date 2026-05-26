package cluster

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
		NodeIDs:          cmd.NodeIDs,
		PlacementGroupID: cmd.PlacementGroupID,
		UserMetadata:     cmd.UserMetadata,
		SSEAlgorithm:     cmd.SSEAlgorithm,
		Parts:            cmd.Parts,
		Segments:         segmentMetaEntriesToRefs(cmd.Segments),
		Tags:             cmd.Tags,
	}
}
