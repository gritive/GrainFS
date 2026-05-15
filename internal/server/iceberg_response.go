package server

import (
	"encoding/json"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

func writeIcebergTable(c *app.RequestContext, tbl *icebergcatalog.Table) {
	var metadata any
	if len(tbl.Metadata) > 0 {
		_ = json.Unmarshal(tbl.Metadata, &metadata)
	}
	c.JSON(consts.StatusOK, map[string]any{
		"metadata-location": tbl.MetadataLocation,
		"metadata":          metadata,
		"config":            map[string]string{},
	})
}

func writeIcebergMappedError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, icebergcatalog.ErrNamespaceNotFound):
		writeIcebergError(c, consts.StatusNotFound, "NoSuchNamespaceException", "namespace not found")
	case errors.Is(err, icebergcatalog.ErrNamespaceExists):
		writeIcebergError(c, consts.StatusConflict, "AlreadyExistsException", "namespace already exists")
	case errors.Is(err, icebergcatalog.ErrNamespaceNotEmpty):
		writeIcebergError(c, consts.StatusConflict, "NamespaceNotEmptyException", "namespace is not empty")
	case errors.Is(err, icebergcatalog.ErrTableNotFound):
		writeIcebergError(c, consts.StatusNotFound, "NoSuchTableException", "table not found")
	case errors.Is(err, icebergcatalog.ErrTableExists):
		writeIcebergError(c, consts.StatusConflict, "AlreadyExistsException", "table already exists")
	case errors.Is(err, icebergcatalog.ErrCommitFailed):
		writeIcebergError(c, consts.StatusConflict, "CommitFailedException", "table metadata pointer changed")
	case errors.Is(err, icebergcatalog.ErrServiceUnavailable):
		writeIcebergError(c, consts.StatusServiceUnavailable, "ServiceUnavailableException", "Iceberg catalog service unavailable")
	default:
		writeIcebergError(c, consts.StatusInternalServerError, "InternalServerError", err.Error())
	}
}

func writeIcebergStorageError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrNoSuchBucket):
		writeIcebergError(c, consts.StatusNotFound, "NoSuchBucketException", "warehouse bucket not found")
	default:
		writeIcebergError(c, consts.StatusInternalServerError, "InternalServerError", err.Error())
	}
}

func writeIcebergError(c *app.RequestContext, status int, typ, message string) {
	c.JSON(status, map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    typ,
			"code":    status,
		},
	})
}
