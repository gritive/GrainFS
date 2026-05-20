package server

import (
	"encoding/json"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"

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
		// Message intentionally embeds the literal "409 Conflict" so
		// client-side retry matchers that grep the err.Error() string —
		// e.g., warp 1.5 pkg/iceberg/commit.go IsConflictError, which
		// substring-checks "409" / "Conflict" — recognize the response.
		// iceberg-go's REST client otherwise emits
		// "CommitFailedException: table metadata pointer changed" with
		// no 4xx hint in the string, defeating those matchers.
		writeIcebergError(c, consts.StatusConflict, "CommitFailedException", "409 Conflict: table metadata pointer changed")
	case errors.Is(err, icebergcatalog.ErrServiceUnavailable):
		// 503 from the iceberg catalog is rare and structurally important —
		// surface the full wrapped error in both the log AND the response
		// body so investigators can tell empty-peers from all-peers-failed
		// from a stray leader-side error type. The wrapped message is
		// already safe to expose (no PII, no internal addresses); the
		// outer error sentinel guarantees stable type matching for
		// clients that need it.
		log.Warn().
			Str("component", "iceberg").
			Err(err).
			Msg("iceberg: returning 503 ServiceUnavailable")
		writeIcebergError(c, consts.StatusServiceUnavailable, "ServiceUnavailableException", err.Error())
	default:
		log.Warn().
			Str("component", "iceberg").
			Err(err).
			Msg("iceberg: returning 500 InternalServerError")
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
	body := map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    typ,
			"code":    status,
		},
	}
	if rid := requestIDFromHertz(c); rid != "" {
		body["request_id"] = rid
	}
	c.JSON(status, body)
}
