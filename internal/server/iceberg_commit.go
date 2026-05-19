package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

// maxIcebergUnconditionalCommitRetries bounds the server-side retry loop for
// CommitTable on ErrCommitFailed when the client sent no requirements (an
// unconditional commit). Real Iceberg clients retry on 409, but several
// benchmark tools (warp 1.5 isRetryable matches the literal string "Conflict",
// which iceberg-go's REST client does not emit — it returns
// "CommitFailedException: ..." instead) do not. Without an internal retry,
// every collision-driven 409 from concurrent commits with empty requirements
// flows out to the client as a permanent error even though the Iceberg
// spec permits the server to apply the update.
const maxIcebergUnconditionalCommitRetries = 5

// commitIcebergTableFrom runs a single iceberg commit attempt against the
// supplied table snapshot and writes the response to c on terminal outcomes
// (success or non-retryable error).
//
// Return values:
//   - (table, true,  nil): commit applied; response already written.
//   - (nil,   false, ErrCommitFailed): the commit raced; the caller may reload
//     tbl and try again. Response NOT written — the caller decides whether to
//     retry or surface the 409.
//   - (nil,   false, other error): non-retryable failure; response already
//     written (validation, storage, or any non-race propose error).
func (s *Server) commitIcebergTableFrom(ctx context.Context, c *app.RequestContext, store icebergcatalog.Catalog, tbl *icebergcatalog.Table, requirements, updates []json.RawMessage) (*icebergcatalog.Table, bool, error) {
	ident := tbl.Identifier
	if err := validateIcebergRequirements(tbl.Metadata, requirements); err != nil {
		writeIcebergMappedError(c, err)
		return nil, false, fmt.Errorf("validate requirements: %w", err)
	}
	metadata, err := applyIcebergUpdates(tbl.Metadata, updates)
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid transaction updates")
		return nil, false, fmt.Errorf("apply updates: %w", err)
	}
	nextMetadataLocation := nextIcebergMetadataLocation(tbl.MetadataLocation)
	if err := s.writeIcebergMetadataObject(ctx, nextMetadataLocation, metadata); err != nil {
		writeIcebergStorageError(c, err)
		return nil, false, fmt.Errorf("write metadata object: %w", err)
	}
	committed, err := store.CommitTable(ctx, catalogWarehouse(ctx, store.(warehouseProvider)), ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: tbl.MetadataLocation,
		NewMetadataLocation:      nextMetadataLocation,
		Metadata:                 metadata,
	})
	if err != nil {
		// Race on the optimistic-concurrency check: surface so the caller
		// can decide to retry against fresh state (only safe when the
		// client sent no requirements). Do NOT write the response yet.
		if errors.Is(err, icebergcatalog.ErrCommitFailed) {
			return nil, false, err
		}
		writeIcebergMappedError(c, err)
		return nil, false, err
	}
	return committed, true, nil
}

func icebergIdentifierKey(ident icebergcatalog.Identifier) string {
	return strings.Join(ident.Namespace, "\x00") + "\x00" + ident.Name
}
