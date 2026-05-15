package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) commitIcebergTableFrom(ctx context.Context, c *app.RequestContext, store icebergcatalog.Catalog, tbl *icebergcatalog.Table, requirements, updates []json.RawMessage) (*icebergcatalog.Table, bool) {
	ident := tbl.Identifier
	if err := validateIcebergRequirements(tbl.Metadata, requirements); err != nil {
		writeIcebergMappedError(c, err)
		return nil, false
	}
	metadata, err := applyIcebergUpdates(tbl.Metadata, updates)
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid transaction updates")
		return nil, false
	}
	nextMetadataLocation := nextIcebergMetadataLocation(tbl.MetadataLocation)
	if err := s.writeIcebergMetadataObject(ctx, nextMetadataLocation, metadata); err != nil {
		writeIcebergStorageError(c, err)
		return nil, false
	}
	committed, err := store.CommitTable(ctx, ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: tbl.MetadataLocation,
		NewMetadataLocation:      nextMetadataLocation,
		Metadata:                 metadata,
	})
	if err != nil {
		writeIcebergMappedError(c, err)
		return nil, false
	}
	return committed, true
}

func icebergIdentifierKey(ident icebergcatalog.Identifier) string {
	return strings.Join(ident.Namespace, "\x00") + "\x00" + ident.Name
}
