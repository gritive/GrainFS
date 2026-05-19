package server

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) icebergListTables(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	tables, err := store.ListTables(ctx, catalogWarehouse(ctx, store.(warehouseProvider)), []string{c.Param("namespace")})
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"identifiers": tables})
}

func (s *Server) icebergCreateTable(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	wh := catalogWarehouse(ctx, store.(warehouseProvider))
	ns := []string{c.Param("namespace")}
	req, err := parseIcebergCreateTableRequest(c.Request.Body())
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid table request")
		return
	}
	ident := icebergcatalog.Identifier{Namespace: ns, Name: req.Name}
	// Build the S3 object path from the physical S3 URI prefix, not from the
	// logical warehouse key (wh). The warehouse key is FSM-only; using it
	// directly produces a non-s3:// location that parseS3Location rejects.
	s3Prefix := wh
	if p, ok := store.(s3URLPrefixProvider); ok {
		s3Prefix = p.S3URLPrefix()
	}
	location := icebergTableBasePath(s3Prefix, wh, ns[0], req.Name)
	metadataLocation := location + "/metadata/00000.json"
	metadata := buildInitialIcebergMetadata(location, req.Schema, req.Properties)
	if _, err := store.LoadTable(ctx, wh, ident); err == nil {
		writeIcebergMappedError(c, icebergcatalog.ErrTableExists)
		return
	} else if err != nil && !errors.Is(err, icebergcatalog.ErrTableNotFound) {
		writeIcebergMappedError(c, err)
		return
	}
	if err := s.writeIcebergMetadataObject(ctx, metadataLocation, metadata); err != nil {
		writeIcebergStorageError(c, err)
		return
	}
	tbl, err := store.CreateTable(ctx, wh, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: metadataLocation,
		Metadata:         metadata,
		Properties:       req.Properties,
	})
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	writeIcebergTable(c, tbl)
}

func (s *Server) icebergLoadTable(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	tbl, err := store.LoadTable(ctx, catalogWarehouse(ctx, store.(warehouseProvider)), icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")})
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	writeIcebergTable(c, tbl)
}

func (s *Server) icebergHeadTable(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if _, err := store.LoadTable(ctx, catalogWarehouse(ctx, store.(warehouseProvider)), icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergDeleteTable(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ident := icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}
	if err := store.DeleteTable(ctx, catalogWarehouse(ctx, store.(warehouseProvider)), ident); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}
