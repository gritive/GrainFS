package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) icebergListTables(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	tables, err := store.ListTables(ctx, []string{c.Param("namespace")})
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
	ns := []string{c.Param("namespace")}
	req, err := parseIcebergCreateTableRequest(c.Request.Body())
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid table request")
		return
	}
	ident := icebergcatalog.Identifier{Namespace: ns, Name: req.Name}
	location := fmt.Sprintf("%s/%s/%s", store.Warehouse(), ns[0], req.Name)
	metadataLocation := location + "/metadata/00000.json"
	metadata := buildInitialIcebergMetadata(location, req.Schema, req.Properties)
	if _, err := store.LoadTable(ctx, ident); err == nil {
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
	tbl, err := store.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
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
	tbl, err := store.LoadTable(ctx, icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")})
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
	if _, err := store.LoadTable(ctx, icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}); err != nil {
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
	if err := store.DeleteTable(ctx, ident); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}
