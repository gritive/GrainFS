package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) icebergCommitTable(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	var req struct {
		Requirements []json.RawMessage `json:"requirements"`
		Updates      []json.RawMessage `json:"updates"`
	}
	if err := decodeIcebergBody(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid table commit request")
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ident := icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}
	tbl, err := store.LoadTable(ctx, ident)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	tbl, ok = s.commitIcebergTableFrom(ctx, c, store, tbl, req.Requirements, req.Updates)
	if !ok {
		return
	}
	writeIcebergTable(c, tbl)
}

func (s *Server) icebergCommitTransaction(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	var req struct {
		TableChanges []struct {
			Identifier   icebergcatalog.Identifier `json:"identifier"`
			Requirements []json.RawMessage         `json:"requirements"`
			Updates      []json.RawMessage         `json:"updates"`
		} `json:"table-changes"`
	}
	if err := decodeIcebergBody(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid transaction commit request")
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	committed := make(map[string]*icebergcatalog.Table)
	for _, change := range req.TableChanges {
		key := icebergIdentifierKey(change.Identifier)
		tbl := committed[key]
		if tbl == nil {
			var err error
			tbl, err = store.LoadTable(ctx, change.Identifier)
			if err != nil {
				writeIcebergMappedError(c, err)
				return
			}
		}
		next, ok := s.commitIcebergTableFrom(ctx, c, store, tbl, change.Requirements, change.Updates)
		if !ok {
			return
		}
		committed[key] = next
	}
	c.JSON(consts.StatusOK, map[string]any{})
}
