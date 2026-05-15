package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) requireIceberg(c *app.RequestContext) (icebergcatalog.Catalog, bool) {
	if !s.routeFeatureAvailable(routeFeatureIceberg) {
		writeIcebergError(c, consts.StatusNotImplemented, "NotImplementedException", "Iceberg REST Catalog is not available: server started without --audit-iceberg or catalog initialization failed")
		return nil, false
	}
	return s.icebergCatalogStore(), true
}

func (s *Server) icebergConfig(_ context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	c.JSON(consts.StatusOK, map[string]map[string]string{
		"defaults":  {"warehouse": store.Warehouse()},
		"overrides": {},
	})
}

func (s *Server) icebergListNamespaces(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	namespaces, err := store.ListNamespaces(ctx)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespaces": namespaces})
}

func (s *Server) icebergCreateNamespace(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	req, err := parseIcebergCreateNamespaceRequest(c.Request.Body())
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid namespace request")
		return
	}
	if err := store.CreateNamespace(ctx, req.Namespace, req.Properties); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespace": req.Namespace, "properties": nonNilMap(req.Properties)})
}

func (s *Server) icebergLoadNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ns := []string{c.Param("namespace")}
	props, err := store.LoadNamespace(ctx, ns)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespace": ns, "properties": props})
}

func (s *Server) icebergHeadNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if _, err := store.LoadNamespace(ctx, []string{c.Param("namespace")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergDeleteNamespace(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if err := store.DeleteNamespace(ctx, []string{c.Param("namespace")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergUnsupported(_ context.Context, c *app.RequestContext) {
	writeIcebergError(c, consts.StatusNotImplemented, "NotImplementedException", "unsupported Iceberg REST Catalog operation")
}
