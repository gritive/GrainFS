package iceberg

import "github.com/gritive/GrainFS/internal/icebergcatalog"

func (h *Handler) icebergCatalogStore() icebergcatalog.Catalog {
	return h.catalog
}
