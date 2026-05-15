package server

import "github.com/gritive/GrainFS/internal/icebergcatalog"

func (s *Server) icebergCatalogStore() icebergcatalog.Catalog {
	return s.icebergCatalog
}
