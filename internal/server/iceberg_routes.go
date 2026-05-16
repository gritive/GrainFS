package server

import "github.com/cloudwego/hertz/pkg/app/server"

func (s *Server) registerIcebergAPI(h *server.Hertz) {
	s.registerIcebergAPIAt(h, routePrefixIceberg)
	s.registerIcebergAPIAt(h, routePrefixIcebergAIStor)
}

func (s *Server) registerIcebergAPIAt(h *server.Hertz, prefix string) {
	h.GET(prefix+"v1/config", s.icebergConfig)
	h.POST(prefix+"v1/warehouses", s.icebergEnsureWarehouse)
	h.DELETE(prefix+"v1/warehouses/:warehouse", s.icebergDeleteWarehouse)
	h.GET(prefix+"v1/namespaces", s.icebergListNamespaces)
	h.POST(prefix+"v1/namespaces", s.icebergCreateNamespace)
	h.GET(prefix+"v1/namespaces/:namespace", s.icebergLoadNamespace)
	h.HEAD(prefix+"v1/namespaces/:namespace", s.icebergHeadNamespace)
	h.DELETE(prefix+"v1/namespaces/:namespace", s.icebergDeleteNamespace)
	h.GET(prefix+"v1/namespaces/:namespace/tables", s.icebergListTables)
	h.POST(prefix+"v1/namespaces/:namespace/tables", s.icebergCreateTable)
	h.GET(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergLoadTable)
	h.HEAD(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergHeadTable)
	h.POST(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergCommitTable)
	h.DELETE(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergDeleteTable)
	h.POST(prefix+"v1/transactions/commit", s.icebergCommitTransaction)
	h.Any(prefix+"*path", s.icebergUnsupported)
}
