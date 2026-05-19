package server

import "github.com/cloudwego/hertz/pkg/app/server"

func (s *Server) registerIcebergAPI(h *server.Hertz) {
	s.registerIcebergAPIAt(h, routePrefixIceberg)
	s.registerIcebergAPIAt(h, routePrefixIcebergAIStor)
}

func (s *Server) registerIcebergAPIAt(h *server.Hertz, prefix string) {
	h.GET(prefix+"v1/config", s.icebergAccessLog(s.icebergConfig))
	h.POST(prefix+"v1/warehouses", s.icebergAccessLog(s.icebergEnsureWarehouse))
	h.DELETE(prefix+"v1/warehouses/:warehouse", s.icebergAccessLog(s.icebergDeleteWarehouse))
	h.GET(prefix+"v1/namespaces", s.icebergAccessLog(s.icebergListNamespaces))
	h.POST(prefix+"v1/namespaces", s.icebergAccessLog(s.icebergCreateNamespace))
	h.GET(prefix+"v1/namespaces/:namespace", s.icebergAccessLog(s.icebergLoadNamespace))
	h.HEAD(prefix+"v1/namespaces/:namespace", s.icebergAccessLog(s.icebergHeadNamespace))
	h.DELETE(prefix+"v1/namespaces/:namespace", s.icebergAccessLog(s.icebergDeleteNamespace))
	h.GET(prefix+"v1/namespaces/:namespace/tables", s.icebergAccessLog(s.icebergListTables))
	h.POST(prefix+"v1/namespaces/:namespace/tables", s.icebergAccessLog(s.icebergCreateTable))
	h.GET(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergAccessLog(s.icebergLoadTable))
	h.HEAD(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergAccessLog(s.icebergHeadTable))
	h.POST(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergAccessLog(s.icebergCommitTable))
	h.DELETE(prefix+"v1/namespaces/:namespace/tables/:table", s.icebergAccessLog(s.icebergDeleteTable))
	h.POST(prefix+"v1/transactions/commit", s.icebergAccessLog(s.icebergCommitTransaction))
	h.Any(prefix+"*path", s.icebergAccessLog(s.icebergUnsupported))
}
