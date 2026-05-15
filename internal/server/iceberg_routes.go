package server

import "github.com/cloudwego/hertz/pkg/app/server"

func (s *Server) registerIcebergAPI(h *server.Hertz) {
	h.GET(routePathIcebergConfig, s.icebergConfig)
	h.GET(routePathIcebergNamespaces, s.icebergListNamespaces)
	h.POST(routePathIcebergNamespaces, s.icebergCreateNamespace)
	h.GET(routePathIcebergNamespace, s.icebergLoadNamespace)
	h.HEAD(routePathIcebergNamespace, s.icebergHeadNamespace)
	h.DELETE(routePathIcebergNamespace, s.icebergDeleteNamespace)
	h.GET(routePathIcebergNamespaceTables, s.icebergListTables)
	h.POST(routePathIcebergNamespaceTables, s.icebergCreateTable)
	h.GET(routePathIcebergTable, s.icebergLoadTable)
	h.HEAD(routePathIcebergTable, s.icebergHeadTable)
	h.POST(routePathIcebergTable, s.icebergCommitTable)
	h.DELETE(routePathIcebergTable, s.icebergDeleteTable)
	h.POST(routePathIcebergTransactionCommit, s.icebergCommitTransaction)
	h.Any(routePathIcebergUnsupported, s.icebergUnsupported)
}
