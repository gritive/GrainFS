package admin

func registerAudit(g router, d *Deps) {
	if d == nil || d.AuditQuery == nil {
		return
	}
	g.POST(routePathAuditQuery, auditQueryHandler(d))
	g.GET(routePathAuditRecentDenies, auditRecentDeniesHandler(d))
	g.GET(routePathAuditBySA, auditBySAHandler(d))
	g.GET(routePathAuditByRequestID, auditByRequestIDHandler(d))
}
