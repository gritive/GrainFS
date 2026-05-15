package server

import "github.com/cloudwego/hertz/pkg/app"

func (s *Server) serveReceiptByID(c *app.RequestContext, id string) {
	s.receiptAPI.ServeGetReceipt(newResponseWriter(c), toHTTPRequest(c), id)
}

func (s *Server) serveReceiptList(c *app.RequestContext) {
	s.receiptAPI.ServeListReceipts(newResponseWriter(c), toHTTPRequest(c))
}
