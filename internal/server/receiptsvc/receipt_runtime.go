package receiptsvc

import "github.com/cloudwego/hertz/pkg/app"

func (h *Handler) serveReceiptByID(c *app.RequestContext, id string) {
	h.deps.API.ServeGetReceipt(h.deps.NewRespWriter(c), h.deps.ToHTTPRequest(c), id)
}

func (h *Handler) serveReceiptList(c *app.RequestContext) {
	h.deps.API.ServeListReceipts(h.deps.NewRespWriter(c), h.deps.ToHTTPRequest(c))
}
