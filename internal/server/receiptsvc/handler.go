package receiptsvc

import (
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/receipt"
)

// Deps carries what the receipt HTTP handlers need from core, as closures + the
// receipt.API backing, so this package never imports `server`.
type Deps struct {
	API              *receipt.API
	FeatureAvailable func() bool
	NewRespWriter    func(*app.RequestContext) http.ResponseWriter
	ToHTTPRequest    func(*app.RequestContext) *http.Request
}

type Handler struct {
	deps Deps
}

func NewHandler(d Deps) *Handler { return &Handler{deps: d} }
