package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// capabilitiesStatus serves GET /v1/cluster/capabilities on the admin UDS.
//
// Body: {"peers": {"<node-id>": {"<capability>": <ready>, ...}, ...}}
//
// Operators (and the bench warmup probe) poll this until every voter has
// gossiped the capabilities they need before sending traffic the gate would
// otherwise reject as "rolling upgrade". Anonymous on admin UDS — gated by
// the socket's filesystem permissions, like the rest of /v1/cluster/*.
func (s *Server) capabilitiesStatus(_ context.Context, c *app.RequestContext) {
	peers := map[string]map[string]bool{}
	if s.cluster != nil {
		peers = s.cluster.CapabilityEvidence()
	}
	data, _ := json.Marshal(struct {
		Peers map[string]map[string]bool `json:"peers"`
	}{Peers: peers})
	c.Data(consts.StatusOK, "application/json", data)
}
