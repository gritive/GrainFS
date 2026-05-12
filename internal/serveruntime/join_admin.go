package serveruntime

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// clusterNodes is a minimal interface for querying the cluster membership.
// Satisfied by *cluster.MetaRaft in production; a stub in tests.
type clusterNodes interface {
	Nodes() []cluster.MetaNodeEntry
}

// soloDataChecker reports whether the local FSM holds any user-created data.
// Satisfied by *cluster.MetaFSM in production; a stub in tests.
type soloDataChecker interface {
	HasUserData() bool
}

// JoinHandler handles POST /v1/cluster/join requests from the `grainfs join`
// CLI. If the node is already a multi-node cluster member the request is a
// no-op. If solo, it writes the .join-pending file and triggers a graceful
// server restart so the next boot performs the actual cluster join.
type JoinHandler struct {
	dataDir     string
	raftAddr    string
	cancel      context.CancelFunc
	nodes       clusterNodes
	dataChecker soloDataChecker // nil = data guard disabled
}

// JoinRequest is the body for POST /v1/cluster/join.
type JoinRequest struct {
	PeerAddr string `json:"peer_addr"`
	Force    bool   `json:"force"`
}

// JoinResponse is the response body for POST /v1/cluster/join.
type JoinResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (h *JoinHandler) Handle(ctx context.Context, c *app.RequestContext) {
	var req JoinRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(400, JoinResponse{Status: "error", Message: "invalid JSON body"})
		return
	}
	req.PeerAddr = strings.TrimSpace(req.PeerAddr)
	if req.PeerAddr == "" {
		c.JSON(400, JoinResponse{Status: "error", Message: "peer_addr is required"})
		return
	}
	if _, _, err := net.SplitHostPort(req.PeerAddr); err != nil {
		c.JSON(400, JoinResponse{Status: "error", Message: "peer_addr must be host:port"})
		return
	}

	// Already a multi-node cluster member → no-op.
	// len(nodes) > 1 is the voter-count guard: MetaRaft only commits a node entry
	// to the FSM after the Raft config-change is accepted, so this check also
	// covers "follower accepted but not yet leader-forwarded" races.
	nodes := h.nodes.Nodes()
	if len(nodes) > 1 {
		c.JSON(200, JoinResponse{Status: "already_member", Message: "node is already part of a multi-node cluster"})
		return
	}

	// Peer is self → no-op.
	if h.isSelf(req.PeerAddr) {
		c.JSON(200, JoinResponse{Status: "self", Message: "peer resolves to this node; already bootstrapped solo"})
		return
	}

	// Data guard: refuse join when the solo node holds user data and Force is
	// not set, to prevent accidental data loss.
	if !req.Force && h.dataChecker != nil && h.dataChecker.HasUserData() {
		c.JSON(409, JoinResponse{
			Status:  "data_present",
			Message: "solo node has user data; re-send with force=true to discard it and join",
		})
		return
	}

	// Write .join-pending and trigger graceful restart.
	pendingFile := filepath.Join(h.dataDir, JoinPendingFile)
	if err := os.WriteFile(pendingFile, []byte(req.PeerAddr), 0o600); err != nil {
		c.JSON(500, JoinResponse{Status: "error", Message: "write join-pending: " + err.Error()})
		return
	}
	log.Info().Str("peer", req.PeerAddr).Msg("join requested — restarting to join cluster")
	c.JSON(200, JoinResponse{Status: "restart_initiated", Message: "node will restart and join " + req.PeerAddr})

	// Trigger graceful shutdown after response is flushed.
	go func() {
		time.Sleep(150 * time.Millisecond)
		h.cancel()
	}()
}

// isSelf returns true when peerAddr resolves to one of this node's own IPs on
// the same port as raftAddr, or equals raftAddr directly.
func (h *JoinHandler) isSelf(peerAddr string) bool {
	if peerAddr == h.raftAddr {
		return true
	}
	peerHost, peerPort, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return false
	}
	_, selfPort, err := net.SplitHostPort(h.raftAddr)
	if err != nil {
		return false
	}
	if peerPort != selfPort {
		return false
	}

	peerIPs, _ := net.LookupHost(peerHost)

	selfHost, _, _ := net.SplitHostPort(h.raftAddr)
	selfIPs, _ := net.LookupHost(selfHost)
	for _, pip := range peerIPs {
		for _, sip := range selfIPs {
			if pip == sip {
				return true
			}
		}
	}

	// Also check all local interface addresses (handles 0.0.0.0 bind).
	ifaces, _ := net.InterfaceAddrs()
	for _, pip := range peerIPs {
		for _, iface := range ifaces {
			var ip net.IP
			switch v := iface.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && ip.String() == pip {
				return true
			}
		}
	}
	return false
}
