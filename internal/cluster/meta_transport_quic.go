package cluster

import (
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// MetaTransportQUIC is the QUIC-backed MetaTransport using StreamMetaRaft.
// It satisfies MetaTransport via *raft.MetaRaftQUICTransport method set.
type MetaTransportQUIC = raft.MetaRaftQUICTransport

// NewMetaTransportQUIC wires a MetaRaftQUICTransport into the shared QUIC
// transport. The returned value satisfies MetaTransport and should be passed
// to MetaRaft.SetTransport before calling Start.
func NewMetaTransportQUIC(tr *transport.QUICTransport, node *raft.Node) *MetaTransportQUIC {
	return raft.NewMetaRaftQUICTransport(tr, node)
}

// NewMetaTransportQUICMux is the mux-aware variant. groupMux must already
// exist; the constructor auto-registers node under the magic groupID
// "__meta__" so the receiver-side mux dispatch is wired up before
// EnableMux installs the accept handler. Pass nil groupMux to fall back to
// legacy-only behavior (equivalent to NewMetaTransportQUIC).
func NewMetaTransportQUICMux(tr *transport.QUICTransport, node *raft.Node, groupMux *raft.GroupRaftQUICMux) *MetaTransportQUIC {
	return raft.NewMetaRaftQUICTransportMux(tr, node, groupMux)
}
