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
