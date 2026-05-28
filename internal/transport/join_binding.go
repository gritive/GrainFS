package transport

import "crypto/tls"

// JoinBindingLabel is the RFC 5705 exporter label binding a Zero-CA invite
// transcript to the join-listener's TLS session. Joiner and leader MUST use
// the identical label/context/length or the derived bytes differ and the
// signatures fail.
const JoinBindingLabel = "grainfs-join-binding"

// JoinBindingLen is the exporter output length in bytes. Exported so the leader
// (internal/cluster gateInvite) can enforce the EXACT length on the bind it is
// handed — a non-empty-but-wrong-length bind from a direct/fake caller of the
// internal HandleJoin API must be rejected, not just an empty one.
const JoinBindingLen = 32

// ExportJoinBinding derives the channel-binding value for one join-listener TLS
// session. context is nil by agreement (the transcript already carries the
// identity fields).
func ExportJoinBinding(state tls.ConnectionState) ([]byte, error) {
	return state.ExportKeyingMaterial(JoinBindingLabel, nil, JoinBindingLen)
}
