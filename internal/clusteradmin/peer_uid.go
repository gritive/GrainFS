package clusteradmin

import (
	"net/http"

	"github.com/gritive/GrainFS/internal/server/admin"
)

// PeerCred is the resolved (or unresolved) peer credentials of an admin UDS
// caller. Audit log entries carry both UID and Resolved so analysts can
// distinguish "uid=0 because root" from "uid=0 because unresolvable".
//
// Authorization is NOT based on this; admin UDS authorization remains group
// ownership (0660 + chown :admin). PeerCred is purely forensic enrichment.
type PeerCred struct {
	UID      uint32
	Resolved bool
}

// peerUIDFrom extracts the caller's peer credentials from an admin UDS
// request. The credentials are plumbed onto r.Context() by the admin
// package's peerCredMiddleware (installed via RegisterAdmin) which reads
// SO_PEERCRED (linux) / LOCAL_PEERCRED (darwin) at Accept time.
//
// Returns {UID:0, Resolved:false} when:
//   - The OS is not linux/darwin (peercred_other.go returns errPeerCredUnsupported)
//   - The middleware did not run on this request (e.g., non-admin Hertz path)
//   - SO_PEERCRED returned an error at Accept time
//
// Best-effort forensic enrichment; functional correctness does not depend
// on resolution.
func peerUIDFrom(r *http.Request) PeerCred {
	if v, ok := admin.PeerCredFromContext(r.Context()); ok {
		return PeerCred{UID: v.UID, Resolved: v.Resolved}
	}
	return PeerCred{}
}
