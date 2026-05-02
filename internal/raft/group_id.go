package raft

import (
	"errors"
	"fmt"
	"strings"
)

// metaGroupID is the reserved groupID used by meta-raft on the shared mux
// transport. It is wire-visible: receivers branch on it in handleMuxRequest
// and dispatchToLocalGroup. User-supplied shard group IDs MUST NOT collide;
// validateGroupID is called at every entry point that accepts a groupID.
const metaGroupID = "__meta__"

// ErrReservedGroupID is returned by validateGroupID when the caller supplies
// a name that would collide with internal routing.
var ErrReservedGroupID = errors.New("groupID uses reserved namespace")

// ValidateGroupID rejects empty, reserved-prefix ("__"), or system-reserved
// names. Callers: meta_fsm.applyPutShardGroup, group_lifecycle.instantiateLocalGroup,
// GroupRaftQUICMux.Register, MetaRaft.ProposeShardGroup. The "__" prefix is
// reserved so future internal IDs don't break wire compatibility.
func ValidateGroupID(id string) error {
	if id == "" {
		return errors.New("groupID empty")
	}
	if id == metaGroupID {
		return fmt.Errorf("groupID %q: %w (meta-raft only)", id, ErrReservedGroupID)
	}
	if strings.HasPrefix(id, "__") {
		return fmt.Errorf("groupID %q: %w (\"__\" prefix reserved)", id, ErrReservedGroupID)
	}
	return nil
}
