package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// IndexGroupEntry describes sharded object index Raft group membership.
// It is a SEPARATE registry from ShardGroupEntry (data groups): index groups
// never appear in ShardGroups() and data groups never appear in IndexGroups().
type IndexGroupEntry struct {
	ID      string
	PeerIDs []string
}

// applyPutIndexGroup is a LEAF apply method: decode the inner MetaPutIndexGroupCmd
// payload and upsert it into f.indexGroups. No post-commit hooks, no DB write,
// no peer normalization (mirrors applyPutShardGroup's mutation core, minus the
// data-group callbacks).
func (f *MetaFSM) applyPutIndexGroup(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutIndexGroup: empty payload")
	}
	var (
		c      *clusterpb.MetaPutIndexGroupCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutIndexGroupCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutIndexGroupCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	ig := c.Group(nil)
	if ig == nil {
		return fmt.Errorf("meta_fsm: PutIndexGroup: nil group")
	}
	peers := make([]string, ig.PeerIdsLength())
	for i := 0; i < ig.PeerIdsLength(); i++ {
		peers[i] = string(ig.PeerIds(i))
	}
	entry := IndexGroupEntry{
		ID:      string(ig.Id()),
		PeerIDs: peers,
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: PutIndexGroup: empty group ID")
	}
	if len(peers) == 0 {
		return fmt.Errorf("meta_fsm: PutIndexGroup: group %q has no peers", entry.ID)
	}
	f.mu.Lock()
	f.indexGroups[entry.ID] = entry
	cb := f.onIndexGroupAdded
	f.mu.Unlock()
	if cb != nil {
		// Defensive copy of peers — callback may keep references.
		cb(IndexGroupEntry{ID: entry.ID, PeerIDs: cloneStringSlice(entry.PeerIDs)})
	}
	return nil
}

// IndexGroups returns a deep copy of the current index groups, sorted by ID.
// PeerIDs slices are copied so callers cannot mutate FSM state.
func (f *MetaFSM) IndexGroups() []IndexGroupEntry {
	f.mu.RLock()
	out := make([]IndexGroupEntry, 0, len(f.indexGroups))
	for _, ig := range f.indexGroups {
		out = append(out, IndexGroupEntry{ID: ig.ID, PeerIDs: cloneStringSlice(ig.PeerIDs)})
	}
	f.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// encodeMetaIndexGroupCmd builds the inner MetaPutIndexGroupCmd and wraps it in a
// full PutIndexGroup MetaCmd, returning the complete command byte slice ready for
// ProposeWait. (Asymmetric with encodeMetaPutShardGroupCmd, which returns only the
// inner payload — the index path keeps encode and extract symmetric.)
func encodeMetaIndexGroupCmd(ig IndexGroupEntry) ([]byte, error) {
	payload, err := encodeMetaPutIndexGroupPayload(ig)
	if err != nil {
		return nil, err
	}
	return encodeMetaCmd(MetaCmdTypePutIndexGroup, payload)
}

// encodeMetaPutIndexGroupPayload builds the inner MetaPutIndexGroupCmd flatbuffer.
func encodeMetaPutIndexGroupPayload(ig IndexGroupEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()

	idOff := b.CreateString(ig.ID)
	peerOffs := make([]flatbuffers.UOffsetT, len(ig.PeerIDs))
	for i := len(ig.PeerIDs) - 1; i >= 0; i-- {
		peerOffs[i] = b.CreateString(ig.PeerIDs[i])
	}
	clusterpb.IndexGroupEntryStartPeerIdsVector(b, len(peerOffs))
	for i := len(peerOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(peerOffs[i])
	}
	peerVec := b.EndVector(len(peerOffs))

	clusterpb.IndexGroupEntryStart(b)
	clusterpb.IndexGroupEntryAddId(b, idOff)
	clusterpb.IndexGroupEntryAddPeerIds(b, peerVec)
	igOff := clusterpb.IndexGroupEntryEnd(b)

	clusterpb.MetaPutIndexGroupCmdStart(b)
	clusterpb.MetaPutIndexGroupCmdAddGroup(b, igOff)
	return fbFinish(b, clusterpb.MetaPutIndexGroupCmdEnd(b)), nil
}

// extractIndexGroupPayload decodes a full MetaCmd byte slice and returns the inner
// MetaPutIndexGroupCmd payload (cmd.DataBytes()) for applyPutIndexGroup.
//
//nolint:unused // referenced by index_group_registry_test.go.
func extractIndexGroupPayload(data []byte) []byte {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	return cmd.DataBytes()
}
