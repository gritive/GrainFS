# Context

## Domain Vocabulary

### Storage Operations Facade

The storage operations facade is the module that upper layers use for meaningful
storage actions instead of probing optional backend capabilities directly. It
owns storage side-effect ordering such as cache invalidation, WAL recording,
recovery write gating, and fallback behavior across decorated backends.

### Shard Group Peer Identity

Shard group peer identity is the node identifier stored in
`ShardGroupEntry.PeerIDs` for data Raft group membership. New shard group
metadata resolves known peer addresses to stable node IDs before writing group
membership; raft addresses are tolerated only as legacy/static aliases that
must be resolved at the cluster address book seam.
