# Context

## Domain Vocabulary

### Storage Operations Facade

The storage operations facade is the module that upper layers use for meaningful
storage actions instead of probing optional backend capabilities directly. It
owns storage side-effect ordering such as cache invalidation, WAL recording,
recovery write gating, and fallback behavior across decorated backends.

### Mutation Result

A mutation result is the storage-facing outcome of a write operation, including
the new object state and any previous-object facts needed for metrics,
invalidation, or protocol responses. Mutation result methods belong on the
storage operations facade so callers do not perform their own pre-mutation
bookkeeping around decorated backends.

The new object state is exposed as object facts, not as a storage object handle:
size, ETag, version identity, and last-modified time. Result normalization
rejects nil objects, negative sizes, and empty ETags for newly written objects
so handlers do not emit responses or metrics from invalid mutation metadata.

Previous-object facts are a summary, not a leaked historical object. They state
whether a previous object existed and the stable fields needed for accounting,
such as size, ETag, and version identity.

The facade reads previous-object facts immediately before the mutation inside
the same operation boundary. This keeps bookkeeping reads with the storage
side-effect ordering they explain, without claiming stronger atomicity than the
backend provides.

For copy mutations, previous-object facts describe the destination object being
overwritten. Source object state belongs to copy-source validation and metadata
selection, not mutation accounting.

Delete mutations have their own result shape because their protocol meaning is
delete-marker state and version identity, not a newly written object. They still
carry previous-object facts for accounting.

Multipart complete reads previous-object facts before invoking the backend
complete operation. The facade contract does not require backend-specific
hooks between object assembly and publication, though a backend may use such a
hook internally as an optimization.

If reading previous-object facts returns not-found, the mutation proceeds with
`Previous.Exists=false`. Any other previous-object read error fails the
operation before mutation.

### Admin API Wire Schema

`internal/adminapi` is the single source of truth for admin HTTP JSON body
types. Server admin handlers and the `volumeadmin` client package use aliases
to those wire types so response fields such as scrub peer failure details and
volume snapshot metadata cannot drift between producer and consumer packages.

Runtime concerns stay outside `adminapi`: handler dependencies, CLI options,
HTTP transport behavior, typed client errors, and server domain models remain
in their owning packages.

### Shard Group Peer Identity

Shard group peer identity is the node identifier stored in
`ShardGroupEntry.PeerIDs` for data Raft group membership. New shard group
metadata resolves known peer addresses to stable node IDs before writing group
membership; raft addresses are tolerated only as legacy/static aliases that
must be resolved at the cluster address book seam.
