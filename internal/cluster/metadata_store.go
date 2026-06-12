package cluster

import "github.com/gritive/GrainFS/internal/metastore"

// Phase 6.5: metadata-store contract aliases. cluster code consumes the
// store through these names; the concrete implementation (badgermeta or
// MemStore) is injected at the composition root. The contract itself lives
// in internal/metastore so adapters can return its sentinel errors without
// importing cluster — see docs/adr/2026-06-13-metastore-contract-package.md.
//
// Dormant in S6.5-1: declared but not yet consumed. S6.5-2+ migrate
// *badger.Txn signatures to these types.
type (
	MetadataStore = metastore.Store
	MetadataTxn   = metastore.Txn
	MetaItem      = metastore.Item
	MetaIterator  = metastore.Iterator
)

var (
	// ErrMetaKeyNotFound is the missing-key sentinel cluster code checks via
	// errors.Is once migrated off badger.ErrKeyNotFound.
	ErrMetaKeyNotFound = metastore.ErrKeyNotFound
	// ErrMetaTxnTooBig is the batch-limit sentinel for apply_actor's
	// split-and-retry path (today badger.ErrTxnTooBig at apply_actor.go:77).
	ErrMetaTxnTooBig = metastore.ErrTxnTooBig
)
