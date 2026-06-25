package cluster

import "github.com/gritive/GrainFS/internal/metastore"

// Phase 6.5: metadata-store contract aliases. cluster code consumes the
// store through these names; the concrete implementation (badgermeta or
// MemStore) is injected at the composition root. The contract itself lives
// in internal/metastore so adapters can return its sentinel errors without
// importing cluster — see docs/adr/2026-06-13-metastore-contract-package.md.
//
// S6.5-2 migrated cluster's former raw badger txn/item/iterator signatures
// to these types.
type (
	MetadataStore       = metastore.Store
	MetadataTxn         = metastore.Txn
	MetaItem            = metastore.Item
	MetaIterator        = metastore.Iterator
	MetaIteratorOptions = metastore.IteratorOptions
)

var (
	// ErrMetaKeyNotFound is the missing-key sentinel cluster code checks
	// (migrated off the badger sentinel in S6.5-2).
	ErrMetaKeyNotFound = metastore.ErrKeyNotFound
	// ErrMetaTxnTooBig is re-exported for cluster storage helpers that still
	// perform Badger-backed writes outside the retired data-group apply path.
	ErrMetaTxnTooBig = metastore.ErrTxnTooBig
)
