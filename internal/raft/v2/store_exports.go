package raftv2

// store_exports.go — exported constructors for the durable Badger-backed
// LogStore, StableStore, and SnapshotStore. These are thin wrappers around the
// package-private implementations so external packages (e.g. internal/cluster,
// internal/serveruntime) can supply durable storage to NewNode without
// reaching into unexported helpers.
//
// Pairing rule (per Config docs in types.go): callers MUST supply durable
// LogStore + StableStore + SnapshotStore together, or none of them. Pairing a
// persistent LogStore with an in-memory StableStore or SnapshotStore violates
// Raft §5.4.1 / §7 safety on restart.

import (
	badger "github.com/dgraph-io/badger/v4"
)

// NewBadgerLogStore opens a durable LogStore backed by BadgerDB under the
// given key prefix. The DB is owned by the caller; the LogStore does not close
// it.
func NewBadgerLogStore(db *badger.DB, prefix []byte) (LogStore, error) {
	return newBadgerLogStore(db, prefix)
}

// NewBadgerStableStore opens a durable StableStore backed by BadgerDB under
// the given key prefix. The DB is owned by the caller.
func NewBadgerStableStore(db *badger.DB, prefix []byte) (StableStore, error) {
	return newBadgerStableStore(db, prefix)
}

// NewBadgerSnapshotStore opens a durable SnapshotStore backed by BadgerDB
// under the given key prefix. The DB is owned by the caller.
func NewBadgerSnapshotStore(db *badger.DB, prefix []byte) (SnapshotStore, error) {
	return newBadgerSnapshotStore(db, prefix)
}
