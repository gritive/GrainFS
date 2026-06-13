package raft

import (
	"fmt"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
)

var (
	v2LogPrefix    = []byte("raft/v2/log/")
	v2StablePrefix = []byte("raft/v2/hardstate/")
	v2SnapPrefix   = []byte("raft/v2/snap/")
)

const v2StoreSubdir = "raft-v2"

// OpenV2Stores opens the shared raft-store BadgerDB under dir/raft-v2 and
// builds the durable LogStore/StableStore/SnapshotStore triple on it. The
// returned closeFn closes the underlying DB; on error nothing is left open.
// Moved here from internal/cluster (Phase 6.5 S3) — opening the raft store
// is Infrastructure, not Domain.
func OpenV2Stores(dir string, encryptionKey []byte) (LogStore, StableStore, SnapshotStore, func() error, error) {
	storeDir := filepath.Join(dir, v2StoreSubdir)
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("mkdir %s: %w", storeDir, err)
	}
	bopts := badgerutil.RaftLogOptions(storeDir, true)
	var err error
	if len(encryptionKey) > 0 {
		bopts, err = badgerutil.RaftLogEncryptedOptions(storeDir, true, encryptionKey)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	db, err := badger.Open(bopts)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open badger %s: %w", storeDir, err)
	}
	ls, err := NewBadgerLogStore(db, v2LogPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerLogStore: %w", err)
	}
	ss, err := NewBadgerStableStore(db, v2StablePrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerStableStore: %w", err)
	}
	sn, err := NewBadgerSnapshotStore(db, v2SnapPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerSnapshotStore: %w", err)
	}
	return ls, ss, sn, db.Close, nil
}
