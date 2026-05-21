package mountsastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

const keyPrefix = "mountsa/"

// ErrNotFound is returned by ApplyDelete when the named MountSA does not exist.
var ErrNotFound = errors.New("mountsa: not found")

// MountSA is a service account for NFS/9P mount clients.
type MountSA struct {
	Name       string `json:"name"`
	NumericUID uint32 `json:"uid"`
	CreatedAt  int64  `json:"created_at"`
	CreatedBy  string `json:"created_by,omitempty"`
}

type Store struct {
	db   *badger.DB
	mu   sync.Mutex
	snap atomic.Pointer[[]MountSA]
}

// NewStore returns a Store backed by db, pre-loaded with any persisted MountSAs.
// Returns an error if the persisted data cannot be read or unmarshalled.
func NewStore(db *badger.DB) (*Store, error) {
	s := &Store{db: db}
	loaded, err := loadAll(db)
	if err != nil {
		return nil, err
	}
	s.snap.Store(&loaded)
	return s, nil
}

// ApplyCreate persists sa. Idempotent: if a MountSA with the same name and
// identical payload already exists, it returns nil.
func (s *Store) ApplyCreate(sa MountSA) error {
	if sa.Name == "" {
		return fmt.Errorf("mountsa: name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for existing entry to enable idempotency.
	existing, ok, err := getFromDB(s.db, sa.Name)
	if err != nil {
		return err
	}
	if ok {
		if existing == sa {
			return nil
		}
		return fmt.Errorf("mountsa: %q already exists with different payload", sa.Name)
	}

	v, err := json.Marshal(sa)
	if err != nil {
		return fmt.Errorf("mountsa: marshal: %w", err)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyPrefix+sa.Name), v)
	}); err != nil {
		return err
	}
	return s.rebuildSnapshotLocked()
}

// ApplyDelete removes the MountSA with the given name. Returns ErrNotFound if missing.
func (s *Store) ApplyDelete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Update(func(txn *badger.Txn) error {
		// Check existence before deleting so we can return ErrNotFound.
		if _, err := txn.Get([]byte(keyPrefix + name)); errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		return txn.Delete([]byte(keyPrefix + name))
	}); err != nil {
		return err
	}
	return s.rebuildSnapshotLocked()
}

// Get returns the MountSA for name and ok=true, or zero value + false if missing.
func (s *Store) Get(name string) (MountSA, bool) {
	for _, sa := range *s.snap.Load() {
		if sa.Name == name {
			return sa, true
		}
	}
	return MountSA{}, false
}

// ListAll returns a copy of all MountSAs sorted by name.
func (s *Store) ListAll() []MountSA {
	snap := *s.snap.Load()
	out := make([]MountSA, len(snap))
	copy(out, snap)
	return out
}

// IsEmpty reports whether there are no MountSAs.
func (s *Store) IsEmpty() bool {
	return len(*s.snap.Load()) == 0
}

// rebuildSnapshotLocked re-reads all entries from Badger and atomically
// replaces the in-memory snapshot. Must be called with s.mu held.
func (s *Store) rebuildSnapshotLocked() error {
	loaded, err := loadAll(s.db)
	if err != nil {
		return err
	}
	s.snap.Store(&loaded)
	return nil
}

// loadAll reads all MountSAs from Badger under keyPrefix, sorted by name.
// Returns an error if any record cannot be read or unmarshalled.
func loadAll(db *badger.DB) ([]MountSA, error) {
	var out []MountSA
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				var sa MountSA
				if err := json.Unmarshal(v, &sa); err != nil {
					return fmt.Errorf("mountsa: unmarshal %q: %w", item.Key(), err)
				}
				// Ensure Name is consistent with the key.
				if sa.Name == "" {
					sa.Name = strings.TrimPrefix(string(item.Key()), keyPrefix)
				}
				out = append(out, sa)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// getFromDB reads a single MountSA by name directly from Badger (not the snapshot).
func getFromDB(db *badger.DB, name string) (MountSA, bool, error) {
	var sa MountSA
	var found bool
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyPrefix + name))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(v []byte) error {
			return json.Unmarshal(v, &sa)
		})
	})
	return sa, found, err
}
