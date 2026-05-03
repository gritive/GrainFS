package badgerstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/incident"
)

const (
	statePrefix = "incident:"
	tsPrefix    = "incident_ts:"
)

type Store struct {
	db *badger.DB
}

func New(db *badger.DB) *Store { return &Store{db: db} }

func (s *Store) Put(ctx context.Context, state incident.IncidentState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.ID == "" {
		return fmt.Errorf("incident store: empty id")
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("incident store: encode %s: %w", state.ID, err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		if old, ok, err := getStateTxn(txn, state.ID); err != nil {
			return err
		} else if ok {
			if err := txn.Delete(tsKey(old.UpdatedAt.UnixNano(), old.ID)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		if err := txn.Set([]byte(statePrefix+state.ID), data); err != nil {
			return err
		}
		return txn.Set(tsKey(state.UpdatedAt.UnixNano(), state.ID), []byte(state.ID))
	})
}

func (s *Store) Get(ctx context.Context, id string) (incident.IncidentState, bool, error) {
	if err := ctx.Err(); err != nil {
		return incident.IncidentState{}, false, err
	}
	var out incident.IncidentState
	err := s.db.View(func(txn *badger.Txn) error {
		var ok bool
		var err error
		out, ok, err = getStateTxn(txn, id)
		if !ok && err == nil {
			return badger.ErrKeyNotFound
		}
		return err
	})
	if err == badger.ErrKeyNotFound {
		return incident.IncidentState{}, false, nil
	}
	if err != nil {
		return incident.IncidentState{}, false, err
	}
	return out, true, nil
}

func (s *Store) List(ctx context.Context, limit int) ([]incident.IncidentState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	out := make([]incident.IncidentState, 0, limit)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(tsPrefix)
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(tsPrefix)
		for it.Seek(tsSeekMax()); it.Valid(); it.Next() {
			item := it.Item()
			if !bytes.HasPrefix(item.Key(), prefix) {
				break
			}
			var id string
			if err := item.Value(func(v []byte) error {
				id = string(v)
				return nil
			}); err != nil {
				return err
			}
			state, ok, err := getStateTxn(txn, id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			out = append(out, state)
			if len(out) >= limit {
				break
			}
		}
		return nil
	})
	return out, err
}

func getStateTxn(txn *badger.Txn, id string) (incident.IncidentState, bool, error) {
	item, err := txn.Get([]byte(statePrefix + id))
	if err == badger.ErrKeyNotFound {
		return incident.IncidentState{}, false, nil
	}
	if err != nil {
		return incident.IncidentState{}, false, err
	}
	var out incident.IncidentState
	if err := item.Value(func(v []byte) error {
		return json.Unmarshal(v, &out)
	}); err != nil {
		return incident.IncidentState{}, false, err
	}
	return out, true, nil
}

func tsKey(unixNano int64, id string) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(unixNano)+uint64(math.MaxInt64))
	key := make([]byte, 0, len(tsPrefix)+len(buf)+1+len(id))
	key = append(key, tsPrefix...)
	key = append(key, buf...)
	key = append(key, 0)
	key = append(key, id...)
	return key
}

func tsSeekMax() []byte {
	key := make([]byte, len(tsPrefix)+8)
	copy(key, tsPrefix)
	for i := len(tsPrefix); i < len(key); i++ {
		key[i] = 0xff
	}
	return key
}
