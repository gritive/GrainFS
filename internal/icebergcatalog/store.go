package icebergcatalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	ErrNamespaceNotFound  = errors.New("iceberg namespace not found")
	ErrNamespaceExists    = errors.New("iceberg namespace already exists")
	ErrNamespaceNotEmpty  = errors.New("iceberg namespace is not empty")
	ErrTableNotFound      = errors.New("iceberg table not found")
	ErrTableExists        = errors.New("iceberg table already exists")
	ErrCommitFailed       = errors.New("iceberg commit failed")
	ErrServiceUnavailable = errors.New("iceberg catalog service unavailable")
)

type Identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type Table struct {
	Identifier       Identifier        `json:"identifier"`
	MetadataLocation string            `json:"metadata-location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Properties       map[string]string `json:"properties,omitempty"`
}

type CreateTableInput struct {
	MetadataLocation string
	Metadata         json.RawMessage
	Properties       map[string]string
}

type CommitTableInput struct {
	ExpectedMetadataLocation string
	NewMetadataLocation      string
	Metadata                 json.RawMessage
}

type Catalog interface {
	Warehouse() string
	CreateNamespace(ctx context.Context, namespace []string, properties map[string]string) error
	LoadNamespace(ctx context.Context, namespace []string) (map[string]string, error)
	ListNamespaces(ctx context.Context) ([][]string, error)
	DeleteNamespace(ctx context.Context, namespace []string) error
	CreateTable(ctx context.Context, ident Identifier, in CreateTableInput) (*Table, error)
	LoadTable(ctx context.Context, ident Identifier) (*Table, error)
	ListTables(ctx context.Context, namespace []string) ([]Identifier, error)
	DeleteTable(ctx context.Context, ident Identifier) error
	CommitTable(ctx context.Context, ident Identifier, in CommitTableInput) (*Table, error)
}

type Store struct {
	db        *badger.DB
	warehouse string
}

type LegacyExport struct {
	Namespaces []LegacyNamespace
	Tables     []LegacyTable
}

type LegacyNamespace struct {
	Namespace  []string
	Properties map[string]string
}

type LegacyTable struct {
	Identifier       Identifier
	MetadataLocation string
	Metadata         json.RawMessage
	Properties       map[string]string
}

func NewStore(db *badger.DB, warehouse string) *Store {
	return &Store{db: db, warehouse: warehouse}
}

func (s *Store) Warehouse() string { return s.warehouse }

func (s *Store) CreateNamespace(_ context.Context, namespace []string, properties map[string]string) error {
	key := namespaceKey(namespace)
	val, err := json.Marshal(namespaceRecord{Namespace: namespace, Properties: cloneMap(properties)})
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(key); err == nil {
			return ErrNamespaceExists
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return txn.Set(key, val)
	})
}

func (s *Store) LoadNamespace(_ context.Context, namespace []string) (map[string]string, error) {
	var rec namespaceRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(namespaceKey(namespace))
		if err == badger.ErrKeyNotFound {
			return ErrNamespaceNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error { return json.Unmarshal(v, &rec) })
	})
	if err != nil {
		return nil, err
	}
	return cloneMap(rec.Properties), nil
}

func (s *Store) ListNamespaces(_ context.Context) ([][]string, error) {
	var out [][]string
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(nsPrefix); it.ValidForPrefix(nsPrefix); it.Next() {
			var rec namespaceRecord
			if err := it.Item().Value(func(v []byte) error { return json.Unmarshal(v, &rec) }); err != nil {
				return err
			}
			out = append(out, append([]string(nil), rec.Namespace...))
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return strings.Join(out[i], "\x1f") < strings.Join(out[j], "\x1f") })
	return out, err
}

func (s *Store) ExportLegacyRows(_ context.Context) (LegacyExport, error) {
	var out LegacyExport
	err := s.db.View(func(txn *badger.Txn) error {
		nsIt := txn.NewIterator(badger.DefaultIteratorOptions)
		defer nsIt.Close()
		for nsIt.Seek(nsPrefix); nsIt.ValidForPrefix(nsPrefix); nsIt.Next() {
			var rec namespaceRecord
			if err := nsIt.Item().Value(func(v []byte) error { return json.Unmarshal(v, &rec) }); err != nil {
				return err
			}
			out.Namespaces = append(out.Namespaces, LegacyNamespace{
				Namespace:  append([]string(nil), rec.Namespace...),
				Properties: cloneMap(rec.Properties),
			})
		}

		tableIt := txn.NewIterator(badger.DefaultIteratorOptions)
		defer tableIt.Close()
		for tableIt.Seek(tablePrefix); tableIt.ValidForPrefix(tablePrefix); tableIt.Next() {
			var rec tableRecord
			if err := tableIt.Item().Value(func(v []byte) error { return json.Unmarshal(v, &rec) }); err != nil {
				return err
			}
			out.Tables = append(out.Tables, LegacyTable{
				Identifier:       cloneIdent(rec.Identifier),
				MetadataLocation: rec.MetadataLocation,
				Metadata:         cloneJSON(rec.Metadata),
				Properties:       cloneMap(rec.Properties),
			})
		}
		return nil
	})
	sort.Slice(out.Namespaces, func(i, j int) bool {
		return strings.Join(out.Namespaces[i].Namespace, "\x1f") < strings.Join(out.Namespaces[j].Namespace, "\x1f")
	})
	sort.Slice(out.Tables, func(i, j int) bool {
		left := strings.Join(append(append([]string(nil), out.Tables[i].Identifier.Namespace...), out.Tables[i].Identifier.Name), "\x1f")
		right := strings.Join(append(append([]string(nil), out.Tables[j].Identifier.Namespace...), out.Tables[j].Identifier.Name), "\x1f")
		return left < right
	})
	return out, err
}

func (s *Store) DeleteNamespace(_ context.Context, namespace []string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(namespaceKey(namespace)); err == badger.ErrKeyNotFound {
			return ErrNamespaceNotFound
		} else if err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := tableNamespacePrefix(namespace)
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			return ErrNamespaceNotEmpty
		}
		return txn.Delete(namespaceKey(namespace))
	})
}

func (s *Store) CreateTable(_ context.Context, ident Identifier, in CreateTableInput) (*Table, error) {
	rec := tableRecord{
		Identifier:       cloneIdent(ident),
		MetadataLocation: in.MetadataLocation,
		Metadata:         cloneJSON(in.Metadata),
		Properties:       cloneMap(in.Properties),
	}
	val, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	err = s.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(namespaceKey(ident.Namespace)); err == badger.ErrKeyNotFound {
			return ErrNamespaceNotFound
		} else if err != nil {
			return err
		}
		if _, err := txn.Get(tableKey(ident)); err == nil {
			return ErrTableExists
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return txn.Set(tableKey(ident), val)
	})
	if err != nil {
		return nil, err
	}
	return rec.table(), nil
}

func (s *Store) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	var rec tableRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(tableKey(ident))
		if err == badger.ErrKeyNotFound {
			if _, nsErr := txn.Get(namespaceKey(ident.Namespace)); nsErr == badger.ErrKeyNotFound {
				return ErrNamespaceNotFound
			}
			return ErrTableNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error { return json.Unmarshal(v, &rec) })
	})
	if err != nil {
		return nil, err
	}
	return rec.table(), nil
}

func (s *Store) ListTables(_ context.Context, namespace []string) ([]Identifier, error) {
	prefix := tableNamespacePrefix(namespace)
	var out []Identifier
	err := s.db.View(func(txn *badger.Txn) error {
		if _, err := txn.Get(namespaceKey(namespace)); err == badger.ErrKeyNotFound {
			return ErrNamespaceNotFound
		} else if err != nil {
			return err
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var rec tableRecord
			if err := it.Item().Value(func(v []byte) error { return json.Unmarshal(v, &rec) }); err != nil {
				return err
			}
			out = append(out, cloneIdent(rec.Identifier))
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, err
}

func (s *Store) DeleteTable(_ context.Context, ident Identifier) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(tableKey(ident)); err == badger.ErrKeyNotFound {
			if _, nsErr := txn.Get(namespaceKey(ident.Namespace)); nsErr == badger.ErrKeyNotFound {
				return ErrNamespaceNotFound
			}
			return ErrTableNotFound
		} else if err != nil {
			return err
		}
		return txn.Delete(tableKey(ident))
	})
}

func (s *Store) CommitTable(_ context.Context, ident Identifier, in CommitTableInput) (*Table, error) {
	var rec tableRecord
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(tableKey(ident))
		if err == badger.ErrKeyNotFound {
			return ErrTableNotFound
		}
		if err != nil {
			return err
		}
		if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &rec) }); err != nil {
			return err
		}
		if rec.MetadataLocation != in.ExpectedMetadataLocation {
			return ErrCommitFailed
		}
		rec.MetadataLocation = in.NewMetadataLocation
		rec.Metadata = cloneJSON(in.Metadata)
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return txn.Set(tableKey(ident), val)
	})
	if err != nil {
		return nil, err
	}
	return rec.table(), nil
}

type namespaceRecord struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties,omitempty"`
}

type tableRecord struct {
	Identifier       Identifier        `json:"identifier"`
	MetadataLocation string            `json:"metadata_location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Properties       map[string]string `json:"properties,omitempty"`
}

func (r tableRecord) table() *Table {
	return &Table{
		Identifier:       cloneIdent(r.Identifier),
		MetadataLocation: r.MetadataLocation,
		Metadata:         cloneJSON(r.Metadata),
		Properties:       cloneMap(r.Properties),
	}
}

var (
	nsPrefix    = []byte("iceberg:ns:")
	tablePrefix = []byte("iceberg:table:")
)

func namespaceKey(namespace []string) []byte {
	return append(append([]byte(nil), nsPrefix...), []byte(joinIdent(namespace))...)
}

func tableNamespacePrefix(namespace []string) []byte {
	p := append(append([]byte(nil), tablePrefix...), []byte(joinIdent(namespace))...)
	return append(p, 0)
}

func tableKey(ident Identifier) []byte {
	p := tableNamespacePrefix(ident.Namespace)
	return append(p, []byte(ident.Name)...)
}

func joinIdent(parts []string) string {
	return strings.Join(parts, "\x1f")
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneIdent(in Identifier) Identifier {
	return Identifier{Namespace: append([]string(nil), in.Namespace...), Name: in.Name}
}

func cloneJSON(in json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), bytes.TrimSpace(in)...)
}
