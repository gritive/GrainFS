package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

type MetaCatalog struct {
	meta      *MetaRaft
	backend   storage.Backend
	warehouse string
	forward   func(context.Context, []byte) error
	read      *MetaCatalogReadSender
	readPeers func() []string
	nextID    atomic.Uint64
	idPrefix  string // 16 hex chars from crypto/rand, unique per instance
	cacheMu   sync.RWMutex
	cache     map[string]cachedIcebergMetadata
}

// newIcebergRequestIDPrefix returns a 16-char hex string from 8 random bytes.
// Panics on crypto/rand failure — this is boot-time only.
func newIcebergRequestIDPrefix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("newIcebergRequestIDPrefix: crypto/rand failed: %w", err))
	}
	return hex.EncodeToString(b[:])
}

type cachedIcebergMetadata struct {
	location string
	metadata []byte
}

func NewMetaCatalog(meta *MetaRaft, backend storage.Backend, warehouse string) *MetaCatalog {
	return &MetaCatalog{meta: meta, backend: backend, warehouse: warehouse, idPrefix: newIcebergRequestIDPrefix()}
}

func NewMetaCatalogWithForwarder(meta *MetaRaft, backend storage.Backend, warehouse string, forward func(context.Context, []byte) error) *MetaCatalog {
	return &MetaCatalog{meta: meta, backend: backend, warehouse: warehouse, forward: forward, idPrefix: newIcebergRequestIDPrefix()}
}

func NewMetaCatalogWithForwarders(
	meta *MetaRaft,
	backend storage.Backend,
	warehouse string,
	forward func(context.Context, []byte) error,
	read *MetaCatalogReadSender,
	readPeers func() []string,
) *MetaCatalog {
	return &MetaCatalog{meta: meta, backend: backend, warehouse: warehouse, forward: forward, read: read, readPeers: readPeers, idPrefix: newIcebergRequestIDPrefix()}
}

func (c *MetaCatalog) Warehouse() string { return c.warehouse }

func (c *MetaCatalog) CreateNamespace(ctx context.Context, namespace []string, properties map[string]string) error {
	cmd := IcebergCreateNamespaceCmd{
		RequestID:  c.requestID("create-namespace"),
		Namespace:  namespace,
		Properties: properties,
	}
	payload, err := encodeMetaIcebergCreateNamespaceCmd(cmd)
	if err != nil {
		return err
	}
	return c.propose(ctx, MetaCmdTypeIcebergCreateNamespace, payload, cmd.RequestID)
}

func (c *MetaCatalog) LoadNamespace(ctx context.Context, namespace []string) (map[string]string, error) {
	if !c.meta.IsLeader() && c.read != nil {
		return c.read.LoadNamespace(ctx, c.readTargets(), namespace)
	}
	return c.loadNamespaceLocal(namespace)
}

func (c *MetaCatalog) loadNamespaceLocal(namespace []string) (map[string]string, error) {
	entry, ok := c.meta.FSM().IcebergNamespace(namespace)
	if !ok {
		return nil, icebergcatalog.ErrNamespaceNotFound
	}
	return cloneStringMap(entry.Properties), nil
}

func (c *MetaCatalog) ListNamespaces(ctx context.Context) ([][]string, error) {
	if !c.meta.IsLeader() && c.read != nil {
		return c.read.ListNamespaces(ctx, c.readTargets())
	}
	return c.listNamespacesLocal(), nil
}

func (c *MetaCatalog) listNamespacesLocal() [][]string {
	entries := c.meta.FSM().IcebergNamespaces()
	out := make([][]string, len(entries))
	for i, entry := range entries {
		out[i] = cloneStringSlice(entry.Namespace)
	}
	return out
}

func (c *MetaCatalog) DeleteNamespace(ctx context.Context, namespace []string) error {
	cmd := IcebergDeleteNamespaceCmd{
		RequestID: c.requestID("delete-namespace"),
		Namespace: namespace,
	}
	payload, err := encodeMetaIcebergDeleteNamespaceCmd(cmd)
	if err != nil {
		return err
	}
	return c.propose(ctx, MetaCmdTypeIcebergDeleteNamespace, payload, cmd.RequestID)
}

func (c *MetaCatalog) CreateTable(ctx context.Context, ident icebergcatalog.Identifier, in icebergcatalog.CreateTableInput) (*icebergcatalog.Table, error) {
	cmd := IcebergCreateTableCmd{
		RequestID:        c.requestID("create-table"),
		Identifier:       ident,
		MetadataLocation: in.MetadataLocation,
		Properties:       in.Properties,
	}
	payload, err := encodeMetaIcebergCreateTableCmd(cmd)
	if err != nil {
		return nil, err
	}
	if err := c.propose(ctx, MetaCmdTypeIcebergCreateTable, payload, cmd.RequestID); err != nil {
		return nil, err
	}
	if !c.meta.IsLeader() && len(in.Metadata) > 0 {
		metadata := make([]byte, len(in.Metadata))
		copy(metadata, in.Metadata)
		c.storeCachedMetadata(ident, in.MetadataLocation, metadata)
		return &icebergcatalog.Table{
			Identifier:       ident,
			MetadataLocation: in.MetadataLocation,
			Metadata:         metadata,
			Properties:       cloneStringMap(in.Properties),
		}, nil
	}
	if !c.meta.IsLeader() {
		return c.LoadTable(ctx, ident)
	}
	tbl, err := c.loadTableLocal(ident)
	if err != nil {
		return nil, err
	}
	c.storeCachedMetadata(ident, tbl.MetadataLocation, tbl.Metadata)
	return tbl, nil
}

func (c *MetaCatalog) LoadTable(ctx context.Context, ident icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	if !c.meta.IsLeader() && c.read != nil {
		return c.read.LoadTable(ctx, c.readTargets(), ident)
	}
	return c.loadTableLocal(ident)
}

func (c *MetaCatalog) loadTableLocal(ident icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	entry, ok := c.meta.FSM().IcebergTable(ident)
	if !ok {
		if _, nsOK := c.meta.FSM().IcebergNamespace(ident.Namespace); !nsOK {
			return nil, icebergcatalog.ErrNamespaceNotFound
		}
		return nil, icebergcatalog.ErrTableNotFound
	}
	if metadata, ok := c.cachedMetadata(ident, entry.MetadataLocation); ok {
		return &icebergcatalog.Table{
			Identifier:       cloneIcebergIdent(entry.Identifier),
			MetadataLocation: entry.MetadataLocation,
			Metadata:         metadata,
			Properties:       cloneStringMap(entry.Properties),
		}, nil
	}
	metadata, err := c.readMetadata(entry.MetadataLocation)
	if err != nil {
		return nil, err
	}
	return &icebergcatalog.Table{
		Identifier:       cloneIcebergIdent(entry.Identifier),
		MetadataLocation: entry.MetadataLocation,
		Metadata:         metadata,
		Properties:       cloneStringMap(entry.Properties),
	}, nil
}

func (c *MetaCatalog) ListTables(ctx context.Context, namespace []string) ([]icebergcatalog.Identifier, error) {
	if !c.meta.IsLeader() && c.read != nil {
		return c.read.ListTables(ctx, c.readTargets(), namespace)
	}
	return c.listTablesLocal(namespace)
}

func (c *MetaCatalog) listTablesLocal(namespace []string) ([]icebergcatalog.Identifier, error) {
	if _, ok := c.meta.FSM().IcebergNamespace(namespace); !ok {
		return nil, icebergcatalog.ErrNamespaceNotFound
	}
	entries := c.meta.FSM().IcebergTables(namespace)
	out := make([]icebergcatalog.Identifier, len(entries))
	for i, entry := range entries {
		out[i] = cloneIcebergIdent(entry.Identifier)
	}
	return out, nil
}

func (c *MetaCatalog) readTargets() []string {
	if c.readPeers != nil {
		return c.readPeers()
	}
	return nil
}

func (c *MetaCatalog) DeleteTable(ctx context.Context, ident icebergcatalog.Identifier) error {
	cmd := IcebergDeleteTableCmd{
		RequestID:  c.requestID("delete-table"),
		Identifier: ident,
	}
	payload, err := encodeMetaIcebergDeleteTableCmd(cmd)
	if err != nil {
		return err
	}
	if err := c.propose(ctx, MetaCmdTypeIcebergDeleteTable, payload, cmd.RequestID); err != nil {
		return err
	}
	c.deleteCachedMetadata(ident)
	return nil
}

func (c *MetaCatalog) CommitTable(ctx context.Context, ident icebergcatalog.Identifier, in icebergcatalog.CommitTableInput) (*icebergcatalog.Table, error) {
	cmd := IcebergCommitTableCmd{
		RequestID:                c.requestID("commit-table"),
		Identifier:               ident,
		ExpectedMetadataLocation: in.ExpectedMetadataLocation,
		NewMetadataLocation:      in.NewMetadataLocation,
	}
	payload, err := encodeMetaIcebergCommitTableCmd(cmd)
	if err != nil {
		return nil, err
	}
	if err := c.propose(ctx, MetaCmdTypeIcebergCommitTable, payload, cmd.RequestID); err != nil {
		return nil, err
	}
	metadata := make([]byte, len(in.Metadata))
	copy(metadata, in.Metadata)
	if len(metadata) > 0 {
		c.storeCachedMetadata(ident, in.NewMetadataLocation, metadata)
	}
	return &icebergcatalog.Table{
		Identifier:       ident,
		MetadataLocation: in.NewMetadataLocation,
		Metadata:         metadata,
	}, nil
}

func (c *MetaCatalog) propose(ctx context.Context, typ MetaCmdType, payload []byte, requestID string) error {
	if c.meta.IsLeader() {
		return c.meta.proposeIcebergCommand(ctx, typ, payload, requestID)
	}
	if c.forward == nil {
		return icebergcatalog.ErrServiceUnavailable
	}
	data, err := encodeMetaCmd(typ, payload)
	if err != nil {
		return err
	}
	return c.forward(ctx, data)
}

func (c *MetaCatalog) cachedMetadata(ident icebergcatalog.Identifier, location string) ([]byte, bool) {
	c.cacheMu.RLock()
	entry, ok := c.cache[icebergTableKey(ident)]
	c.cacheMu.RUnlock()
	if !ok || entry.location != location {
		return nil, false
	}
	metadata := make([]byte, len(entry.metadata))
	copy(metadata, entry.metadata)
	return metadata, true
}

func (c *MetaCatalog) storeCachedMetadata(ident icebergcatalog.Identifier, location string, metadata []byte) {
	c.cacheMu.Lock()
	if c.cache == nil {
		c.cache = make(map[string]cachedIcebergMetadata)
	}
	c.cache[icebergTableKey(ident)] = cachedIcebergMetadata{
		location: location,
		metadata: append([]byte(nil), metadata...),
	}
	c.cacheMu.Unlock()
}

func (c *MetaCatalog) deleteCachedMetadata(ident icebergcatalog.Identifier) {
	c.cacheMu.Lock()
	delete(c.cache, icebergTableKey(ident))
	c.cacheMu.Unlock()
}

func (c *MetaCatalog) requestID(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, c.idPrefix, c.nextID.Add(1))
}

// readMetadata fetches a metadata.json from the cluster backend.
//
// Under heavy concurrent CommitTable load, an immediately-following LoadTable
// against the freshly committed metadata location can race the backend's
// post-write visibility — the FSM entry is durable on raft commit, but the
// backend write that landed just before the propose is sometimes not yet
// observable from a different reader. The error surfaces as
// storage.ErrObjectNotFound on the read side and propagates as a 500 to the
// client even though the catalog state itself is consistent.
//
// Apply a small bounded retry with backoff to bridge the consistency
// window. The schedule is tuned to stay well under warp's 10 s per-request
// timeout while absorbing typical millisecond-scale propagation delays:
// 5 attempts, 5/15/35/75 ms cumulative ≈ 130 ms worst case. Non-ENOENT
// errors return immediately.
func (c *MetaCatalog) readMetadata(location string) ([]byte, error) {
	bucket, key, ok := parseIcebergS3Location(location)
	if !ok {
		return nil, fmt.Errorf("invalid Iceberg metadata location: %s", location)
	}
	var lastErr error
	backoffs := []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for _, backoff := range backoffs {
		if backoff > 0 {
			time.Sleep(backoff)
		}
		rc, _, err := c.backend.GetObject(context.Background(), bucket, key)
		if err == nil {
			defer rc.Close()
			return io.ReadAll(rc)
		}
		lastErr = err
		if !errors.Is(err, storage.ErrObjectNotFound) {
			// Only retry the visible "not yet there" race. Anything else
			// (permission, bucket gone, network) is a real error.
			return nil, err
		}
	}
	return nil, lastErr
}

func parseIcebergS3Location(location string) (bucket, key string, ok bool) {
	const prefix = "s3://"
	if !strings.HasPrefix(location, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(location, prefix)
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", false
	}
	return rest[:slash], rest[slash+1:], true
}

var _ icebergcatalog.Catalog = (*MetaCatalog)(nil)
