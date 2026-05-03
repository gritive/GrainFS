package cluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

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
}

func NewMetaCatalog(meta *MetaRaft, backend storage.Backend, warehouse string) *MetaCatalog {
	return &MetaCatalog{meta: meta, backend: backend, warehouse: warehouse}
}

func NewMetaCatalogWithForwarder(meta *MetaRaft, backend storage.Backend, warehouse string, forward func(context.Context, []byte) error) *MetaCatalog {
	return &MetaCatalog{meta: meta, backend: backend, warehouse: warehouse, forward: forward}
}

func NewMetaCatalogWithForwarders(
	meta *MetaRaft,
	backend storage.Backend,
	warehouse string,
	forward func(context.Context, []byte) error,
	read *MetaCatalogReadSender,
	readPeers func() []string,
) *MetaCatalog {
	return &MetaCatalog{meta: meta, backend: backend, warehouse: warehouse, forward: forward, read: read, readPeers: readPeers}
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
	return c.LoadTable(ctx, ident)
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
	return c.propose(ctx, MetaCmdTypeIcebergDeleteTable, payload, cmd.RequestID)
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

func (c *MetaCatalog) requestID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, c.nextID.Add(1))
}

func (c *MetaCatalog) readMetadata(location string) ([]byte, error) {
	bucket, key, ok := parseIcebergS3Location(location)
	if !ok {
		return nil, fmt.Errorf("invalid Iceberg metadata location: %s", location)
	}
	rc, _, err := c.backend.GetObject(context.Background(), bucket, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
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
