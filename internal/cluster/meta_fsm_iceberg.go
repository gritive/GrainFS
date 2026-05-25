package cluster

import (
	"fmt"
	"sort"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

// icebergNSMap returns the per-warehouse namespace map, creating it if absent.
// Caller must hold f.mu (write lock).
func (f *MetaFSM) icebergNSMap(warehouse string) map[string]IcebergNamespaceEntry {
	m := f.icebergNamespaces[warehouse]
	if m == nil {
		m = make(map[string]IcebergNamespaceEntry)
		f.icebergNamespaces[warehouse] = m
	}
	return m
}

// icebergTblMap returns the per-warehouse table map, creating it if absent.
// Caller must hold f.mu (write lock).
func (f *MetaFSM) icebergTblMap(warehouse string) map[string]IcebergTableEntry {
	m := f.icebergTables[warehouse]
	if m == nil {
		m = make(map[string]IcebergTableEntry)
		f.icebergTables[warehouse] = m
	}
	return m
}

func (f *MetaFSM) applyIcebergCreateNamespace(data []byte) error {
	c, err := decodeMetaIcebergCreateNamespaceCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCreateNamespace: %w", err)
	}
	key := icebergNamespaceKey(c.Namespace)
	var result error
	f.mu.Lock()
	wh := icebergWarehouseKey(c.Warehouse)
	nsMap := f.icebergNSMap(wh)
	if _, ok := nsMap[key]; ok {
		result = icebergcatalog.ErrNamespaceExists
	} else {
		nsMap[key] = IcebergNamespaceEntry{
			Warehouse:  wh,
			Namespace:  cloneStringSlice(c.Namespace),
			Properties: cloneStringMap(c.Properties),
		}
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergDeleteNamespace(data []byte) error {
	c, err := decodeMetaIcebergDeleteNamespaceCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergDeleteNamespace: %w", err)
	}
	key := icebergNamespaceKey(c.Namespace)
	var result error
	wh := icebergWarehouseKey(c.Warehouse)
	f.mu.Lock()
	nsMap := f.icebergNSMap(wh)
	tblMap := f.icebergTblMap(wh)
	if _, ok := nsMap[key]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else {
		prefix := key + "\x1f"
		for tableKey := range tblMap {
			if strings.HasPrefix(tableKey, prefix) {
				result = icebergcatalog.ErrNamespaceNotEmpty
				break
			}
		}
		if result == nil {
			delete(nsMap, key)
		}
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergCreateTable(data []byte) error {
	c, err := decodeMetaIcebergCreateTableCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCreateTable: %w", err)
	}
	nsKey := icebergNamespaceKey(c.Identifier.Namespace)
	tableKey := icebergTableKey(c.Identifier)
	var result error
	wh := icebergWarehouseKey(c.Warehouse)
	f.mu.Lock()
	nsMap := f.icebergNSMap(wh)
	tblMap := f.icebergTblMap(wh)
	if _, ok := nsMap[nsKey]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else if _, ok := tblMap[tableKey]; ok {
		result = icebergcatalog.ErrTableExists
	} else {
		tblMap[tableKey] = IcebergTableEntry{
			Warehouse:        wh,
			Identifier:       cloneIcebergIdent(c.Identifier),
			MetadataLocation: c.MetadataLocation,
			Properties:       cloneStringMap(c.Properties),
		}
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergCommitTable(data []byte) error {
	c, err := decodeMetaIcebergCommitTableCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergCommitTable: %w", err)
	}
	tableKey := icebergTableKey(c.Identifier)
	var result error
	f.mu.Lock()
	tblMap := f.icebergTblMap(icebergWarehouseKey(c.Warehouse))
	entry, ok := tblMap[tableKey]
	if !ok {
		result = icebergcatalog.ErrTableNotFound
	} else if entry.MetadataLocation != c.ExpectedMetadataLocation {
		result = icebergcatalog.ErrCommitFailed
	} else {
		entry.MetadataLocation = c.NewMetadataLocation
		tblMap[tableKey] = entry
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) applyIcebergDeleteTable(data []byte) error {
	c, err := decodeMetaIcebergDeleteTableCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: IcebergDeleteTable: %w", err)
	}
	nsKey := icebergNamespaceKey(c.Identifier.Namespace)
	tableKey := icebergTableKey(c.Identifier)
	var result error
	wh2 := icebergWarehouseKey(c.Warehouse)
	f.mu.Lock()
	nsMap := f.icebergNSMap(wh2)
	tblMap := f.icebergTblMap(wh2)
	if _, ok := nsMap[nsKey]; !ok {
		result = icebergcatalog.ErrNamespaceNotFound
	} else if _, ok := tblMap[tableKey]; !ok {
		result = icebergcatalog.ErrTableNotFound
	} else {
		delete(tblMap, tableKey)
	}
	f.mu.Unlock()
	f.publishIcebergResult(c.RequestID, result)
	return nil
}

func (f *MetaFSM) IcebergNamespace(warehouse string, namespace []string) (IcebergNamespaceEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	wh := icebergWarehouseKey(warehouse)
	nsMap := f.icebergNamespaces[wh]
	if nsMap == nil {
		return IcebergNamespaceEntry{}, false
	}
	entry, ok := nsMap[icebergNamespaceKey(namespace)]
	if !ok {
		return IcebergNamespaceEntry{}, false
	}
	return IcebergNamespaceEntry{
		Warehouse:  wh,
		Namespace:  cloneStringSlice(entry.Namespace),
		Properties: cloneStringMap(entry.Properties),
	}, true
}

func (f *MetaFSM) IcebergNamespaces(warehouse string) []IcebergNamespaceEntry {
	wh := icebergWarehouseKey(warehouse)
	f.mu.RLock()
	nsMap := f.icebergNamespaces[wh]
	out := make([]IcebergNamespaceEntry, 0, len(nsMap))
	for _, entry := range nsMap {
		out = append(out, IcebergNamespaceEntry{
			Warehouse:  wh,
			Namespace:  cloneStringSlice(entry.Namespace),
			Properties: cloneStringMap(entry.Properties),
		})
	}
	f.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return icebergNamespaceKey(out[i].Namespace) < icebergNamespaceKey(out[j].Namespace)
	})
	return out
}

func (f *MetaFSM) IcebergTable(warehouse string, ident icebergcatalog.Identifier) (IcebergTableEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	tblMap := f.icebergTables[icebergWarehouseKey(warehouse)]
	if tblMap == nil {
		return IcebergTableEntry{}, false
	}
	entry, ok := tblMap[icebergTableKey(ident)]
	if !ok {
		return IcebergTableEntry{}, false
	}
	return cloneIcebergTableEntry(entry), true
}

func (f *MetaFSM) IcebergTables(warehouse string, namespace []string) []IcebergTableEntry {
	prefix := icebergNamespaceKey(namespace) + "\x1f"
	f.mu.RLock()
	tblMap := f.icebergTables[icebergWarehouseKey(warehouse)]
	out := make([]IcebergTableEntry, 0)
	for key, entry := range tblMap {
		if strings.HasPrefix(key, prefix) {
			out = append(out, cloneIcebergTableEntry(entry))
		}
	}
	f.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].Identifier.Name < out[j].Identifier.Name
	})
	return out
}

func encodeMetaIcebergCreateNamespaceCmd(c IcebergCreateNamespaceCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	namespaceVec := buildStringVector(b, c.Namespace, clusterpb.MetaIcebergCreateNamespaceCmdStartNamespaceVector)
	propsVec := buildKeyValuePropertiesVector(b, c.Properties, clusterpb.MetaIcebergCreateNamespaceCmdStartPropertiesVector)
	clusterpb.MetaIcebergCreateNamespaceCmdStart(b)
	clusterpb.MetaIcebergCreateNamespaceCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCreateNamespaceCmdAddNamespace(b, namespaceVec)
	clusterpb.MetaIcebergCreateNamespaceCmdAddProperties(b, propsVec)
	clusterpb.MetaIcebergCreateNamespaceCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergCreateNamespaceCmdEnd(b)), nil
}

func decodeMetaIcebergCreateNamespaceCmd(data []byte) (IcebergCreateNamespaceCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergCreateNamespaceCmd {
		return clusterpb.GetRootAsMetaIcebergCreateNamespaceCmd(d, 0)
	})
	if err != nil {
		return IcebergCreateNamespaceCmd{}, err
	}
	return IcebergCreateNamespaceCmd{
		RequestID:  string(t.RequestId()),
		Warehouse:  string(t.Warehouse()),
		Namespace:  readStringVector(t.NamespaceLength(), t.Namespace),
		Properties: readKeyValueProperties(t.PropertiesLength(), t.Properties),
	}, nil
}

func encodeMetaIcebergDeleteNamespaceCmd(c IcebergDeleteNamespaceCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	namespaceVec := buildStringVector(b, c.Namespace, clusterpb.MetaIcebergDeleteNamespaceCmdStartNamespaceVector)
	clusterpb.MetaIcebergDeleteNamespaceCmdStart(b)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddNamespace(b, namespaceVec)
	clusterpb.MetaIcebergDeleteNamespaceCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergDeleteNamespaceCmdEnd(b)), nil
}

func decodeMetaIcebergDeleteNamespaceCmd(data []byte) (IcebergDeleteNamespaceCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergDeleteNamespaceCmd {
		return clusterpb.GetRootAsMetaIcebergDeleteNamespaceCmd(d, 0)
	})
	if err != nil {
		return IcebergDeleteNamespaceCmd{}, err
	}
	return IcebergDeleteNamespaceCmd{
		RequestID: string(t.RequestId()),
		Warehouse: string(t.Warehouse()),
		Namespace: readStringVector(t.NamespaceLength(), t.Namespace),
	}, nil
}

func encodeMetaIcebergCreateTableCmd(c IcebergCreateTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	locationOff := b.CreateString(c.MetadataLocation)
	propsVec := buildKeyValuePropertiesVector(b, c.Properties, clusterpb.MetaIcebergCreateTableCmdStartPropertiesVector)
	clusterpb.MetaIcebergCreateTableCmdStart(b)
	clusterpb.MetaIcebergCreateTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCreateTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergCreateTableCmdAddMetadataLocation(b, locationOff)
	clusterpb.MetaIcebergCreateTableCmdAddProperties(b, propsVec)
	clusterpb.MetaIcebergCreateTableCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergCreateTableCmdEnd(b)), nil
}

func decodeMetaIcebergCreateTableCmd(data []byte) (IcebergCreateTableCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergCreateTableCmd {
		return clusterpb.GetRootAsMetaIcebergCreateTableCmd(d, 0)
	})
	if err != nil {
		return IcebergCreateTableCmd{}, err
	}
	ident, err := readIcebergIdentifier(t.Identifier(nil))
	if err != nil {
		return IcebergCreateTableCmd{}, err
	}
	return IcebergCreateTableCmd{
		RequestID:        string(t.RequestId()),
		Warehouse:        string(t.Warehouse()),
		Identifier:       ident,
		MetadataLocation: string(t.MetadataLocation()),
		Properties:       readKeyValueProperties(t.PropertiesLength(), t.Properties),
	}, nil
}

func encodeMetaIcebergCommitTableCmd(c IcebergCommitTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	expectedOff := b.CreateString(c.ExpectedMetadataLocation)
	nextOff := b.CreateString(c.NewMetadataLocation)
	clusterpb.MetaIcebergCommitTableCmdStart(b)
	clusterpb.MetaIcebergCommitTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergCommitTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergCommitTableCmdAddExpectedMetadataLocation(b, expectedOff)
	clusterpb.MetaIcebergCommitTableCmdAddNewMetadataLocation(b, nextOff)
	clusterpb.MetaIcebergCommitTableCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergCommitTableCmdEnd(b)), nil
}

func decodeMetaIcebergCommitTableCmd(data []byte) (IcebergCommitTableCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergCommitTableCmd {
		return clusterpb.GetRootAsMetaIcebergCommitTableCmd(d, 0)
	})
	if err != nil {
		return IcebergCommitTableCmd{}, err
	}
	ident, err := readIcebergIdentifier(t.Identifier(nil))
	if err != nil {
		return IcebergCommitTableCmd{}, err
	}
	return IcebergCommitTableCmd{
		RequestID:                string(t.RequestId()),
		Warehouse:                string(t.Warehouse()),
		Identifier:               ident,
		ExpectedMetadataLocation: string(t.ExpectedMetadataLocation()),
		NewMetadataLocation:      string(t.NewMetadataLocation()),
	}, nil
}

func encodeMetaIcebergDeleteTableCmd(c IcebergDeleteTableCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(c.RequestID)
	warehouseOff := b.CreateString(c.Warehouse)
	identOff := buildIcebergIdentifier(b, c.Identifier)
	clusterpb.MetaIcebergDeleteTableCmdStart(b)
	clusterpb.MetaIcebergDeleteTableCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaIcebergDeleteTableCmdAddIdentifier(b, identOff)
	clusterpb.MetaIcebergDeleteTableCmdAddWarehouse(b, warehouseOff)
	return fbFinish(b, clusterpb.MetaIcebergDeleteTableCmdEnd(b)), nil
}

func decodeMetaIcebergDeleteTableCmd(data []byte) (IcebergDeleteTableCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaIcebergDeleteTableCmd {
		return clusterpb.GetRootAsMetaIcebergDeleteTableCmd(d, 0)
	})
	if err != nil {
		return IcebergDeleteTableCmd{}, err
	}
	ident, err := readIcebergIdentifier(t.Identifier(nil))
	if err != nil {
		return IcebergDeleteTableCmd{}, err
	}
	return IcebergDeleteTableCmd{
		RequestID:  string(t.RequestId()),
		Warehouse:  string(t.Warehouse()),
		Identifier: ident,
	}, nil
}

// icebergDefaultWarehouse is the warehouse key used for legacy single-warehouse
// installations and for commands that carry an empty warehouse string (pre-T38
// raft log entries replayed after upgrade).
const icebergDefaultWarehouse = "default"

// icebergWarehouseKey returns warehouse if non-empty, falling back to
// icebergDefaultWarehouse for legacy / single-warehouse mode.
func icebergWarehouseKey(warehouse string) string {
	if warehouse == "" {
		return icebergDefaultWarehouse
	}
	return warehouse
}

func icebergNamespaceKey(namespace []string) string {
	return strings.Join(namespace, "\x1f")
}

func icebergTableKey(ident icebergcatalog.Identifier) string {
	return icebergNamespaceKey(ident.Namespace) + "\x1f" + ident.Name
}

func cloneIcebergIdent(in icebergcatalog.Identifier) icebergcatalog.Identifier {
	return icebergcatalog.Identifier{Namespace: cloneStringSlice(in.Namespace), Name: in.Name}
}

func cloneIcebergTableEntry(in IcebergTableEntry) IcebergTableEntry {
	return IcebergTableEntry{
		Warehouse:        in.Warehouse,
		Identifier:       cloneIcebergIdent(in.Identifier),
		MetadataLocation: in.MetadataLocation,
		Properties:       cloneStringMap(in.Properties),
	}
}

func buildIcebergIdentifier(b *flatbuffers.Builder, ident icebergcatalog.Identifier) flatbuffers.UOffsetT {
	namespaceVec := buildStringVector(b, ident.Namespace, clusterpb.IcebergIdentifierStartNamespaceVector)
	nameOff := b.CreateString(ident.Name)
	clusterpb.IcebergIdentifierStart(b)
	clusterpb.IcebergIdentifierAddNamespace(b, namespaceVec)
	clusterpb.IcebergIdentifierAddName(b, nameOff)
	return clusterpb.IcebergIdentifierEnd(b)
}

func readIcebergIdentifier(ident *clusterpb.IcebergIdentifier) (icebergcatalog.Identifier, error) {
	if ident == nil {
		return icebergcatalog.Identifier{}, fmt.Errorf("missing iceberg identifier")
	}
	return icebergcatalog.Identifier{
		Namespace: readStringVector(ident.NamespaceLength(), ident.Namespace),
		Name:      string(ident.Name()),
	}, nil
}

func buildIcebergNamespaceEntriesVector(b *flatbuffers.Builder, entries []IcebergNamespaceEntry) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		namespaceVec := buildStringVector(b, entries[i].Namespace, clusterpb.IcebergNamespaceEntryStartNamespaceVector)
		propsVec := buildKeyValuePropertiesVector(b, entries[i].Properties, clusterpb.IcebergNamespaceEntryStartPropertiesVector)
		warehouseOff := b.CreateString(entries[i].Warehouse)
		clusterpb.IcebergNamespaceEntryStart(b)
		clusterpb.IcebergNamespaceEntryAddNamespace(b, namespaceVec)
		clusterpb.IcebergNamespaceEntryAddProperties(b, propsVec)
		clusterpb.IcebergNamespaceEntryAddWarehouse(b, warehouseOff)
		offsets[i] = clusterpb.IcebergNamespaceEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartIcebergNamespacesVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

func buildIcebergTableEntriesVector(b *flatbuffers.Builder, entries []IcebergTableEntry) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		identOff := buildIcebergIdentifier(b, entries[i].Identifier)
		locationOff := b.CreateString(entries[i].MetadataLocation)
		propsVec := buildKeyValuePropertiesVector(b, entries[i].Properties, clusterpb.IcebergTableEntryStartPropertiesVector)
		warehouseOff := b.CreateString(entries[i].Warehouse)
		clusterpb.IcebergTableEntryStart(b)
		clusterpb.IcebergTableEntryAddIdentifier(b, identOff)
		clusterpb.IcebergTableEntryAddMetadataLocation(b, locationOff)
		clusterpb.IcebergTableEntryAddProperties(b, propsVec)
		clusterpb.IcebergTableEntryAddWarehouse(b, warehouseOff)
		offsets[i] = clusterpb.IcebergTableEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartIcebergTablesVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}
