package cluster

import (
	"fmt"
	"sort"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

func (f *MetaFSM) applyAddNode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: AddNode: empty payload")
	}
	var (
		c      *clusterpb.MetaAddNodeCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaAddNodeCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaAddNodeCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	node := c.Node(nil)
	if node == nil {
		return fmt.Errorf("meta_fsm: AddNode: nil node")
	}
	entry := MetaNodeEntry{
		ID:      string(node.Id()),
		Address: string(node.Address()),
		Role:    node.Role(),
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: AddNode: empty node ID")
	}
	f.mu.Lock()
	f.nodes[entry.ID] = entry
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyRemoveNode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: RemoveNode: empty payload")
	}
	var (
		c      *clusterpb.MetaRemoveNodeCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaRemoveNodeCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaRemoveNodeCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	nodeID := string(c.NodeId())
	f.mu.Lock()
	delete(f.nodes, nodeID)
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyPutShardGroup(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutShardGroup: empty payload")
	}
	var (
		c      *clusterpb.MetaPutShardGroupCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutShardGroupCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutShardGroupCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	sg := c.Group(nil)
	if sg == nil {
		return fmt.Errorf("meta_fsm: PutShardGroup: nil group")
	}
	peers := make([]string, sg.PeerIdsLength())
	for i := 0; i < sg.PeerIdsLength(); i++ {
		peers[i] = string(sg.PeerIds(i))
	}
	entry := ShardGroupEntry{
		ID:      string(sg.Id()),
		PeerIDs: peers,
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: PutShardGroup: empty group ID")
	}
	// Reserved-namespace check. apply runs on log replay too; warn-and-skip
	// (rather than error-and-crash) so an old log entry containing a name
	// that became reserved later doesn't poison startup. Proposals are
	// rejected upstream in MetaRaft.ProposeShardGroup.
	if err := raft.ValidateGroupID(entry.ID); err != nil {
		log.Warn().Err(err).Str("group_id", entry.ID).Msg("meta_fsm: PutShardGroup: rejecting reserved group ID; entry will not be applied")
		return nil
	}
	if len(peers) == 0 {
		return fmt.Errorf("meta_fsm: PutShardGroup: group %q has no peers", entry.ID)
	}
	f.mu.Lock()
	f.shardGroups[entry.ID] = entry
	cbPeers := f.normalizeShardGroupPeersLocked(entry.PeerIDs)
	cb := f.onShardGroupAdded
	f.mu.Unlock()
	if cb != nil {
		// Defensive copy of peers — callback may keep references.
		cb(ShardGroupEntry{ID: entry.ID, PeerIDs: cbPeers})
	}
	return nil
}

func (f *MetaFSM) applyPutBucketAssignment(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty payload")
	}
	var (
		c      *clusterpb.MetaPutBucketAssignmentCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutBucketAssignmentCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutBucketAssignmentCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	entry := c.Entry(nil)
	if entry == nil {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: nil entry")
	}
	bucket := string(entry.Bucket())
	groupID := string(entry.GroupId())
	if bucket == "" {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty bucket")
	}
	if groupID == "" {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty groupID")
	}

	f.mu.Lock()
	f.bucketAssignments[bucket] = groupID
	cb := f.onBucketAssigned
	f.mu.Unlock()

	if cb != nil {
		cb(bucket, groupID)
	}
	return nil
}

type metaPutObjectIndexCmd struct {
	Entry          ObjectIndexEntry
	PreserveLatest bool
}

type objectIndexSnapshotEntry struct {
	ObjectIndexEntry
	IsLatest bool
	sortKey  string
}

func objectIndexLatestKey(bucket, key string) string {
	return bucket + "\x00" + key
}

func objectIndexVersionKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

func (f *MetaFSM) applyPutObjectIndex(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutObjectIndex: empty payload")
	}
	c, err := decodeMetaPutObjectIndexCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: PutObjectIndex: %w", err)
	}
	e := c.Entry
	if e.Bucket == "" || e.Key == "" || e.VersionID == "" {
		return fmt.Errorf("meta_fsm: PutObjectIndex: empty bucket/key/version")
	}
	if e.PlacementGroupID == "" {
		return fmt.Errorf("meta_fsm: PutObjectIndex: empty placement_group_id")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	vkey := objectIndexVersionKey(e.Bucket, e.Key, e.VersionID)
	// Overwrite: decrement ref for old entry's generation before replacing.
	if old, ok := f.objectIndex[vkey]; ok {
		f.decDEKRef(old.DekGen)
	}
	f.objectIndex[vkey] = cloneObjectIndexEntry(e)
	f.incDEKRef(e.DekGen)
	if !c.PreserveLatest {
		f.objectLatest[objectIndexLatestKey(e.Bucket, e.Key)] = e.VersionID
	}
	return nil
}

func (f *MetaFSM) applyDeleteObjectIndex(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: DeleteObjectIndex: empty payload")
	}
	bucket, key, versionID, err := decodeMetaDeleteObjectIndexCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: DeleteObjectIndex: %w", err)
	}
	if bucket == "" || key == "" || versionID == "" {
		return fmt.Errorf("meta_fsm: DeleteObjectIndex: empty bucket/key/version")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	vkey := objectIndexVersionKey(bucket, key, versionID)
	if old, ok := f.objectIndex[vkey]; ok {
		f.decDEKRef(old.DekGen)
	}
	delete(f.objectIndex, vkey)
	lkey := objectIndexLatestKey(bucket, key)
	if f.objectLatest[lkey] != versionID {
		return nil
	}

	var latest ObjectIndexEntry
	found := false
	for _, entry := range f.objectIndex {
		if entry.Bucket != bucket || entry.Key != key {
			continue
		}
		if !found || entry.ModTime > latest.ModTime || (entry.ModTime == latest.ModTime && entry.VersionID > latest.VersionID) {
			latest = entry
			found = true
		}
	}
	if !found {
		delete(f.objectLatest, lkey)
		return nil
	}
	f.objectLatest[lkey] = latest.VersionID
	return nil
}

// BucketAssignments returns a copy of the current bucket→group_id map.
func (f *MetaFSM) BucketAssignments() map[string]string {
	f.mu.RLock()
	out := make(map[string]string, len(f.bucketAssignments))
	for k, v := range f.bucketAssignments {
		out[k] = v
	}
	f.mu.RUnlock()
	return out
}

// HasUserData reports whether the FSM holds any user-created buckets.
// Used by the join handler to guard against accidental data loss.
func (f *MetaFSM) HasUserData() bool {
	f.mu.RLock()
	has := len(f.bucketAssignments) > 0
	f.mu.RUnlock()
	return has
}

func (f *MetaFSM) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	versionID, ok := f.objectLatest[objectIndexLatestKey(bucket, key)]
	if !ok {
		return ObjectIndexEntry{}, false
	}
	entry, ok := f.objectIndex[objectIndexVersionKey(bucket, key, versionID)]
	if !ok {
		return ObjectIndexEntry{}, false
	}
	return cloneObjectIndexEntry(entry), true
}

func (f *MetaFSM) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, ok := f.objectIndex[objectIndexVersionKey(bucket, key, versionID)]
	if !ok {
		return ObjectIndexEntry{}, false
	}
	return cloneObjectIndexEntry(entry), true
}

func (f *MetaFSM) ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	entries, _ := f.ObjectIndexLatestEntriesPage(bucket, prefix, "", maxKeys)
	return entries
}

// ObjectIndexLatestEntriesPage returns objects ordered by key for a single
// pagination page. Entries whose key is greater than `marker` (excluding the
// marker itself) up to `maxKeys` results are returned. `truncated` reports
// whether more entries match beyond the returned slice — callers use it to
// emit S3's IsTruncated/NextMarker fields. `maxKeys <= 0` disables the cap
// (used by WalkObjects-style callers that want every match).
func (f *MetaFSM) ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) (entries []ObjectIndexEntry, truncated bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries = make([]ObjectIndexEntry, 0)
	for lkey, versionID := range f.objectLatest {
		parts := strings.SplitN(lkey, "\x00", 2)
		if len(parts) != 2 || parts[0] != bucket || !strings.HasPrefix(parts[1], prefix) {
			continue
		}
		if marker != "" && parts[1] <= marker {
			continue
		}
		entry, ok := f.objectIndex[objectIndexVersionKey(bucket, parts[1], versionID)]
		if !ok || entry.IsDeleteMarker {
			continue
		}
		entries = append(entries, cloneObjectIndexEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	if maxKeys > 0 && len(entries) > maxKeys {
		entries = entries[:maxKeys]
		truncated = true
	}
	return entries, truncated
}

func (f *MetaFSM) ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries := make([]ObjectIndexEntry, 0)
	for _, entry := range f.objectIndex {
		if entry.Bucket != bucket || !strings.HasPrefix(entry.Key, prefix) {
			continue
		}
		entries = append(entries, cloneObjectIndexEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key != entries[j].Key {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].VersionID > entries[j].VersionID
	})
	if maxKeys > 0 && len(entries) > maxKeys {
		entries = entries[:maxKeys]
	}
	return entries
}

func (f *MetaFSM) ObjectIndexSummary(bucket string) ObjectIndexSummary {
	f.mu.RLock()
	defer f.mu.RUnlock()
	counts := make(map[string]int)
	for _, entry := range f.objectIndex {
		if bucket != "" && entry.Bucket != bucket {
			continue
		}
		counts[entry.PlacementGroupID]++
	}
	return ObjectIndexSummary{
		Bucket:               bucket,
		PlacementGroupCounts: counts,
	}
}

func (f *MetaFSM) PlacementReport(bucket, key string, maxRows int) PlacementReport {
	f.mu.RLock()
	defer f.mu.RUnlock()

	groups := make(map[string]ShardGroupEntry, len(f.shardGroups))
	for id, sg := range f.shardGroups {
		groups[id] = ShardGroupEntry{ID: sg.ID, PeerIDs: f.normalizeShardGroupPeersLocked(sg.PeerIDs)}
	}

	entries := make([]ObjectIndexEntry, 0)
	for _, entry := range f.objectIndex {
		if bucket != "" && entry.Bucket != bucket {
			continue
		}
		if key != "" && entry.Key != key {
			continue
		}
		if entry.IsDeleteMarker {
			continue
		}
		entries = append(entries, cloneObjectIndexEntry(entry))
	}
	return BuildPlacementReport(entries, groups, PlacementReportOptions{
		Bucket:  bucket,
		Key:     key,
		MaxRows: maxRows,
	})
}

// ShardGroups returns a deep copy of current shard groups.
// PeerIDs slices are copied so callers cannot mutate FSM state.
func (f *MetaFSM) ShardGroups() []ShardGroupEntry {
	f.mu.RLock()
	out := make([]ShardGroupEntry, 0, len(f.shardGroups))
	for _, sg := range f.shardGroups {
		peers := f.normalizeShardGroupPeersLocked(sg.PeerIDs)
		out = append(out, ShardGroupEntry{ID: sg.ID, PeerIDs: peers})
	}
	f.mu.RUnlock()
	return out
}

// ShardGroup returns the entry for id and true, or zero-value and false if not found.
// Returned PeerIDs is a defensive copy.
func (f *MetaFSM) ShardGroup(id string) (ShardGroupEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	g, ok := f.shardGroups[id]
	if !ok {
		return ShardGroupEntry{}, false
	}
	peers := f.normalizeShardGroupPeersLocked(g.PeerIDs)
	return ShardGroupEntry{ID: g.ID, PeerIDs: peers}, true
}

func (f *MetaFSM) normalizeShardGroupPeersLocked(peers []string) []string {
	out := make([]string, len(peers))
	for i, peer := range peers {
		out[i] = peer
		if nodeID, ok := f.resolveNodeIDByAddressLocked(peer); ok {
			out[i] = nodeID
		}
	}
	return out
}

func (f *MetaFSM) resolveNodeIDByAddressLocked(addr string) (string, bool) {
	for _, node := range f.nodes {
		if addr == node.Address && node.ID != "" {
			return node.ID, true
		}
	}
	return "", false
}

// Nodes returns a copy of current cluster members.
func (f *MetaFSM) Nodes() []MetaNodeEntry {
	f.mu.RLock()
	out := make([]MetaNodeEntry, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, n)
	}
	f.mu.RUnlock()
	return out
}

func encodeMetaPutBucketAssignmentCmd(bucket, groupID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	groupIDOff := b.CreateString(groupID)
	clusterpb.BucketAssignmentEntryStart(b)
	clusterpb.BucketAssignmentEntryAddBucket(b, bucketOff)
	clusterpb.BucketAssignmentEntryAddGroupId(b, groupIDOff)
	entryOff := clusterpb.BucketAssignmentEntryEnd(b)
	clusterpb.MetaPutBucketAssignmentCmdStart(b)
	clusterpb.MetaPutBucketAssignmentCmdAddEntry(b, entryOff)
	return fbFinish(b, clusterpb.MetaPutBucketAssignmentCmdEnd(b)), nil
}

func encodeMetaPutObjectIndexCmd(entry ObjectIndexEntry, preserveLatest bool) ([]byte, error) {
	b := clusterBuilderPool.Get()
	entryOff := buildMetaObjectIndexEntry(b, objectIndexSnapshotEntry{ObjectIndexEntry: entry})
	clusterpb.MetaPutObjectIndexCmdStart(b)
	clusterpb.MetaPutObjectIndexCmdAddEntry(b, entryOff)
	if preserveLatest {
		clusterpb.MetaPutObjectIndexCmdAddPreserveLatest(b, true)
	}
	return fbFinish(b, clusterpb.MetaPutObjectIndexCmdEnd(b)), nil
}

func decodeMetaPutObjectIndexCmd(data []byte) (metaPutObjectIndexCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPutObjectIndexCmd {
		return clusterpb.GetRootAsMetaPutObjectIndexCmd(d, 0)
	})
	if err != nil {
		return metaPutObjectIndexCmd{}, err
	}
	entry := t.Entry(nil)
	if entry == nil {
		return metaPutObjectIndexCmd{}, fmt.Errorf("nil entry")
	}
	return metaPutObjectIndexCmd{
		Entry:          readMetaObjectIndexEntry(entry),
		PreserveLatest: t.PreserveLatest(),
	}, nil
}

func encodeMetaDeleteObjectIndexCmd(bucket, key, versionID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	keyOff := b.CreateString(key)
	versionOff := b.CreateString(versionID)
	clusterpb.MetaDeleteObjectIndexCmdStart(b)
	clusterpb.MetaDeleteObjectIndexCmdAddBucket(b, bucketOff)
	clusterpb.MetaDeleteObjectIndexCmdAddKey(b, keyOff)
	clusterpb.MetaDeleteObjectIndexCmdAddVersionId(b, versionOff)
	return fbFinish(b, clusterpb.MetaDeleteObjectIndexCmdEnd(b)), nil
}

func decodeMetaDeleteObjectIndexCmd(data []byte) (bucket, key, versionID string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaDeleteObjectIndexCmd {
		return clusterpb.GetRootAsMetaDeleteObjectIndexCmd(d, 0)
	})
	if err != nil {
		return "", "", "", err
	}
	return string(t.Bucket()), string(t.Key()), string(t.VersionId()), nil
}

func buildMetaObjectIndexEntry(b *flatbuffers.Builder, entry objectIndexSnapshotEntry) flatbuffers.UOffsetT {
	e := entry.ObjectIndexEntry
	bucketOff := b.CreateString(e.Bucket)
	keyOff := b.CreateString(e.Key)
	versionOff := b.CreateString(e.VersionID)
	groupOff := b.CreateString(e.PlacementGroupID)
	contentTypeOff := b.CreateString(e.ContentType)
	etagOff := b.CreateString(e.ETag)
	var nodeIDsOff flatbuffers.UOffsetT
	if len(e.NodeIDs) > 0 {
		nodeIDsOff = buildStringVector(b, e.NodeIDs, clusterpb.MetaObjectIndexEntryStartNodeIdsVector)
	}
	// parts — build child MultipartPartEntry tables BEFORE MetaObjectIndexEntryStart.
	var partsOff flatbuffers.UOffsetT
	if len(e.Parts) > 0 {
		partOffs := make([]flatbuffers.UOffsetT, len(e.Parts))
		for i, p := range e.Parts {
			etOff := b.CreateString(p.ETag)
			clusterpb.MultipartPartEntryStart(b)
			clusterpb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
			clusterpb.MultipartPartEntryAddSize(b, p.Size)
			clusterpb.MultipartPartEntryAddEtag(b, etOff)
			partOffs[i] = clusterpb.MultipartPartEntryEnd(b)
		}
		clusterpb.MetaObjectIndexEntryStartPartsVector(b, len(partOffs))
		for i := len(partOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(partOffs[i])
		}
		partsOff = b.EndVector(len(partOffs))
	}
	clusterpb.MetaObjectIndexEntryStart(b)
	clusterpb.MetaObjectIndexEntryAddBucket(b, bucketOff)
	clusterpb.MetaObjectIndexEntryAddKey(b, keyOff)
	clusterpb.MetaObjectIndexEntryAddVersionId(b, versionOff)
	clusterpb.MetaObjectIndexEntryAddPlacementGroupId(b, groupOff)
	clusterpb.MetaObjectIndexEntryAddSize(b, e.Size)
	clusterpb.MetaObjectIndexEntryAddContentType(b, contentTypeOff)
	clusterpb.MetaObjectIndexEntryAddEtag(b, etagOff)
	clusterpb.MetaObjectIndexEntryAddModTime(b, e.ModTime)
	clusterpb.MetaObjectIndexEntryAddEcData(b, e.ECData)
	clusterpb.MetaObjectIndexEntryAddEcParity(b, e.ECParity)
	if nodeIDsOff != 0 {
		clusterpb.MetaObjectIndexEntryAddNodeIds(b, nodeIDsOff)
	}
	if e.IsDeleteMarker {
		clusterpb.MetaObjectIndexEntryAddIsDeleteMarker(b, true)
	}
	if entry.IsLatest {
		clusterpb.MetaObjectIndexEntryAddIsLatest(b, true)
	}
	if partsOff != 0 {
		clusterpb.MetaObjectIndexEntryAddParts(b, partsOff)
	}
	if e.DekGen != 0 {
		clusterpb.MetaObjectIndexEntryAddDekGen(b, e.DekGen)
	}
	return clusterpb.MetaObjectIndexEntryEnd(b)
}

func buildMetaObjectIndexEntriesVector(b *flatbuffers.Builder, entries []objectIndexSnapshotEntry) flatbuffers.UOffsetT {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].sortKey < entries[j].sortKey
	})
	offsets := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		offsets[i] = buildMetaObjectIndexEntry(b, entries[i])
	}
	clusterpb.MetaStateSnapshotStartObjectIndexVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

func readMetaObjectIndexEntry(entry *clusterpb.MetaObjectIndexEntry) ObjectIndexEntry {
	var parts []storage.MultipartPartEntry
	if n := entry.PartsLength(); n > 0 {
		parts = make([]storage.MultipartPartEntry, n)
		var pe clusterpb.MultipartPartEntry
		for i := 0; i < n; i++ {
			if !entry.Parts(&pe, i) {
				continue
			}
			parts[i] = storage.MultipartPartEntry{
				PartNumber: int(pe.PartNumber()),
				Size:       pe.Size(),
				ETag:       string(pe.Etag()),
			}
		}
	}
	return ObjectIndexEntry{
		Bucket:           string(entry.Bucket()),
		Key:              string(entry.Key()),
		VersionID:        string(entry.VersionId()),
		PlacementGroupID: string(entry.PlacementGroupId()),
		Size:             entry.Size(),
		ContentType:      string(entry.ContentType()),
		ETag:             string(entry.Etag()),
		ModTime:          entry.ModTime(),
		ECData:           entry.EcData(),
		ECParity:         entry.EcParity(),
		NodeIDs:          readStringVector(entry.NodeIdsLength(), entry.NodeIds),
		IsDeleteMarker:   entry.IsDeleteMarker(),
		Parts:            parts,
		DekGen:           entry.DekGen(),
	}
}
