package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestMultipartUploadIDEncodeParse(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		wantGroupID string
		wantRaw     string
		wantOK      bool
	}{
		{name: "round-trip", id: encodeMultipartUploadID("group-7", "raw-id-1"), wantGroupID: "group-7", wantRaw: "raw-id-1", wantOK: true},
		{name: "legacy un-prefixed", id: "raw-id-1", wantOK: false},
		{name: "empty", id: "", wantOK: false},
		{name: "prefix only", id: "mpg:", wantOK: false},
		{name: "missing raw", id: "mpg:group-7:", wantOK: false},
		{name: "missing group", id: "mpg::raw", wantOK: false},
		{name: "no separator after group", id: "mpg:group-7", wantOK: false},
		{name: "raw with colon kept intact", id: "mpg:group-7:raw:with:colons", wantGroupID: "group-7", wantRaw: "raw:with:colons", wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupID, raw, ok := parseMultipartUploadID(tt.id)
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.Equal(t, tt.wantGroupID, groupID)
				require.Equal(t, tt.wantRaw, raw)
			}
		})
	}
}

// divergentMultipartCluster wires TWO coordinators over the SAME data groups
// but with DIVERGENT boot-frozen placement candidate sets, reproducing a
// dynamically-grown cluster: coordinator A froze its candidate set when only
// group-1..group-3 existed; group-4/group-5 then joined WITHOUT an FSM
// placement generation (expandShardGroupsForJoinedNode records none), so
// coordinator B's boot freeze sees group-1..group-5. The same (bucket, key)
// hashes to DIFFERENT groups on A and B.
type divergentMultipartCluster struct {
	coordA *ClusterCoordinator // boot-frozen candidates: group-1..group-3
	coordB *ClusterCoordinator // boot-frozen candidates: group-1..group-5
	setA   []string
	setB   []string
}

func newDivergentMultipartCluster(t *testing.T) *divergentMultipartCluster {
	t.Helper()
	const selfID = "test-node"
	mgr := NewDataGroupManager()
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{}}
	addGroup := func(id string) {
		gb := newTestGroupBackend(t, id)
		require.NoError(t, gb.CreateBucket(context.Background(), "bk"))
		mgr.Add(NewDataGroupWithBackend(id, []string{selfID}, gb))
		meta.groups[id] = ShardGroupEntry{ID: id, PeerIDs: []string{selfID}}
	}

	setA := []string{"group-1", "group-2", "group-3"}
	setB := []string{"group-1", "group-2", "group-3", "group-4", "group-5"}
	for _, id := range setA {
		addGroup(id)
	}
	router := NewRouter(mgr)
	router.AssignBucket("bk", "group-1")
	ec := ECConfig{DataShards: 1, ParityShards: 0}

	coordA := NewClusterCoordinator(&fakeBackend{}, mgr, router, meta, selfID).WithECConfig(ec)
	// Dynamic join growth after A's boot freeze — no placement generation.
	for _, id := range setB[len(setA):] {
		addGroup(id)
	}
	coordB := NewClusterCoordinator(&fakeBackend{}, mgr, router, meta, selfID).WithECConfig(ec)

	require.Equal(t, setA, coordA.runtimeState().opRouter.currentPlacementGroupIDs(), "A must be boot-frozen at 3 groups")
	require.Equal(t, setB, coordB.runtimeState().opRouter.currentPlacementGroupIDs(), "B must be boot-frozen at 5 groups")
	return &divergentMultipartCluster{coordA: coordA, coordB: coordB, setA: setA, setB: setB}
}

// divergentKey returns a key whose hash placement differs between the two
// boot-frozen candidate sets — the misroute precondition.
func (c *divergentMultipartCluster) divergentKey(t *testing.T) string {
	t.Helper()
	for i := range 256 {
		key := fmt.Sprintf("mp/cross-node-%d.bin", i)
		if groupIDForObject("bk", key, c.setA) != groupIDForObject("bk", key, c.setB) {
			return key
		}
	}
	t.Fatal("no key with divergent placement found")
	return ""
}

// TestMultipartSessionOps_SurviveDivergentPlacement is the regression test for
// the cross-node multipart misroute: session ops (UploadPart / ListParts /
// Complete) issued on a node whose boot-frozen placement candidate set differs
// from the creating node's must still reach the group that owns the session.
// The uploadID returned by CreateMultipartUpload encodes the owning group
// (mpg:<groupID>:<raw>), so session ops route by it instead of re-hashing
// against the local (divergent) candidate set.
//
// RED without the fix: coordA hashes (bucket, key) onto a different group than
// coordB used at create time → NoSuchUpload (storage.ErrUploadNotFound).
func TestMultipartSessionOps_SurviveDivergentPlacement(t *testing.T) {
	cl := newDivergentMultipartCluster(t)
	ctx := context.Background()
	key := cl.divergentKey(t)

	up, err := cl.coordB.CreateMultipartUpload(ctx, "bk", key, "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, up.UploadID)

	// Session ops on coordinator A (divergent placement view).
	parts, err := cl.coordA.ListParts(ctx, "bk", key, up.UploadID, 100)
	require.NoError(t, err, "ListParts on a non-creating node must find the session")
	require.Empty(t, parts)

	part, err := cl.coordA.UploadPart(ctx, "bk", key, up.UploadID, 1, strings.NewReader("cross-node-part-payload"), "")
	require.NoError(t, err, "UploadPart on a non-creating node must find the session")

	parts, err = cl.coordA.ListParts(ctx, "bk", key, up.UploadID, 100)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Equal(t, part.ETag, parts[0].ETag)

	obj, err := cl.coordA.CompleteMultipartUpload(ctx, "bk", key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err, "Complete on a non-creating node must find the session")
	require.Equal(t, int64(len("cross-node-part-payload")), obj.Size)
}

// TestMultipartAbort_SurvivesDivergentPlacement covers the Abort leg and the
// listing round-trip: ListMultipartUploads must return the SAME group-encoded
// uploadID Create returned (from any node), and that listed ID must abort
// successfully on the non-creating node.
func TestMultipartAbort_SurvivesDivergentPlacement(t *testing.T) {
	cl := newDivergentMultipartCluster(t)
	ctx := context.Background()
	key := cl.divergentKey(t)

	up, err := cl.coordB.CreateMultipartUpload(ctx, "bk", key, "application/octet-stream")
	require.NoError(t, err)

	listed, err := cl.coordA.ListMultipartUploads(ctx, "bk", "", 100)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, up.UploadID, listed[0].UploadID,
		"listing must return the same group-encoded uploadID Create returned")

	require.NoError(t, cl.coordA.AbortMultipartUpload(ctx, "bk", key, listed[0].UploadID),
		"Abort on a non-creating node must find the session")

	listed, err = cl.coordA.ListMultipartUploads(ctx, "bk", "", 100)
	require.NoError(t, err)
	require.Empty(t, listed)
}

// TestMultipartScan_RecordsCarryOwningGroup covers the lifecycle abort leg:
// ScanLocalMultipartUploads fans out over locally-owned groups, and the
// lifecycle MPU worker feeds each record's UploadID straight back into
// AbortMultipartUpload. The scan must therefore re-encode the per-group raw ID
// with its owning group — otherwise expired MPUs abort via the legacy hash
// fallback and leak on nodes with divergent boot-frozen placement.
func TestMultipartScan_RecordsCarryOwningGroup(t *testing.T) {
	cl := newDivergentMultipartCluster(t)
	ctx := context.Background()
	key := cl.divergentKey(t)

	up, err := cl.coordB.CreateMultipartUpload(ctx, "bk", key, "application/octet-stream")
	require.NoError(t, err)
	owningGroup, _, ok := parseMultipartUploadID(up.UploadID)
	require.True(t, ok)

	ch, err := cl.coordA.ScanLocalMultipartUploads("bk")
	require.NoError(t, err)
	var recs []storage.MultipartUploadRecord
	for rec := range ch {
		recs = append(recs, rec)
	}
	require.Len(t, recs, 1)
	require.Equal(t, up.UploadID, recs[0].UploadID,
		"scan must return the same group-encoded uploadID Create returned")
	groupID, _, ok := parseMultipartUploadID(recs[0].UploadID)
	require.True(t, ok, "scanned uploadID must be group-encoded")
	require.Equal(t, owningGroup, groupID)

	// The lifecycle worker aborts with the scanned ID verbatim — must route to
	// the owning group even on the divergent coordinator.
	require.NoError(t, cl.coordA.AbortMultipartUpload(ctx, "bk", key, recs[0].UploadID))

	ch, err = cl.coordA.ScanLocalMultipartUploads("bk")
	require.NoError(t, err)
	for range ch {
		t.Fatal("scan must be empty after abort")
	}
}

// TestMultipartCreateWithTags_EncodesGroup covers the tags-create overload:
// the returned ID must be group-encoded the same way so all session ops route
// consistently.
func TestMultipartCreateWithTags_EncodesGroup(t *testing.T) {
	cl := newDivergentMultipartCluster(t)
	ctx := context.Background()
	key := cl.divergentKey(t)

	id, err := cl.coordB.CreateMultipartUploadWithTags(ctx, "bk", key, "application/octet-stream", []storage.Tag{{Key: "env", Value: "test"}})
	require.NoError(t, err)
	groupID, _, ok := parseMultipartUploadID(id)
	require.True(t, ok, "tags-create must return a group-encoded uploadID")
	require.Equal(t, groupIDForObject("bk", key, cl.setB), groupID)

	require.NoError(t, cl.coordA.AbortMultipartUpload(ctx, "bk", key, id))
}
