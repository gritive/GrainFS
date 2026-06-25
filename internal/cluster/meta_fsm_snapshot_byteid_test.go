package cluster

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// byteIdentityMetaFSMFixtures returns MetaFSM fixtures whose FlatBuffers-root
// (the deterministic MetaStateSnapshot before envelope sealing + trailers)
// bytes are frozen as golden constants captured from clean HEAD, before the
// Snapshot() encode decomposition into per-section build*Vector helpers.
//
// DETERMINISM CONTRACT: every MAP-sourced section (nodes, shardGroups,
// bucketRecords, loadSnapshot, peers/byNodeID, revokedPeerSpkis/deny) is
// limited to <=1 element per fixture — Go randomizes map iteration order, so a
// multi-element map section would flap the golden hex run-to-run. Multi-element
// coverage comes only from SLICE/sorted sections (placementGenerations,
// lastRotationRequests, kekStatuses, revokedNodeIds [sorted]) and from NESTED
// slices (multiple PeerIDs in one shardGroup, multiple groupIDs in one
// generation), which are deterministic.
//
// Each fixture is constructed WITHOUT a DEKKeeper so no DKVS trailer is
// emitted; the test goldens only trailers.fbData (the FB root), which carries
// no encrypted DEK regardless.
func byteIdentityMetaFSMFixtures(t *testing.T) map[string]*MetaFSM {
	t.Helper()

	empty := func() *MetaFSM {
		f := NewMetaFSM()
		wireSnapshotKEK(t, f)
		return f
	}

	// emptyFSM: all-empty vectors and default cluster config.
	emptyFSM := empty()

	// singleMaps: exactly one element in each map-sourced section, plus
	// present bucket policy, multi-element nested slices, and sorted
	// multi-element slice sections.
	singleMaps := empty()
	singleMaps.nodes = map[string]MetaNodeEntry{
		"n0": {ID: "n0", Address: "127.0.0.1:7000", Role: 1},
	}
	singleMaps.shardGroups = map[string]ShardGroupEntry{
		"g0": {ID: "g0", PeerIDs: []string{"n0", "n1", "n2"}},
	}
	singleMaps.placementGenerations = []placementGeneration{
		{epoch: 0, groupIDs: []string{"g0"}},
		{epoch: 5, groupIDs: []string{"g0", "g1", "g2"}},
	}
	singleMaps.bucketRecords = map[string]BucketRecord{
		"bk": {GroupID: "g0", Versioning: "Enabled", Policy: []byte(`{"v":1}`)},
	}
	singleMaps.loadSnapshot = map[string]LoadStatEntry{
		"n0": {
			NodeID: "n0", DiskUsedPct: 42.5, DiskAvailBytes: 1 << 30,
			RequestsPerSec: 12.5, UpdatedAt: time.Unix(1700000000, 0),
		},
	}
	singleMaps.lastRotationRequests = []rotationRequestRecord{
		{requestID: [16]byte{1, 2, 3}, status: RotationStatus(1), applyIndex: 10},
		{requestID: [16]byte{4, 5, 6}, status: RotationStatus(2), applyIndex: 20},
	}
	singleMaps.kekStatuses = []kekStatusRecord{
		{version: 1, status: KEKLifecycleStatus(0), retireCommitIndex: 0},
		{version: 2, status: KEKLifecycleStatus(1), retireCommitIndex: 99},
	}
	require.NoError(t, singleMaps.peers.registerMember("a", spki(0x11), "addr-a", true, 7))
	// Denylist one SPKI (single element keeps revokedPeerSpkis deterministic).
	singleMaps.peers.denylist(spki(0x22))
	singleMaps.revokedNodeIDs = map[string]struct{}{
		"z-node": {}, "a-node": {}, "m-node": {},
	}
	singleMaps.clusterKeyDropped = true
	singleMaps.presentFlipBegun = true

	// emptyPolicy: bucket present but Policy nil (exercises the conditional
	// policy byte-vector emission — must NOT emit an empty policy vector).
	emptyPolicy := empty()
	emptyPolicy.bucketRecords = map[string]BucketRecord{
		"bk2": {GroupID: "g9", Versioning: "", Policy: nil},
	}

	return map[string]*MetaFSM{
		"empty":        emptyFSM,
		"single_maps":  singleMaps,
		"empty_policy": emptyPolicy,
	}
}

// goldenMetaSnapshotFB freezes the FlatBuffers-root hex for each fixture,
// captured from clean HEAD before the build*Vector decomposition.
var goldenMetaSnapshotFB = map[string]string{
	"empty":        "3000000000002a00380034003000240020000000000000001c0018000000140010000c0008000000000004002c0028002a0000003400000034000000340000003400000034000000340000004c0000004c0000004c000000500000005000000050000000400000000000000000000000000000000000000000000000180000000c00000008000c0000000400080000000028d3ed7cc7ffff00000000000000000000000000000000000000000000000000000000",
	"empty_policy": "3000000000002a00380034003000240020000000000000001c0018000000140010000c0008000000000004002c0028002a0000003400000034000000340000003400000034000000340000004c0000004c0000004c000000880000008800000088000000780000000000000000000000000000000000000000000000180000000c00000008000c0000000400080000000028d3ed7cc7ffff0000000000000000010000001000000000000a0010000c00080004000a0000000c00000010000000140000000000000000000000020000006739000003000000626b320000000000000000000000000000000000",
	"single_maps":  "340000000000000000002a003c00380034002800240000000000000020001c0000001800140010000c000b000a00040030002c002a00000038000000000001016400000094000000f8000000280100009c010000b4010000b4010000f801000084020000e4020000e40200003802000003000000240000001400000004000000060000007a2d6e6f64650000060000006d2d6e6f6465000006000000612d6e6f6465000001000000040000005effffff04000000200000002200000000000000000000000000000000000000000000000000000000000000010000001400000010001800140010000c000b000a0004001000000007000000000001010c000000140000003400000006000000616464722d610000200000001100000000000000000000000000000000000000000000000000000000000000010000006100000002000000100000001400000000000600080004000600000001000000daffffff630000000000000000000000000000010200000002000000140000004800000000000a0018001400130004000a0000000a00000000000000000000000000000104000000100000000102030000000000000000000000000000000a00140010000f0004000a000000140000000000000000000002040000001000000004050600000000000000000000000000180000000c00000008000c0000000400080000000028d3ed7cc7ffff00000000010000001400000000000e00280024001c0014000c0004000e00000000f153650000000000000000000029400000004000000000000000000040454004000000020000006e30000001000000100000000c00140010000c00080004000c00000010000000180000002000000024000000070000007b2276223a317d0007000000456e61626c656400020000006730000002000000626b0000010000001000000000000a0010000c00080007000a0000000000000108000000180000000e0000003132372e302e302e313a373030300000020000006e3000000200000010000000240000000800080000000400080000000400000001000000040000000200000067300000bcffffff0c0000000500000000000000030000000c000000100000001400000002000000673000000200000067310000020000006732000000000000010000000c00000008000c000800040008000000080000002c000000030000000c0000001000000014000000020000006e300000020000006e310000020000006e3200000200000067300000",
}

// To refresh the golden map above after an INTENTIONAL wire change, log the
// current FB-root output from within the assertion test below (e.g.
// t.Logf("%q: %s", name, hex.EncodeToString(trailers.fbData))) and paste the
// values back into goldenMetaSnapshotFB.

// TestByteIdentity_MetaFSMSnapshot asserts the MetaStateSnapshot FlatBuffers
// root produced by (*MetaFSM).Snapshot() is byte-identical to the frozen golden
// captured from clean HEAD. It peels the non-deterministic envelope (random
// ephemeral DEK + AES-GCM nonce + fresh UUIDv7) down to the deterministic FB
// root so the assertion is stable across runs. This guards the encode-side
// build*Vector decomposition: a reorder can round-trip (semantic) yet drift
// bytes — only this golden catches that.
func TestByteIdentity_MetaFSMSnapshot(t *testing.T) {
	for name, f := range byteIdentityMetaFSMFixtures(t) {
		want, ok := goldenMetaSnapshotFB[name]
		require.True(t, ok, "missing golden for %q", name)

		sealed, err := f.Snapshot()
		require.NoError(t, err)
		plain, err := f.openSnapshotEnvelope(sealed)
		require.NoError(t, err)
		trailers, err := peelMetaSnapshotTrailers(plain)
		require.NoError(t, err)

		require.Equal(t, want, hex.EncodeToString(trailers.fbData),
			"MetaStateSnapshot FB-root wire bytes drifted for %q", name)
	}
}
