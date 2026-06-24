package cluster

import (
	"bytes"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/group"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// ipstDecoded holds the three IAM policy stores decoded from the IPST trailer,
// staged for the commit phase. (§2 stores: policy / group / policy-attach.)
type ipstDecoded struct {
	polSnap    []policystore.PolicyEntry
	grpSnap    []group.GroupEntry
	attachSnap policyattach.AttachSnapshot
}

// restoredMetaState is the package-private carrier that threads every value
// decoded during Restore's DECODE PHASE into the COMMIT and SIDE-EFFECTS
// phases. It exists so the decode sub-functions can be split out of the
// monolithic Restore without capturing locals in closures: explicit data flow
// is easier to review and reason about than closure capture, and it preserves
// the invariant that NO f.* field is written until commitCoreState.
//
// Field groups mirror the snapshot sections (core topology / DEK / satellite
// stores / peer registry); each group was a cluster of locals in the original
// 570-LOC function, so the carrier adds no new state, only structure.
type restoredMetaState struct {
	// --- core topology ---
	nodes                map[string]MetaNodeEntry
	shardGroups          map[string]ShardGroupEntry
	placementGenerations []placementGeneration
	bucketRecords        map[string]BucketRecord
	loadSnapshot         map[string]LoadStatEntry
	activePlan           *RebalancePlan
	lastRotationRequests []rotationRequestRecord
	kekStatuses          []kekStatusRecord
	clusterKeyDropped    bool
	presentFlipBegun     bool
	revokedNodeIDs       map[string]struct{}
	newClusterCfgSnap    *clusterConfigSnap

	// --- DEK (R2 two-pass decode, pass 1) ---
	hasDEKData       bool
	dekVersions      map[uint32][]byte
	dekActive        uint32
	dekRefs          map[uint32]uint64
	activeKEKVersion uint32
	dekRewrapDone    map[uint32]map[string]uint32

	// --- satellite stores (decoded, pending commit) ---
	iamTempStore               *iam.Store
	cfgValues                  map[string]string
	ipst                       *ipstDecoded
	protocolCredentials        []protocred.Credential
	protocolCredentialRequests []ProtocolCredentialRequestRecord
	restoreProtocolCredentials bool

	// --- JKEY (pre-validated scratch KeySet) ---
	hasJKEYData    bool
	jkeyCurrent    *iamjwt.KeySeed
	jkeyPrevious   *iamjwt.KeySeed
	scratchJWTKeys *iamjwt.KeySet

	// --- peer registry (pre-validated indexes) ---
	peersByNodeID map[string]peerEntry
	peersBySPKI   map[[32]byte]string
	peers         []peerEntry
	deny          map[[32]byte]struct{}
}

// parseSnapshotRoot opens the snapshot envelope, peels the binary trailers, and
// parses the FlatBuffers MetaStateSnapshot root (recover-guarded against a
// malformed buffer). It touches no f.* field. (Restore L399-427.)
func (f *MetaFSM) parseSnapshotRoot(data []byte) (*clusterpb.MetaStateSnapshot, metaSnapshotTrailers, error) {
	var trailers metaSnapshotTrailers

	plain, err := f.openSnapshotEnvelope(data)
	if err != nil {
		return nil, trailers, err
	}

	trailers, err = peelMetaSnapshotTrailers(plain)
	if err != nil {
		return nil, trailers, err
	}

	var (
		snap   *clusterpb.MetaStateSnapshot
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaStateSnapshot flatbuffer: %v", r)
			}
		}()
		snap = clusterpb.GetRootAsMetaStateSnapshot(trailers.fbData, 0)
	}()
	if decErr != nil {
		return nil, trailers, decErr
	}
	return snap, trailers, nil
}

// decodeCoreTopology decodes nodes / shard groups / placement generations /
// bucket records / load snapshot / active plan into the carrier. Pure decode —
// no f.* writes. (Restore L429-530.)
func decodeCoreTopology(snap *clusterpb.MetaStateSnapshot, st *restoredMetaState) error {
	newNodes := make(map[string]MetaNodeEntry, snap.NodesLength())
	var nodeEntry clusterpb.MetaNodeEntry
	for i := 0; i < snap.NodesLength(); i++ {
		if snap.Nodes(&nodeEntry, i) {
			e := MetaNodeEntry{
				ID:      string(nodeEntry.Id()),
				Address: string(nodeEntry.Address()),
				Role:    nodeEntry.Role(),
			}
			newNodes[e.ID] = e
		}
	}

	newShardGroups := make(map[string]ShardGroupEntry, snap.ShardGroupsLength())
	var sgEntry clusterpb.ShardGroupEntry
	for i := 0; i < snap.ShardGroupsLength(); i++ {
		if !snap.ShardGroups(&sgEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: shard group %d decode failed", i)
		}
		peers := make([]string, sgEntry.PeerIdsLength())
		for j := 0; j < sgEntry.PeerIdsLength(); j++ {
			peers[j] = string(sgEntry.PeerIds(j))
		}
		e := ShardGroupEntry{
			ID:      string(sgEntry.Id()),
			PeerIDs: peers,
		}
		// Mirror applyPutShardGroup: drop reserved IDs so log-replay and
		// snapshot-restore land on the same FSM state. Without this, a node
		// that joins via snapshot install would carry a reserved ID while a
		// peer that replayed from log would not — silent quorum divergence.
		// Pre-v0.0.19 snapshots may still contain such IDs; we skip them.
		if err := raft.ValidateGroupID(e.ID); err != nil {
			log.Warn().Err(err).Str("group_id", e.ID).Msg("meta_fsm: Restore: dropping reserved group ID from snapshot")
			continue
		}
		newShardGroups[e.ID] = e
	}

	// Phase 7 placement-generation registry. Missing slot (legacy snapshots) →
	// PlacementGenerationsLength()==0 → nil, preserving single-generation behavior.
	var newPlacementGenerations []placementGeneration
	if n := snap.PlacementGenerationsLength(); n > 0 {
		newPlacementGenerations = make([]placementGeneration, 0, n)
		var pgEntry clusterpb.PlacementGenerationEntry
		for i := 0; i < n; i++ {
			if !snap.PlacementGenerations(&pgEntry, i) {
				return fmt.Errorf("meta_fsm: Restore: placement generation %d decode failed", i)
			}
			ids := make([]string, pgEntry.GroupIdsLength())
			for j := 0; j < pgEntry.GroupIdsLength(); j++ {
				ids[j] = string(pgEntry.GroupIds(j))
			}
			newPlacementGenerations = append(newPlacementGenerations, placementGeneration{epoch: pgEntry.Epoch(), groupIDs: ids})
		}
	}

	newBucketRecords := make(map[string]BucketRecord, snap.BucketAssignmentsLength())
	var baEntry clusterpb.BucketAssignmentEntry
	for i := 0; i < snap.BucketAssignmentsLength(); i++ {
		if !snap.BucketAssignments(&baEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: bucket assignment %d decode failed", i)
		}
		bucket := string(baEntry.Bucket())
		var policy []byte
		if pb := baEntry.PolicyBytes(); len(pb) > 0 {
			policy = append([]byte(nil), pb...)
		}
		newBucketRecords[bucket] = BucketRecord{
			GroupID:    string(baEntry.GroupId()),
			Versioning: string(baEntry.Versioning()),
			Policy:     policy,
		}
	}

	newLoadSnapshot := make(map[string]LoadStatEntry, snap.LoadSnapshotLength())
	var lsEntry clusterpb.LoadStatEntry
	for i := 0; i < snap.LoadSnapshotLength(); i++ {
		if !snap.LoadSnapshot(&lsEntry, i) {
			continue
		}
		e := LoadStatEntry{
			NodeID:         string(lsEntry.NodeId()),
			DiskUsedPct:    lsEntry.DiskUsedPct(),
			DiskAvailBytes: lsEntry.DiskAvailBytes(),
			RequestsPerSec: lsEntry.RequestsPerSec(),
			UpdatedAt:      time.Unix(lsEntry.UpdatedAtUnix(), 0),
		}
		newLoadSnapshot[e.NodeID] = e
	}

	var newActivePlan *RebalancePlan
	var planFB clusterpb.RebalancePlan
	if p := snap.ActivePlan(&planFB); p != nil && len(p.PlanId()) > 0 {
		newActivePlan = &RebalancePlan{
			PlanID:    string(p.PlanId()),
			GroupID:   string(p.GroupId()),
			FromNode:  string(p.FromNode()),
			ToNode:    string(p.ToNode()),
			CreatedAt: time.Unix(p.CreatedAtUnix(), 0),
		}
	}

	st.nodes = newNodes
	st.shardGroups = newShardGroups
	st.placementGenerations = newPlacementGenerations
	st.bucketRecords = newBucketRecords
	st.loadSnapshot = newLoadSnapshot
	st.activePlan = newActivePlan
	return nil
}

// decodeClusterConfig decodes the embedded ClusterConfig FBS blob. Empty/missing
// blob (legacy pre-Slice-1 snapshots) leaves st.newClusterCfgSnap nil, so the
// commit leaves the existing clusterCfg untouched. (Restore L532-544.)
func decodeClusterConfig(snap *clusterpb.MetaStateSnapshot, st *restoredMetaState) error {
	if snap.ClusterConfigLength() == 0 {
		return nil
	}
	cs, err := deserializeClusterConfig(snap.ClusterConfigBytes())
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: decode cluster config: %w", err)
	}
	st.newClusterCfgSnap = cs
	return nil
}

// decodeKEKRotationState decodes last_rotation_request_entries (slot 11) and
// kek_status_entries (slot 12). (Restore L546-574.)
func decodeKEKRotationState(snap *clusterpb.MetaStateSnapshot, st *restoredMetaState) error {
	newLastRotationRequests := make([]rotationRequestRecord, 0, snap.LastRotationRequestEntriesLength())
	var lrrFB clusterpb.LastRotationRequestEntry
	for i := 0; i < snap.LastRotationRequestEntriesLength(); i++ {
		if !snap.LastRotationRequestEntries(&lrrFB, i) {
			return fmt.Errorf("meta_fsm: Restore: last_rotation_request_entries[%d] decode failed", i)
		}
		var rid [16]byte
		copy(rid[:], lrrFB.RequestIdBytes())
		newLastRotationRequests = append(newLastRotationRequests, rotationRequestRecord{
			requestID:  rid,
			status:     RotationStatus(lrrFB.Status()),
			applyIndex: lrrFB.ApplyIndex(),
		})
	}

	newKEKStatuses := make([]kekStatusRecord, 0, snap.KekStatusEntriesLength())
	var kekFB clusterpb.KEKStatusEntry
	for i := 0; i < snap.KekStatusEntriesLength(); i++ {
		if !snap.KekStatusEntries(&kekFB, i) {
			return fmt.Errorf("meta_fsm: Restore: kek_status_entries[%d] decode failed", i)
		}
		newKEKStatuses = append(newKEKStatuses, kekStatusRecord{
			version:           kekFB.Version(),
			status:            KEKLifecycleStatus(kekFB.Status()),
			retireCommitIndex: kekFB.RetireCommitIndex(),
		})
	}

	st.lastRotationRequests = newLastRotationRequests
	st.kekStatuses = newKEKStatuses
	return nil
}

// decodePeerRegistry decodes the zero-CA peer registry (slot 13), validates +
// builds the peer indexes, decodes the denylist (slot 14) and the
// cluster-key-dropped (slot 15) / present-flip-begun / revoked-node (slot 17)
// bits. Validation runs HERE in the decode phase so a corrupt peer vector fails
// before any core FSM state is committed (meta-raft invariant). (Restore L576-650.)
func decodePeerRegistry(snap *clusterpb.MetaStateSnapshot, st *restoredMetaState) error {
	// zero-CA peer registry (Task 5, slot 13): decode every PeerEntry. A missing
	// vector (legacy pre-Task-5 snapshot) yields an empty slice — the commit then
	// clears the registry, matching a fresh node with no membership yet.
	newPeers := make([]peerEntry, 0, snap.PeersLength())
	var peerFB clusterpb.PeerEntry
	for i := 0; i < snap.PeersLength(); i++ {
		if !snap.Peers(&peerFB, i) {
			return fmt.Errorf("meta_fsm: Restore: peers[%d] decode failed", i)
		}
		// SPKI MUST be exactly 32 bytes. copy() into [32]byte would silently
		// truncate/zero-pad a malformed length, so reject BEFORE the copy — a
		// corrupt meta snapshot is fatal, matching the decode-failure convention
		// above (deterministic across nodes: same bytes → same hard-error).
		if n := len(peerFB.SpkiBytes()); n != 32 {
			return fmt.Errorf("meta_fsm: Restore: peers[%d] SPKI length %d, want 32", i, n)
		}
		var spki [32]byte
		copy(spki[:], peerFB.SpkiBytes())
		newPeers = append(newPeers, peerEntry{
			NodeID:          string(peerFB.NodeId()),
			SPKI:            spki,
			Address:         string(peerFB.Address()),
			State:           peerState(peerFB.State()),
			PresentsPerNode: peerFB.PresentsPerNode(),
			NodeKeyKEKGen:   peerFB.NodeKeyKekGen(),
		})
	}
	// VALIDATE + BUILD the peer indexes HERE, in the decode phase, so a corrupt
	// peer vector (duplicate node ID / duplicate SPKI / bad state) fails BEFORE
	// any core FSM state is committed below. Previously peer import ran AFTER
	// the f.nodes/shardGroups commit, leaving a FAILED Restore with
	// partially-mutated core state (violating the meta-raft invariant that a
	// failed Restore leaves the FSM un-restored). The commit phase swaps these
	// pre-validated maps in via commitPeerIndexes, which cannot fail.
	newPeersByNodeID, newPeersBySPKI, err := validatePeerEntries(newPeers)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: peer registry validate: %w", err)
	}

	newDenyEntries := make([][32]byte, 0, snap.RevokedPeerSpkisLength())
	var denyFB clusterpb.SPKIBytes
	for i := 0; i < snap.RevokedPeerSpkisLength(); i++ {
		if !snap.RevokedPeerSpkis(&denyFB, i) {
			return fmt.Errorf("meta_fsm: Restore: revoked_peer_spkis[%d] decode failed", i)
		}
		raw := denyFB.ValueBytes()
		if len(raw) != 32 {
			return fmt.Errorf("meta_fsm: Restore: revoked_peer_spkis[%d] SPKI length %d, want 32", i, len(raw))
		}
		var spki [32]byte
		copy(spki[:], raw)
		newDenyEntries = append(newDenyEntries, spki)
	}
	newDeny, err := validateDenylistEntries(newDenyEntries)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: denylist validate: %w", err)
	}

	// zero-CA cutover drop bit (slot 15, H3): decode into a local here so the
	// commit + callback-fire below use the captured value, never a field read
	// outside the lock. Legacy snapshots default false (FlatBuffer default).
	droppedDecoded := snap.ClusterKeyDropped()

	// zero-CA revoked-node set (slot 17): decode into a local map. A missing
	// vector (legacy snapshot) yields an empty set, matching a fresh FSM. The
	// evacuator re-derives from the restored set on its next tick, so no
	// onNodeRevoked callback is fired from Restore (mirrors onRebalancePlan).
	newRevokedNodeIDs := make(map[string]struct{}, snap.RevokedNodeIdsLength())
	for i := 0; i < snap.RevokedNodeIdsLength(); i++ {
		id := string(snap.RevokedNodeIds(i))
		if id == "" {
			continue
		}
		newRevokedNodeIDs[id] = struct{}{}
	}

	st.peers = newPeers
	st.peersByNodeID = newPeersByNodeID
	st.peersBySPKI = newPeersBySPKI
	st.deny = newDeny
	st.clusterKeyDropped = droppedDecoded
	st.presentFlipBegun = snap.PresentFlipBegun()
	st.revokedNodeIDs = newRevokedNodeIDs
	return nil
}

// decodeDEKTrailer decodes the DEK version snapshot (R2 two-pass decode, pass 1).
// This MUST run before decodeIAMTrailer, which needs the decoded DEK versions to
// build a transient read-only encryptor for the IAM payload. Gate on hasDEKData,
// NOT dekActive>0 — gen 0 is the legitimate genesis-bootstrap active gen.
// (Restore L663-682.)
func (f *MetaFSM) decodeDEKTrailer(trailers metaSnapshotTrailers, st *restoredMetaState) error {
	if len(trailers.dekData) == 0 {
		return nil
	}
	versions, active, refs, activeKEK, done, err := decodeMetaDEKVersionSnapshot(trailers.dekData)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: decode DEK versions: %w", err)
	}
	st.dekVersions = versions
	st.dekActive = active
	st.dekRefs = refs
	st.activeKEKVersion = activeKEK
	st.dekRewrapDone = done
	st.hasDEKData = true
	return nil
}

// decodeIAMTrailer decodes + validates the IAM trailer (R2 two-pass decode,
// pass 2) into a temporary store. It builds a transient read-only encryptor
// lazily from the DEK versions decoded by decodeDEKTrailer (which MUST have run
// first). An IAM trailer with no DEK trailer is a corrupt snapshot → hard-fail.
// (Restore L684-717.)
func (f *MetaFSM) decodeIAMTrailer(trailers metaSnapshotTrailers, st *restoredMetaState) error {
	if len(trailers.iamData) == 0 {
		return nil
	}
	if f.iamStore == nil || f.iamApplier == nil {
		log.Warn().Int("iam_len", len(trailers.iamData)).Msg("meta_fsm: Restore: snapshot contains IAM section but IAM not wired; skipping IAM restore")
		return nil
	}
	if !st.hasDEKData {
		// IAM trailer present but no DEK trailer — corrupt snapshot.
		// Format guard refuses pre-R2 dirs at boot, so this combination
		// should be unreachable in honest operation. Hard-fail rather
		// than warn-skip (which would silently lose IAM state).
		return fmt.Errorf("meta_fsm: Restore: IAM trailer present but DEK trailer missing — corrupt snapshot")
	}
	transient, err := encrypt.NewTransientReadOnlyDEK(f.clusterID[:], st.dekVersions, st.dekActive, st.activeKEKVersion, f.KEKStore())
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: build transient read-only DEK: %w", err)
	}
	iamDecodeEnc := storage.NewTransientDataEncryptor(transient, f.clusterID[:])
	tmp := iam.NewStore()
	if err := iam.ReadSnapshot(bytes.NewReader(trailers.iamData), tmp, iamDecodeEnc); err != nil {
		return fmt.Errorf("meta_fsm: Restore: decode IAM: %w", err)
	}
	st.iamTempStore = tmp
	return nil
}

// decodeConfigAndPolicyTrailers decodes the GCFG config values and the IPST
// IAM policy stores (§2 stores). Each warns-and-skips if its store is unwired.
// (Restore L719-750.)
func (f *MetaFSM) decodeConfigAndPolicyTrailers(trailers metaSnapshotTrailers, st *restoredMetaState) error {
	// GCFG: decode config values.
	if len(trailers.cfgData) > 0 {
		if f.cfgStore == nil {
			log.Warn().Int("cfg_len", len(trailers.cfgData)).Msg("meta_fsm: Restore: snapshot contains config section but config store not wired; skipping")
		} else {
			values, err := decodeMetaConfigSnapshot(trailers.cfgData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode config: %w", err)
			}
			st.cfgValues = values
		}
	}

	// IPST: decode IAM policy stores snapshot (§2 stores).
	if len(trailers.ipstData) > 0 {
		if f.policyStore == nil && f.groupStore == nil && f.policyAttachStore == nil {
			log.Warn().Int("ipst_len", len(trailers.ipstData)).Msg("meta_fsm: Restore: snapshot contains IPST section but no policy stores wired; skipping")
		} else {
			polSnap, grpSnap, attachSnap, _, err := decodeMetaIAMPolicyStoresSnapshot(trailers.ipstData)
			if err != nil {
				return fmt.Errorf("meta_fsm: Restore: decode IAM policy stores: %w", err)
			}
			st.ipst = &ipstDecoded{polSnap, grpSnap, attachSnap}
		}
	}
	return nil
}

// decodeProtocolCredentialTrailer decodes the protocol-credential rows +
// requests (warns-and-skips if the store is unwired). (Restore L752-766.)
func (f *MetaFSM) decodeProtocolCredentialTrailer(trailers metaSnapshotTrailers, st *restoredMetaState) error {
	st.restoreProtocolCredentials = f.protocolCredentialStore != nil
	if len(trailers.pcreData) == 0 {
		return nil
	}
	if !st.restoreProtocolCredentials {
		log.Warn().Int("pcre_len", len(trailers.pcreData)).Msg("meta_fsm: Restore: snapshot contains protocol credentials but store not wired; skipping")
		return nil
	}
	rows, requests, err := decodeProtocolCredentialsSnapshotState(trailers.pcreData)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: decode protocol credentials: %w", err)
	}
	st.protocolCredentials = rows
	st.protocolCredentialRequests = requests
	return nil
}

// decodeJWTKeysTrailer decodes the JWT signing keys and stages them onto a
// scratch KeySet via LoadFromSeeds BEFORE any f.* field is touched, so a
// partial-unwrap failure leaves jwtKeyStore/jwtKeys untouched (F14 atomicity).
// JKEY data with no DEK keeper wired is a hard-fail (F9). (Restore L768-801.)
func (f *MetaFSM) decodeJWTKeysTrailer(trailers metaSnapshotTrailers, st *restoredMetaState) error {
	st.hasJKEYData = len(trailers.jkeyData) > 0
	if !st.hasJKEYData {
		return nil
	}
	if f.dekKeeper == nil {
		return fmt.Errorf("meta_fsm: Restore: JKEY trailer present but DEK keeper not wired — cannot unwrap signing keys")
	}
	cur, prev, err := decodeJWTKeyStore(trailers.jkeyData)
	if err != nil {
		return fmt.Errorf("meta_fsm: Restore: decode JKEY: %w", err)
	}
	var seeds []iamjwt.KeySeed
	if cur != nil {
		seeds = append(seeds, *cur)
	}
	if prev != nil {
		seeds = append(seeds, *prev)
	}
	scratch := iamjwt.NewKeySet()
	if err := scratch.LoadFromSeeds(seeds, f.dekKeeper); err != nil {
		return fmt.Errorf("meta_fsm: Restore: JKEY LoadFromSeeds: %w", err)
	}
	st.jkeyCurrent = cur
	st.jkeyPrevious = prev
	st.scratchJWTKeys = scratch
	return nil
}

// commitCoreState swaps the decoded core topology + DEK pending fields +
// protocol-credential store into f.* under f.mu. No error is possible: every
// decode already succeeded. It returns the onBucketAssigned callback captured
// under the lock so commitSatelliteStores can fire it outside the lock — the
// same lock-in / fire-out pattern as the original. (Restore L806-862.)
func (f *MetaFSM) commitCoreState(st *restoredMetaState) func(bucket, groupID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes = st.nodes
	f.shardGroups = st.shardGroups
	f.placementGenerations = st.placementGenerations
	f.bucketRecords = st.bucketRecords
	f.loadSnapshot = st.loadSnapshot
	f.activePlan = st.activePlan
	f.lastRotationRequests = st.lastRotationRequests
	f.kekStatuses = st.kekStatuses
	f.clusterKeyDropped = st.clusterKeyDropped
	f.presentFlipBegun = st.presentFlipBegun
	f.revokedNodeIDs = st.revokedNodeIDs
	if st.hasDEKData {
		f.pendingDEKVersions = st.dekVersions
		f.pendingDEKActive = st.dekActive
		f.activeKEKVersion = st.activeKEKVersion
		// Task 4c: remember which KEK version the snapshot wraps were sealed under.
		// This field is read by SnapshotCapturedKEKVersion() → rebuildDEKKeeperFromRestore.
		// It must NOT be overwritten by KEKRotateCmd log replay, hence the separate field.
		f.pendingActiveKEKVersion = st.activeKEKVersion
		if st.dekRefs != nil {
			f.dekRefCounts = st.dekRefs
		} else {
			// Pre-Task-12 snapshot: no ref_counts trailer field. Object index was
			// removed in Phase 4; start with empty ref counts.
			f.dekRefCounts = make(map[uint32]uint64)
		}
		// Restore rewrap done set. Pre-S6d snapshots decode nil → empty map.
		if st.dekRewrapDone != nil {
			f.dekRewrapDone = st.dekRewrapDone
		} else {
			f.dekRewrapDone = nil
		}
	} else {
		// No DKVS trailer (the leader's keeper was empty when it snapshotted).
		// Symmetrically RESET the pending DEK fields so the post-restore DEK
		// install (installSnapshotDEKs on the live path, rebuildDEKKeeperFromRestore
		// on boot) sees an empty version set and no-ops, rather than installing
		// from STALE versions left over from a prior with-trailer restore. Without
		// this reset the no-op contract would be violated on a repeated live restore.
		f.pendingDEKVersions = nil
		f.pendingDEKActive = 0
		f.pendingActiveKEKVersion = 0
		// Symmetric with the with-trailer branch: clear stale rewrap-done entries
		// so a no-trailer restore onto an FSM that previously held completion
		// records cannot leave phantom prune-safety input behind.
		f.dekRewrapDone = nil
	}
	if st.restoreProtocolCredentials {
		f.protocolCredentialStore.Restore(st.protocolCredentials)
		f.protocolCredentialRequests = make(map[string]ProtocolCredentialRequestRecord, len(st.protocolCredentialRequests))
		for _, row := range st.protocolCredentialRequests {
			f.protocolCredentialRequests[row.RequestID] = row
		}
	}
	return f.onBucketAssigned
}

// commitSatelliteStores commits the cluster config, fires the bucket-assigned
// callback (outside f.mu), and commits the IAM / GCFG / IPST / JKEY stores. Each
// satellite store has its own mutex, so these run outside f.mu. All are
// documented atomic/no-error. (Restore L864-925.)
func (f *MetaFSM) commitSatelliteStores(st *restoredMetaState, onBucketAssigned func(bucket, groupID string)) {
	if st.newClusterCfgSnap != nil {
		f.clusterCfg.ReplaceSnap(st.newClusterCfgSnap)
	}
	if onBucketAssigned != nil {
		for bucket, rec := range st.bucketRecords {
			onBucketAssigned(bucket, rec.GroupID)
		}
	}
	// onRebalancePlan is intentionally NOT called here.
	// Rebalancer handles resume on next tick by checking ActivePlan().

	// IAM commit — iamTempStore holds the fully-decoded snapshot; swap it in atomically.
	// RestoreFrom copies the state pointer from iamTempStore into f.iamStore in one
	// atomic store — no second decode/parse is needed, so no error is possible here
	// (F17: eliminates the error-returning ReadSnapshot call after core fields commit).
	if st.iamTempStore != nil {
		f.iamStore.RestoreFrom(st.iamTempStore)
	}

	// GCFG commit.
	if st.cfgValues != nil {
		f.cfgStore.Restore(st.cfgValues)
	}

	// IPST commit — apply all §2 + §A policy stores.
	if st.ipst != nil {
		// Warn per nil store. The stores form a single coherent unit
		// (group memberships, attached policies, bucket policies all reference
		// each other); silently dropping one half desyncs the others against
		// the snapshot. The all-nil path warns once above; here we surface
		// per-store gaps so the operator sees exactly what was lost.
		if f.policyStore == nil {
			log.Warn().Int("entries", len(st.ipst.polSnap)).Msg("meta_fsm: Restore: IPST has policy entries but policyStore not wired; entries dropped")
		} else {
			f.policyStore.ReplaceAll(st.ipst.polSnap)
		}
		if f.groupStore == nil {
			log.Warn().Int("entries", len(st.ipst.grpSnap)).Msg("meta_fsm: Restore: IPST has group entries but groupStore not wired; entries dropped")
		} else {
			f.groupStore.ReplaceAll(st.ipst.grpSnap)
		}
		if f.policyAttachStore == nil {
			log.Warn().Int("sa_entries", len(st.ipst.attachSnap.SAAttachments)).Int("group_entries", len(st.ipst.attachSnap.GroupAttachments)).Msg("meta_fsm: Restore: IPST has policy-attach entries but policyAttachStore not wired; entries dropped")
		} else {
			f.policyAttachStore.ReplaceAll(st.ipst.attachSnap)
		}
		// Invalidate the resolver cache so stale pre-restore entries don't
		// survive the snapshot install. Empty saIDs+buckets nukes the full cache.
		if f.policyResolver != nil {
			f.policyResolver.Invalidate(nil, nil)
		}
	}

	// JKEY commit — both f.jwtKeyStore and f.jwtKeys are updated together.
	// scratchJWTKeys was built (and LoadFromSeeds succeeded) in the decode phase
	// above, so no error is possible here (F14: atomic commit).
	if st.hasJKEYData {
		f.jwtKeyStore.ReplaceAll(st.jkeyCurrent, st.jkeyPrevious)
		f.jwtKeys = st.scratchJWTKeys
	} else {
		f.jwtKeyStore.ReplaceAll(nil, nil)
	}
}

// commitPeerRegistryAndFireCallbacks is the LAST step of Restore: it swaps in the
// pre-validated peer registry indexes + denylist, fires firePeersChanged (which
// rebuilds the transport accept-set), then fires the cluster-key-dropped and
// present-flip boot callbacks. These side effects fire ONLY after Restore is
// guaranteed to return nil — every preceding store commit is documented
// atomic/no-error, and nothing after this point can err. (Restore L927-967.)
func (f *MetaFSM) commitPeerRegistryAndFireCallbacks(st *restoredMetaState) {
	// zero-CA peer registry (Task 5): swap in the pre-validated registry indexes
	// (validated in the decode phase above), then fire onPeersChanged so the
	// transport composer rebuilds the accept-set union. Without this, the
	// per-node SPKIs vanish after snapshot install and the composer silently
	// drops them → partition. commitPeerIndexes cannot fail — all validation
	// (duplicate node ID / SPKI, bad state) ran before any core state committed,
	// so a corrupt peer vector never leaves partially-mutated FSM state. The
	// registry has its own mutex, so the swap runs outside f.mu. firePeersChanged
	// snapshots the callback under RLock and invokes it outside (existing pattern).
	//
	// This commit + callback are deliberately the LAST side effects of Restore:
	// the accept-set rebuild must fire ONLY after Restore is guaranteed to return
	// nil. All preceding store commits (IPST, JKEY) are documented atomic/no-error,
	// so a late failure must NOT have rebuilt the transport accept-set for a
	// Restore that ultimately fails. Nothing after this point can err.
	f.peers.commitDenylist(st.deny)
	f.peers.commitPeerIndexes(st.peersByNodeID, st.peersBySPKI)
	f.firePeersChanged()

	// zero-CA cutover drop bit (spec §8 H3): if the restored snapshot says the
	// cluster key was dropped, fire the boot callback OUTSIDE f.mu (the callback
	// mutates transport state). Gated on the decoded local, not a field read, to
	// avoid an outside-lock field access. Dormant in PR-1 (snapshot always false).
	if st.clusterKeyDropped {
		f.mu.RLock()
		cb := f.onClusterKeyDropped
		f.mu.RUnlock()
		if cb != nil {
			cb()
		}
	}
	// PR-2a §8c step 5 (F2 spec-gate fix): fire outside f.mu when bit restored.
	if st.presentFlipBegun {
		f.mu.Lock()
		cb := f.onPresentFlip
		f.mu.Unlock()
		if cb != nil {
			cb()
		}
	}
}
