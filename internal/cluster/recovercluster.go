package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

const RecoverClusterMarkerPath = "recovery/recovercluster.json"

var (
	ErrRecoverClusterNoSnapshot = errors.New("recovercluster: no source snapshot found")
	ErrRecoverClusterReadOnly   = errors.New("recovercluster: recovered cluster is write-disabled until verify --mark-writable")
)

type RecoverClusterOptions struct {
	SourceData        string
	TargetData        string
	NewNodeID         string
	NewRaftAddr       string
	BadgerManagedMode bool
	StripJointState   bool
}

type RecoverClusterPlan struct {
	Options              RecoverClusterOptions
	SourceRaftDir        string
	TargetRaftDir        string
	TargetMetaDir        string
	SnapshotIndex        uint64
	SnapshotTerm         uint64
	SnapshotSize         int64
	OriginalServers      []raft.Server
	JointPhase           raft.JointPhase
	JointOldVoters       []string
	JointNewVoters       []string
	JointEnterIndex      uint64
	JointManagedLearners []string
	SourceManagedMode    bool
	ManagedModePresent   bool
	TargetFresh          bool
	BlockedReason        string
}

type RecoverClusterMarker struct {
	Writable          bool          `json:"writable"`
	VerifiedAt        time.Time     `json:"verified_at,omitempty"`
	SourceSnapshot    SnapshotID    `json:"source_snapshot"`
	OriginalServers   []raft.Server `json:"original_servers"`
	RecoveredNodeID   string        `json:"recovered_node_id"`
	RecoveredRaftAddr string        `json:"recovered_raft_addr"`
	CreatedAt         time.Time     `json:"created_at"`
}

type SnapshotID struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Size  int64  `json:"size"`
}

// Recovery flow:
// source raft -> read-only plan -> execute full snapshot restore -> rewrite
// active membership to {new-node-id voter} -> marker writable=false -> serve
// wraps storage.Backend until verify flips the marker.
func BuildRecoverClusterPlan(opts RecoverClusterOptions) (*RecoverClusterPlan, error) {
	if opts.SourceData == "" || opts.TargetData == "" || opts.NewNodeID == "" || opts.NewRaftAddr == "" {
		return nil, fmt.Errorf("source-data, target-data, new-node-id, and new-raft-addr are required")
	}
	sourceRaftDir := filepath.Join(opts.SourceData, "raft")
	targetRaftDir := filepath.Join(opts.TargetData, "raft")
	targetMetaDir := filepath.Join(opts.TargetData, "meta")

	if hasGroups(opts.SourceData) {
		return nil, fmt.Errorf("multi-Raft group recovery is not supported: %s exists", filepath.Join(opts.SourceData, "groups"))
	}

	snap, err := inspectV2SnapshotReadOnly(sourceRaftDir)
	if err != nil {
		return nil, err
	}
	if snap == nil || snap.Index == 0 {
		return nil, ErrRecoverClusterNoSnapshot
	}
	if snap.JointPhase != raft.JointNone && !opts.StripJointState {
		return nil, fmt.Errorf("source snapshot is in joint consensus phase %d; rerun with --strip-joint-state to recover as a clean single-node cluster", snap.JointPhase)
	}
	if err := checkRecoverTargetFresh(opts.TargetData); err != nil {
		return nil, err
	}

	return &RecoverClusterPlan{
		Options:              opts,
		SourceRaftDir:        sourceRaftDir,
		TargetRaftDir:        targetRaftDir,
		TargetMetaDir:        targetMetaDir,
		SnapshotIndex:        snap.Index,
		SnapshotTerm:         snap.Term,
		SnapshotSize:         int64(len(snap.Data)),
		OriginalServers:      snap.Servers,
		JointPhase:           snap.JointPhase,
		JointOldVoters:       snap.JointOldVoters,
		JointNewVoters:       snap.JointNewVoters,
		JointEnterIndex:      snap.JointEnterIndex,
		JointManagedLearners: snap.JointManagedLearners,
		SourceManagedMode:    false,
		ManagedModePresent:   false,
		TargetFresh:          true,
	}, nil
}

func ExecuteRecoverClusterPlan(plan *RecoverClusterPlan) error {
	if plan == nil {
		return fmt.Errorf("recovercluster: nil plan")
	}
	if err := checkRecoverTargetFresh(plan.Options.TargetData); err != nil {
		return err
	}
	snap, err := inspectV2SnapshotReadOnly(plan.SourceRaftDir)
	if err != nil {
		return err
	}
	if snap == nil || snap.Index == 0 || len(snap.Data) == 0 {
		return ErrRecoverClusterNoSnapshot
	}
	marker := RecoverClusterMarker{
		Writable:          false,
		SourceSnapshot:    SnapshotID{Index: snap.Index, Term: snap.Term, Size: int64(len(snap.Data))},
		OriginalServers:   plan.OriginalServers,
		RecoveredNodeID:   plan.Options.NewNodeID,
		RecoveredRaftAddr: plan.Options.NewRaftAddr,
		CreatedAt:         time.Now().UTC(),
	}
	if err := WriteRecoverClusterMarker(plan.Options.TargetData, marker); err != nil {
		return fmt.Errorf("write recovery marker: %w", err)
	}

	if err := os.MkdirAll(plan.TargetMetaDir, 0o755); err != nil {
		return fmt.Errorf("create target meta dir: %w", err)
	}
	db, err := badger.Open(badger.DefaultOptions(plan.TargetMetaDir).WithLogger(nil))
	if err != nil {
		return fmt.Errorf("open target meta db: %w", err)
	}
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	if err := fsm.Restore(raft.SnapshotMeta{Index: snap.Index, Term: snap.Term, FormatVersion: snap.FormatVersion}, snap.Data); err != nil {
		_ = db.Close()
		return fmt.Errorf("restore FSM snapshot: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close target meta db: %w", err)
	}

	storeDB, logs, _, snaps, err := openRaftV2Stores(plan.TargetRaftDir)
	if err != nil {
		return fmt.Errorf("open target raft stores: %w", err)
	}
	defer storeDB.Close()

	recovered := &raft.Snapshot{
		LastIncludedIndex: snap.Index,
		LastIncludedTerm:  snap.Term,
		Index:             snap.Index,
		Term:              snap.Term,
		Data:              snap.Data,
		Configuration:     []string{plan.Options.NewNodeID},
		Servers:           []raft.Server{{ID: plan.Options.NewNodeID, Suffrage: raft.Voter}},
		FormatVersion:     snap.FormatVersion,
	}
	if err := snaps.Save(recovered); err != nil {
		return fmt.Errorf("write recovered snapshot: %w", err)
	}
	if err := logs.TruncateAfter(logs.FirstIndex() - 1); err != nil {
		return fmt.Errorf("clear recovered raft log: %w", err)
	}
	type snapshotBoundaryInstaller interface {
		InstallSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) error
	}
	installer, ok := logs.(snapshotBoundaryInstaller)
	if !ok {
		return fmt.Errorf("target raft log store cannot install snapshot boundary")
	}
	if err := installer.InstallSnapshotBoundary(recovered.Index, recovered.Term); err != nil {
		return fmt.Errorf("write recovered snapshot boundary: %w", err)
	}
	return nil
}

func MarkRecoverClusterWritable(dataDir string) error {
	marker, err := LoadRecoverClusterMarker(dataDir)
	if err != nil {
		return err
	}
	if marker == nil {
		return fmt.Errorf("recovery marker not found at %s", dataDir)
	}
	if err := VerifyRecoverClusterTarget(dataDir, *marker); err != nil {
		return err
	}
	marker.Writable = true
	marker.VerifiedAt = time.Now().UTC()
	return WriteRecoverClusterMarker(dataDir, *marker)
}

func VerifyRecoverClusterTarget(dataDir string, marker RecoverClusterMarker) error {
	metaDir := filepath.Join(dataDir, "meta")
	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil).WithReadOnly(true))
	if err != nil {
		return fmt.Errorf("verify target meta db: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("verify target meta db close: %w", err)
	}

	snap, err := inspectV2SnapshotReadOnly(filepath.Join(dataDir, "raft"))
	if err != nil {
		return fmt.Errorf("verify target raft snapshot: %w", err)
	}
	if snap == nil || snap.Index == 0 {
		return ErrRecoverClusterNoSnapshot
	}
	if snap.Index != marker.SourceSnapshot.Index || snap.Term != marker.SourceSnapshot.Term {
		return fmt.Errorf("verify target raft snapshot: marker points to %d/%d, raft has %d/%d",
			marker.SourceSnapshot.Index, marker.SourceSnapshot.Term, snap.Index, snap.Term)
	}
	if len(snap.Servers) != 1 || snap.Servers[0].ID != marker.RecoveredNodeID || snap.Servers[0].Suffrage != raft.Voter {
		return fmt.Errorf("verify target raft snapshot: active membership is not recovered single voter %q", marker.RecoveredNodeID)
	}
	return nil
}

func inspectV2SnapshotReadOnly(dir string) (*raft.Snapshot, error) {
	db, _, _, snaps, err := openRaftV2StoresReadOnly(dir)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return snaps.Latest()
}

func openRaftV2StoresReadOnly(dir string) (*badger.DB, raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	storeDir := filepath.Join(dir, raftV2StoreSubdir)
	db, err := badger.Open(badgerutil.SmallOptions(storeDir).WithReadOnly(true))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open badger %s: %w", storeDir, err)
	}
	ls, err := raft.NewBadgerLogStore(db, raftV2LogPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, err
	}
	ss, err := raft.NewBadgerStableStore(db, raftV2StablePrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, err
	}
	sn, err := raft.NewBadgerSnapshotStore(db, raftV2SnapPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, err
	}
	return db, ls, ss, sn, nil
}

func LoadRecoverClusterMarker(dataDir string) (*RecoverClusterMarker, error) {
	path := filepath.Join(dataDir, RecoverClusterMarkerPath)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var marker RecoverClusterMarker
	if err := json.Unmarshal(raw, &marker); err != nil {
		return nil, fmt.Errorf("read recovery marker: %w", err)
	}
	return &marker, nil
}

func WriteRecoverClusterMarker(dataDir string, marker RecoverClusterMarker) error {
	path := filepath.Join(dataDir, RecoverClusterMarkerPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func checkRecoverTargetFresh(targetData string) error {
	dirty := []string{"meta", "raft", "groups", "data", "blobs", "wal", "snapshots", "recovery"}
	for _, name := range dirty {
		if _, err := os.Stat(filepath.Join(targetData, name)); err == nil {
			return fmt.Errorf("target is not fresh: %s already exists", filepath.Join(targetData, name))
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func hasGroups(dataDir string) bool {
	entries, err := os.ReadDir(filepath.Join(dataDir, "groups"))
	return err == nil && len(entries) > 0
}
